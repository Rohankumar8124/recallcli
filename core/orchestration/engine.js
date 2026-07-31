/**
 * Embedded durable execution engine.
 *
 * Long recallcli operations (indexing 50+ GitHub repos, cross-device sync drains)
 * are long enough to be interrupted — Ctrl-C, a laptop sleeping, a rate limit,
 * a dropped connection. Without persistence the only recovery is to start over,
 * which re-does every completed unit of work and re-issues every side effect.
 *
 * This is a write-ahead step log on the SQLite we already ship. Each step is
 * recorded *before* its side effect runs and marked terminal after, so an
 * interrupted run can resume from the first step that did not finish. Steps are
 * keyed by a deterministic idempotency key, so resuming never repeats work that
 * already succeeded.
 *
 * Deliberately not Temporal.io / DBOS / Convex: every general-purpose durable
 * workflow runtime needs a server or a hosted backend. recallcli is a `npm i -g` CLI
 * that must work offline, so the engine is embedded — no daemon, no port, no
 * extra resident memory. See docs in README (“Durable orchestration”).
 *
 * Concurrency: WAL mode plus a busy_timeout, so a CLI invocation and the
 * background watcher can touch the same database without either erroring out.
 */

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const DEFAULT_DB = path.join(__dirname, "..", "..", "storage", "workflows.db");

// Terminal + non-terminal step states.
const STATUS = {
    PENDING: "pending",
    IN_FLIGHT: "in_flight",
    DONE: "done",
    FAILED: "failed",
};

const RUN_STATUS = {
    RUNNING: "running",
    COMPLETED: "completed",
    FAILED: "failed",
};

let _db = null;
let _dbPath = null;

/**
 * Open (once) and migrate the workflow database.
 *
 * synchronous=NORMAL rather than FULL: losing the last transaction on a power
 * cut is acceptable here because a lost step simply replays, and replay is safe
 * by construction. busy_timeout lets the CLI and daemon contend without errors.
 */
function db(dbPath = process.env.recallcli_WORKFLOW_DB || DEFAULT_DB) {
    if (_db && _dbPath === dbPath) return _db;
    if (_db) {
        try { _db.close(); } catch { /* already closed */ }
        _db = null;
    }

    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    const Database = require("better-sqlite3");
    const d = new Database(dbPath);

    d.pragma("journal_mode = WAL");
    d.pragma("synchronous = NORMAL");
    d.pragma("busy_timeout = 5000");
    // Keep the -wal file from growing without bound in a long-lived daemon.
    d.pragma("wal_autocheckpoint = 1000");
    d.pragma("journal_size_limit = 6291456"); // 6 MB

    d.exec(`
        CREATE TABLE IF NOT EXISTS runs (
            run_id      TEXT PRIMARY KEY,
            workflow    TEXT NOT NULL,
            status      TEXT NOT NULL,
            project     TEXT,
            input_json  TEXT,
            created_at  INTEGER NOT NULL,
            updated_at  INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS steps (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          TEXT NOT NULL,
            step_name       TEXT NOT NULL,
            idem_key        TEXT NOT NULL UNIQUE,
            status          TEXT NOT NULL,
            attempt         INTEGER NOT NULL DEFAULT 0,
            output_json     TEXT,
            error           TEXT,
            created_at      INTEGER NOT NULL,
            updated_at      INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_steps_run    ON steps(run_id, status);
        CREATE INDEX IF NOT EXISTS idx_runs_status  ON runs(status, updated_at);
    `);

    _db = d;
    _dbPath = dbPath;
    return d;
}

/** Close the handle — used by tests and by the daemon on shutdown. */
function close() {
    if (_db) {
        try { _db.close(); } catch { /* noop */ }
        _db = null;
        _dbPath = null;
    }
}

/**
 * Deterministic idempotency key.
 *
 * Must be stable across processes and runs so a resumed run recognises work it
 * already did. Object keys are sorted so `{a,b}` and `{b,a}` hash identically.
 */
function idempotencyKey(runId, stepName, input) {
    const canonical = JSON.stringify(input === undefined ? null : input, (_k, v) => {
        if (v && typeof v === "object" && !Array.isArray(v)) {
            return Object.keys(v).sort().reduce((acc, k) => { acc[k] = v[k]; return acc; }, {});
        }
        return v;
    });
    return crypto.createHash("sha256")
        .update(`${runId}${stepName}${canonical}`)
        .digest("hex");
}

/**
 * AWS "full jitter" backoff. Plain exponential backoff makes every interrupted
 * client retry in lockstep; randomising the whole interval spreads them out.
 */
function fullJitterDelay(attempt, base = 500, cap = 30000, rng = Math.random) {
    const expo = Math.min(cap, base * 2 ** attempt);
    return Math.floor(rng() * expo);
}

/**
 * Default retry policy. Transport failures and 5xx are transient; 4xx other
 * than 429 are deterministic and must not burn the GitHub rate-limit budget.
 */
function defaultIsRetryable(err) {
    if (!err) return false;
    const code = err.code || err.errno;
    if (["ECONNRESET", "ETIMEDOUT", "EAI_AGAIN", "ENOTFOUND", "ECONNREFUSED", "EPIPE"].includes(code)) {
        return true;
    }
    const status = err.status || err.statusCode || err.response?.status;
    if (status === 429) return true;
    if (typeof status === "number" && status >= 500 && status < 600) return true;
    return false;
}

/**
 * Honour a server's own pacing when it gives one — Retry-After, or GitHub's
 * x-ratelimit-reset — instead of guessing with the backoff formula.
 */
function serverRequestedDelayMs(err, now = Date.now()) {
    const headers = err?.headers || err?.response?.headers;
    if (!headers) return null;
    const get = (k) => (typeof headers.get === "function" ? headers.get(k) : headers[k]);

    const retryAfter = get("retry-after");
    if (retryAfter != null) {
        const secs = Number(retryAfter);
        if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
        const when = Date.parse(retryAfter);
        if (Number.isFinite(when)) return Math.max(0, when - now);
    }

    const remaining = get("x-ratelimit-remaining");
    const reset = get("x-ratelimit-reset");
    if (String(remaining) === "0" && reset != null) {
        const resetMs = Number(reset) * 1000;
        if (Number.isFinite(resetMs)) return Math.max(0, resetMs - now);
    }
    return null;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function newRunId() {
    return `wf_${crypto.randomBytes(5).toString("hex")}`;
}

/** Begin a durable run. */
function startRun(workflow, input = {}, opts = {}) {
    const d = db(opts.dbPath);
    const runId = opts.runId || newRunId();
    const now = Date.now();
    d.prepare(`
        INSERT INTO runs (run_id, workflow, status, project, input_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at
    `).run(runId, workflow, RUN_STATUS.RUNNING, opts.project || null, JSON.stringify(input), now, now);
    return { runId, workflow };
}

function finishRun(runId, status, opts = {}) {
    db(opts.dbPath).prepare("UPDATE runs SET status = ?, updated_at = ? WHERE run_id = ?")
        .run(status, Date.now(), runId);
}

function getRun(runId, opts = {}) {
    return db(opts.dbPath).prepare("SELECT * FROM runs WHERE run_id = ?").get(runId) || null;
}

function listRuns({ status, workflow, limit = 20, dbPath } = {}) {
    const clauses = [];
    const args = [];
    if (status) { clauses.push("status = ?"); args.push(status); }
    if (workflow) { clauses.push("workflow = ?"); args.push(workflow); }
    const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    return db(dbPath).prepare(
        `SELECT * FROM runs ${where} ORDER BY updated_at DESC LIMIT ?`
    ).all(...args, limit);
}

function getSteps(runId, opts = {}) {
    return db(opts.dbPath)
        .prepare("SELECT * FROM steps WHERE run_id = ? ORDER BY id ASC")
        .all(runId);
}

/** Per-run progress counters, used by `recallcli workflow` and by resume output. */
function runProgress(runId, opts = {}) {
    const rows = db(opts.dbPath).prepare(
        "SELECT status, COUNT(*) AS n FROM steps WHERE run_id = ? GROUP BY status"
    ).all(runId);
    const out = { done: 0, failed: 0, pending: 0, in_flight: 0, total: 0 };
    for (const r of rows) {
        out[r.status] = r.n;
        out.total += r.n;
    }
    return out;
}

/**
 * Execute one durable step.
 *
 * Contract:
 *  - If this exact step already completed (same run, name and input), its stored
 *    output is returned and `fn` is NOT called again. This is what makes resume
 *    safe for side-effecting work.
 *  - Otherwise `fn` runs, with retries for transient failures only.
 *  - The row is written before `fn` executes, so a hard crash mid-call leaves an
 *    `in_flight` row that resume can detect rather than silent loss.
 *
 * @param {string}   runId
 * @param {string}   stepName
 * @param {Function} fn        async () => result  (result must be JSON-serialisable)
 * @param {object}   opts      { input, retries, isRetryable, baseDelay, maxDelay, onRetry, dbPath }
 */
async function runStep(runId, stepName, fn, opts = {}) {
    const d = db(opts.dbPath);
    const input = opts.input;
    const key = idempotencyKey(runId, stepName, input);
    const now = Date.now();

    const existing = d.prepare("SELECT * FROM steps WHERE idem_key = ?").get(key);
    if (existing && existing.status === STATUS.DONE) {
        return {
            result: existing.output_json ? JSON.parse(existing.output_json) : null,
            cached: true,
            attempt: existing.attempt,
        };
    }

    if (!existing) {
        d.prepare(`
            INSERT INTO steps (run_id, step_name, idem_key, status, attempt, created_at, updated_at)
            VALUES (?, ?, ?, ?, 0, ?, ?)
        `).run(runId, stepName, key, STATUS.PENDING, now, now);
    }

    const maxRetries = opts.retries ?? 3;
    const isRetryable = opts.isRetryable || defaultIsRetryable;
    let lastErr;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        d.prepare("UPDATE steps SET status = ?, attempt = ?, updated_at = ? WHERE idem_key = ?")
            .run(STATUS.IN_FLIGHT, attempt, Date.now(), key);

        try {
            const result = await fn({ attempt, runId, stepName });
            d.prepare("UPDATE steps SET status = ?, output_json = ?, error = NULL, updated_at = ? WHERE idem_key = ?")
                .run(STATUS.DONE, JSON.stringify(result ?? null), Date.now(), key);
            return { result, cached: false, attempt };
        } catch (err) {
            lastErr = err;
            const retryable = isRetryable(err);

            if (!retryable || attempt === maxRetries) {
                d.prepare("UPDATE steps SET status = ?, error = ?, updated_at = ? WHERE idem_key = ?")
                    .run(STATUS.FAILED, String(err?.message || err), Date.now(), key);
                throw err;
            }

            const delay = serverRequestedDelayMs(err)
                ?? fullJitterDelay(attempt, opts.baseDelay ?? 500, opts.maxDelay ?? 30000);
            if (opts.onRetry) opts.onRetry({ stepName, attempt, delay, error: err });
            await sleep(delay);
        }
    }

    throw lastErr;
}

/**
 * Drive a list of units through a run, skipping ones already completed.
 *
 * `units` are described declaratively so a resumed process can rebuild the same
 * step names and inputs, then let runStep()'s idempotency check short-circuit
 * everything already done.
 *
 * @param {string}   runId
 * @param {Array}    units    [{ name, input, run: async(input) => result }]
 * @param {object}   opts     { onProgress, continueOnError, ...runStep opts }
 */
async function runAll(runId, units, opts = {}) {
    const results = [];
    let completed = 0;
    let skipped = 0;
    let failed = 0;

    for (const unit of units) {
        // A unit is either a named activity (portable across backends — the
        // Temporal workflow can only schedule these) or an inline closure
        // (embedded-only, kept for callers that do not need portability).
        const body = unit.activity
            ? () => require("./activities").resolve(unit.activity)(unit.input)
            : () => unit.run(unit.input);

        try {
            const { result, cached } = await runStep(
                runId,
                unit.name,
                body,
                { ...opts, input: unit.input }
            );
            results.push({ name: unit.name, ok: true, cached, result });
            if (cached) skipped++; else completed++;
        } catch (err) {
            failed++;
            results.push({ name: unit.name, ok: false, error: String(err?.message || err) });
            if (!opts.continueOnError) {
                finishRun(runId, RUN_STATUS.FAILED, opts);
                throw err;
            }
        }
        if (opts.onProgress) {
            opts.onProgress({ completed, skipped, failed, total: units.length, name: unit.name });
        }
    }

    finishRun(runId, failed && !opts.continueOnError ? RUN_STATUS.FAILED : RUN_STATUS.COMPLETED, opts);
    return { results, completed, skipped, failed, total: units.length };
}

/** Most recent resumable (non-terminal) run for a workflow. */
function findResumableRun(workflow, opts = {}) {
    return db(opts.dbPath).prepare(
        "SELECT * FROM runs WHERE workflow = ? AND status = ? ORDER BY updated_at DESC LIMIT 1"
    ).get(workflow, RUN_STATUS.RUNNING) || null;
}

/**
 * Delete finished runs older than `days`, then reclaim pages.
 * Without this the step log grows monotonically on a machine that indexes often.
 */
function prune({ days = 30, dbPath } = {}) {
    const d = db(dbPath);
    const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
    const old = d.prepare(
        "SELECT run_id FROM runs WHERE status != ? AND updated_at < ?"
    ).all(RUN_STATUS.RUNNING, cutoff).map((r) => r.run_id);

    if (!old.length) return { runs: 0, steps: 0 };

    const placeholders = old.map(() => "?").join(",");
    const steps = d.prepare(`DELETE FROM steps WHERE run_id IN (${placeholders})`).run(...old);
    const runs = d.prepare(`DELETE FROM runs  WHERE run_id IN (${placeholders})`).run(...old);
    d.pragma("wal_checkpoint(TRUNCATE)");
    return { runs: runs.changes, steps: steps.changes };
}

module.exports = {
    STATUS,
    RUN_STATUS,
    db,
    close,
    idempotencyKey,
    fullJitterDelay,
    defaultIsRetryable,
    serverRequestedDelayMs,
    startRun,
    finishRun,
    getRun,
    listRuns,
    getSteps,
    runProgress,
    runStep,
    runAll,
    findResumableRun,
    prune,
};
