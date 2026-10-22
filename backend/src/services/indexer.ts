import {
  Contract,
  rpc,
  TransactionBuilder,
  Networks,
  scValToNative,
} from "@stellar/stellar-sdk";
import { recordEventWithDb } from "./eventHistory";
import { getDb } from "./db";
import {
  eventsIndexedTotal,
  ledgersScannedTotal,
  lastIndexedLedger,
  indexerLatestLedger,
  indexerLedgerLag,
  indexerErrorsTotal,
  indexerCircuitState,
} from "./metrics";
import { logger } from "../logger";

const FALLBACK_POLLING_ENABLED = process.env.INDEXER_FALLBACK_POLLING_ENABLED === "true";
const FALLBACK_POLL_INTERVAL_MS = Number(process.env.INDEXER_FALLBACK_POLL_INTERVAL_MS ?? 10000);

let rpcServer: rpc.Server | null = null;
let contractId: string | null = null;
let networkPassphrase: string = Networks.TESTNET;
let lastProcessedLedger = 0;
let indexerInterval: NodeJS.Timeout | null = null;
let indexerStartLedger: number | null = null;
let isIndexing = false;
let lastObservedLedger: number | null = null;
let lastFailureKind: IndexerRpcFailureKind | null = null;
let invalidStartLedgerConfigured = false;

const INDEXER_CURSOR_TABLE = "indexer_cursor";
const CHECKPOINT_ROW_ID = 1;

/**
 * Enumerated reason the most recent indexer poll failed.
 *
 * Raw provider errors are mapped to these coarse kinds so the monitoring
 * outcome signal never has to carry an error message (which can embed an RPC
 * URL, credential, or contract ID).
 */
export type IndexerRpcFailureKind =
  | "rate_limited"
  | "disconnected"
  | "provider_error"
  | "unknown";

export type IndexerBackfillStatusCode =
  | 200
  | 400
  | 429
  | 502
  | 503;

export type IndexerBackfillOutcome =
  | "success"
  | "transient_delay"
  | "invalid_start_ledger"
  | "unconfigured"
  | "circuit_open"
  | "rpc_error";

export interface IndexerBackfillResult {
  ok: boolean;
  statusCode: IndexerBackfillStatusCode;
  outcome: IndexerBackfillOutcome;
  code:
    | "OK"
    | "NO_NEW_LEDGERS"
    | "TRUNCATED_PAGE"
    | "INVALID_START_LEDGER"
    | "UNCONFIGURED"
    | "CIRCUIT_OPEN"
    | "RATE_LIMITED"
    | "DISCONNECTED"
    | "PROVIDER_ERROR"
    | "RPC_ERROR";
  startLedger: number;
  latestLedger: number | null;
  persistedCheckpointBefore: number;
  persistedCheckpointAfter: number;
  eventsIndexed: number;
  pagesFetched: number;
  truncated: boolean;
  failureKind: IndexerRpcFailureKind | null;
  detail: string;
}

function extractStatusCode(err: unknown): number | undefined {
  if (err && typeof err === "object") {
    const e = err as Record<string, any>;
    const raw = e["status"] ?? e["statusCode"] ?? e["response"]?.["status"];
    return typeof raw === "number" ? raw : undefined;
  }
  return undefined;
}

function errorHaystack(err: unknown): string {
  if (!err || typeof err !== "object") {
    return String(err ?? "").toLowerCase();
  }
  const e = err as Record<string, any>;
  return [e["message"], e["code"], e["name"]]
    .filter((part) => part !== undefined && part !== null)
    .map((part) => String(part))
    .join(" ")
    .toLowerCase();
}

/**
 * Classifies an RPC failure into the coarse kinds surfaced by the indexer
 * monitoring outcome. Deliberately returns a kind, never the original message,
 * so callers cannot leak provider URLs or credentials by accident.
 */
export function classifyRpcFailure(err: unknown): IndexerRpcFailureKind {
  const statusCode = extractStatusCode(err);
  const text = errorHaystack(err);

  if (
    statusCode === 429 ||
    text.includes("429") ||
    text.includes("rate limit") ||
    text.includes("too many requests") ||
    text.includes("throttl")
  ) {
    return "rate_limited";
  }

  if (
    statusCode === 503 ||
    statusCode === 504 ||
    text.includes("econnrefused") ||
    text.includes("econnreset") ||
    text.includes("etimedout") ||
    text.includes("epipe") ||
    text.includes("socket hang up") ||
    text.includes("disconnect") ||
    text.includes("network") ||
    text.includes("timeout") ||
    text.includes("fetch failed")
  ) {
    return "disconnected";
  }

  return statusCode === undefined ? "unknown" : "provider_error";
}


export enum CircuitState {
  CLOSED = "CLOSED",
  OPEN = "OPEN",
  HALF_OPEN = "HALF_OPEN",
}

export class CircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED;
  private failureCount: number = 0;
  private lastFailureTime: number = 0;
  private readonly failureThreshold: number = 5;
  private readonly timeoutMs: number;

  constructor(timeoutMs: number = 60000) {
    this.timeoutMs = timeoutMs;
  }

  public getState(): CircuitState {
    if (this.state === CircuitState.OPEN) {
      const now = Date.now();
      if (now - this.lastFailureTime >= this.timeoutMs) {
        this.setState(CircuitState.HALF_OPEN);
      }
    }
    return this.state;
  }

  public onSuccess(): void {
    if (this.state !== CircuitState.CLOSED) {
      logger.info("circuit breaker probe succeeded");
      this.setState(CircuitState.CLOSED);
    }
    this.failureCount = 0;
  }

  public onFailure(): void {
    this.failureCount++;
    this.lastFailureTime = Date.now();

    if (this.state === CircuitState.CLOSED && this.failureCount >= this.failureThreshold) {
      logger.warn({ failureThreshold: this.failureThreshold }, "circuit breaker failure threshold reached");
      this.setState(CircuitState.OPEN);
    } else if (this.state === CircuitState.HALF_OPEN) {
      logger.warn("circuit breaker probe failed");
      this.setState(CircuitState.OPEN);
    }
  }

  /** Resets the circuit breaker to CLOSED with zero failures. Intended for tests. */
  public reset(): void {
    this.failureCount = 0;
    this.lastFailureTime = 0;
    this.setState(CircuitState.CLOSED);
  }

  /** Consecutive failures recorded since the last successful poll. */
  public getFailureCount(): number {
    return this.failureCount;
  }

  /** Number of consecutive failures that opens the circuit. */
  public getFailureThreshold(): number {
    return this.failureThreshold;
  }

  private setState(newState: CircuitState): void {
    if (this.state !== newState) {
      logger.info({ from: this.state, to: newState }, "circuit breaker state changed");
      this.state = newState;
    }
    const stateValue =
      newState === CircuitState.CLOSED ? 0
      : newState === CircuitState.HALF_OPEN ? 1
      : 2;
    indexerCircuitState.set(stateValue);
  }
}

const CIRCUIT_BREAKER_TIMEOUT_MS = Number(process.env.CIRCUIT_BREAKER_TIMEOUT_MS ?? 60000);
const circuitBreaker = new CircuitBreaker(CIRCUIT_BREAKER_TIMEOUT_MS);

export function getCircuitBreakerStatus(): CircuitState {
  return circuitBreaker.getState();
}

/**
 * Secret-free view of the indexer's progress and RPC health, consumed by the
 * monitoring outcome signal in `indexerMonitor.ts`. Carries ledgers, counts and
 * enumerated state only — never the RPC URL, contract ID, or credentials.
 */
export interface IndexerMonitoringSnapshot {
  /** Whether an RPC endpoint and contract are configured. */
  rpcConfigured: boolean;
  /** Highest ledger observed from RPC, or null if no poll has succeeded yet. */
  latestLedger: number | null;
  /** Persisted checkpoint (last fully processed ledger). */
  indexedLedger: number;
  /** Unprocessed ledger backlog, clamped at zero. */
  ledgerLag: number;
  /** Consecutive failed polls since the last success. */
  consecutiveFailures: number;
  /** Consecutive failures that open the circuit. */
  failureThreshold: number;
  /** Current circuit breaker state. */
  circuitState: CircuitState;
  /** Coarse kind of the last poll failure, or null after a success. */
  lastFailureKind: IndexerRpcFailureKind | null;
  /** Whether a poll is currently in flight. */
  pollInFlight: boolean;
}

export function getIndexerMonitoringSnapshot(): IndexerMonitoringSnapshot {
  const latestLedger = lastObservedLedger;
  return {
    rpcConfigured: Boolean(rpcServer && contractId),
    latestLedger,
    indexedLedger: lastProcessedLedger,
    ledgerLag:
      latestLedger === null
        ? 0
        : Math.max(0, latestLedger - lastProcessedLedger),
    consecutiveFailures: circuitBreaker.getFailureCount(),
    failureThreshold: circuitBreaker.getFailureThreshold(),
    circuitState: circuitBreaker.getState(),
    lastFailureKind,
    pollInFlight: isIndexing,
  };
}

/** Records a successful poll: clears the failure budget and the failure kind. */
function recordPollSuccess(): void {
  circuitBreaker.onSuccess();
  lastFailureKind = null;
}

function isFallbackPollingEnabled(): boolean {
  return FALLBACK_POLLING_ENABLED;
}

function getFallbackPollInterval(): number {
  return Math.max(1000, FALLBACK_POLL_INTERVAL_MS);
}

function loadCheckpoint(db: any): void {
  try {
    const row = db
      .prepare(`SELECT last_ledger_sequence FROM ${INDEXER_CURSOR_TABLE} WHERE id = @id`)
      .get({ id: CHECKPOINT_ROW_ID }) as { last_ledger_sequence: number } | undefined;

    if (row && row.last_ledger_sequence > 0) {
      lastProcessedLedger = row.last_ledger_sequence;
      logger.info({ lastProcessedLedger }, "loaded indexer checkpoint from database");
    } else {
      logger.info("no checkpoint found, starting from ledger 0");
    }
  } catch (err) {
    logger.error({ err }, "failed to load indexer checkpoint, starting from ledger 0");
    lastProcessedLedger = 0;
  }
}

function saveCheckpoint(db: any, ledgerSequence: number): void {
  db.transaction(() => {
    const existing = db
      .prepare(`SELECT id FROM ${INDEXER_CURSOR_TABLE} WHERE id = @id`)
      .get({ id: CHECKPOINT_ROW_ID }) as { id: number } | undefined;

    if (existing) {
      db.prepare(
        `UPDATE ${INDEXER_CURSOR_TABLE} SET last_ledger_sequence = @ledger WHERE id = @id`,
      ).run({ ledger: ledgerSequence, id: CHECKPOINT_ROW_ID });
    } else {
      db.prepare(
        `INSERT INTO ${INDEXER_CURSOR_TABLE} (id, last_ledger_sequence) VALUES (@id, @ledger)`,
      ).run({ id: CHECKPOINT_ROW_ID, ledger: ledgerSequence });
    }
  })();
  logger.debug({ ledgerSequence }, "checkpoint saved to database");
}

function readPersistedCheckpoint(db: any): number {
  try {
    const row = db
      .prepare(`SELECT last_ledger_sequence FROM ${INDEXER_CURSOR_TABLE} WHERE id = @id`)
      .get({ id: CHECKPOINT_ROW_ID }) as { last_ledger_sequence: number } | undefined;
    return row && typeof row.last_ledger_sequence === "number" && row.last_ledger_sequence > 0
      ? row.last_ledger_sequence
      : 0;
  } catch {
    return 0;
  }
}

function parseStartLedgerInput(raw: unknown): { valid: true; value: number } | { valid: false } {
  if (typeof raw === "number") {
    if (Number.isInteger(raw) && raw >= 0 && Number.isSafeInteger(raw)) {
      return { valid: true, value: raw };
    }
    return { valid: false };
  }
  if (typeof raw === "string") {
    const trimmed = raw.trim();
    if (!/^\d+$/.test(trimmed)) {
      return { valid: false };
    }
    const parsed = Number(trimmed);
    if (Number.isInteger(parsed) && parsed >= 0 && Number.isSafeInteger(parsed)) {
      return { valid: true, value: parsed };
    }
  }
  return { valid: false };
}

function mapFailureKindToStatusCode(kind: IndexerRpcFailureKind): IndexerBackfillStatusCode {
  switch (kind) {
    case "rate_limited":
      return 429;
    case "disconnected":
      return 503;
    case "provider_error":
    case "unknown":
    default:
      return 502;
  }
}

function mapFailureKindToCode(
  kind: IndexerRpcFailureKind,
): "RATE_LIMITED" | "DISCONNECTED" | "PROVIDER_ERROR" | "RPC_ERROR" {
  switch (kind) {
    case "rate_limited":
      return "RATE_LIMITED";
    case "disconnected":
      return "DISCONNECTED";
    case "provider_error":
      return "PROVIDER_ERROR";
    default:
      return "RPC_ERROR";
  }
}

export function initIndexer(
  rpcUrl: string,
  contractIdParam: string,
  networkPass?: string,
): void {
  rpcServer = new rpc.Server(rpcUrl);
  contractId = contractIdParam;
  if (networkPass) {
    networkPassphrase = networkPass;
  }

  indexerStartLedger = null;
  invalidStartLedgerConfigured = false;

  const startLedgerEnv = process.env.INDEXER_START_LEDGER;
  if (startLedgerEnv !== undefined && startLedgerEnv.trim() !== "") {
    const parsed = parseStartLedgerInput(startLedgerEnv);
    if (parsed.valid) {
      indexerStartLedger = parsed.value;
      if (parsed.value !== 0) {
        logger.warn({ startLedger: parsed.value }, "INDEXER_START_LEDGER override active");
      }
    } else {
      invalidStartLedgerConfigured = true;
      logger.error({ value: startLedgerEnv }, "invalid INDEXER_START_LEDGER value");
    }
  }

  const db = getDb();
  ensureIndexerCursorTable(db);
  loadCheckpoint(db);

  if (indexerStartLedger !== null && indexerStartLedger > lastProcessedLedger) {
    // Apply start ledger override in memory for the first backfill poll only;
    // do not write to indexer_cursor until the backfill scan completes cleanly.
    lastProcessedLedger = indexerStartLedger;
    logger.info({ lastProcessedLedger }, "applied INDEXER_START_LEDGER override to checkpoint");
  }

  lastIndexedLedger.set(lastProcessedLedger);
}

function ensureIndexerCursorTable(db: any): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS ${INDEXER_CURSOR_TABLE} (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      last_ledger_sequence INTEGER NOT NULL
    );
  `);
}

export function startIndexer(intervalMs = 10000): void {
  if (indexerInterval) {
    return;
  }

  const effectiveInterval = isFallbackPollingEnabled()
    ? getFallbackPollInterval()
    : intervalMs;

  logger.info(
    { intervalMs: effectiveInterval, fallbackMode: isFallbackPollingEnabled() },
    "event indexer started",
  );

  indexerInterval = setInterval(() => {
    indexEvents().catch((err) => {
      logger.error({ err }, "indexer error");
    });
  }, effectiveInterval);

  indexEvents().catch((err) => {
    logger.error({ err }, "initial indexer error");
  });
}

export function stopIndexer(): void {
  if (indexerInterval) {
    clearInterval(indexerInterval);
    indexerInterval = null;
    logger.info("event indexer stopped");
  }
}

/**
 * Resets all module-level indexer state.
 * Intended for use in tests only — allows each test to start with a clean slate
 * without module-cache pollution from a previous `initIndexer` call.
 * @internal
 */
export function resetIndexerState(): void {
  rpcServer = null;
  contractId = null;
  networkPassphrase = Networks.TESTNET;
  lastProcessedLedger = 0;
  indexerInterval = null;
  indexerStartLedger = null;
  invalidStartLedgerConfigured = false;
  lastObservedLedger = null;
  lastFailureKind = null;
  isIndexing = false;
  circuitBreaker.reset();
}

interface ScanBatchSummary {
  eventsIndexed: number;
  pagesFetched: number;
  truncated: boolean;
  checkpoint: number;
}

/**
 * Executes one explicit indexer backfill / poll cycle from the configured start
 * ledger or persisted checkpoint and returns a deterministic status code and
 * structured outcome without silently mutating the persisted checkpoint on
 * boundary inputs, truncated pages, or RPC errors.
 */
export async function runIndexerBackfill(options?: {
  startLedger?: number | string;
}): Promise<IndexerBackfillResult> {
  let db: any = null;
  try {
    db = getDb();
  } catch {
    db = null;
  }

  const persistedBefore = db ? readPersistedCheckpoint(db) : lastProcessedLedger;

  // 1. Validate explicit or environment-configured startLedger boundary inputs
  let effectiveBaseLedger = lastProcessedLedger;
  if (options?.startLedger !== undefined) {
    const parsed = parseStartLedgerInput(options.startLedger);
    if (!parsed.valid) {
      return {
        ok: false,
        statusCode: 400,
        outcome: "invalid_start_ledger",
        code: "INVALID_START_LEDGER",
        startLedger: persistedBefore + 1,
        latestLedger: lastObservedLedger,
        persistedCheckpointBefore: persistedBefore,
        persistedCheckpointAfter: persistedBefore,
        eventsIndexed: 0,
        pagesFetched: 0,
        truncated: false,
        failureKind: null,
        detail: "startLedger must be a non-negative safe integer; persisted checkpoint was not modified.",
      };
    }
    effectiveBaseLedger = parsed.value;
  } else if (invalidStartLedgerConfigured) {
    return {
      ok: false,
      statusCode: 400,
      outcome: "invalid_start_ledger",
      code: "INVALID_START_LEDGER",
      startLedger: persistedBefore + 1,
      latestLedger: lastObservedLedger,
      persistedCheckpointBefore: persistedBefore,
      persistedCheckpointAfter: persistedBefore,
      eventsIndexed: 0,
      pagesFetched: 0,
      truncated: false,
      failureKind: null,
      detail: "Configured INDEXER_START_LEDGER is invalid; persisted checkpoint was not modified.",
    };
  }

  const targetStartLedger = effectiveBaseLedger + 1;

  if (!rpcServer || !contractId || !db) {
    return {
      ok: false,
      statusCode: 503,
      outcome: "unconfigured",
      code: "UNCONFIGURED",
      startLedger: targetStartLedger,
      latestLedger: lastObservedLedger,
      persistedCheckpointBefore: persistedBefore,
      persistedCheckpointAfter: persistedBefore,
      eventsIndexed: 0,
      pagesFetched: 0,
      truncated: false,
      failureKind: null,
      detail: "Indexer RPC server or contract ID is not configured; persisted checkpoint was not modified.",
    };
  }

  const state = circuitBreaker.getState();
  if (state === CircuitState.OPEN) {
    return {
      ok: false,
      statusCode: 503,
      outcome: "circuit_open",
      code: "CIRCUIT_OPEN",
      startLedger: targetStartLedger,
      latestLedger: lastObservedLedger,
      persistedCheckpointBefore: persistedBefore,
      persistedCheckpointAfter: persistedBefore,
      eventsIndexed: 0,
      pagesFetched: 0,
      truncated: false,
      failureKind: lastFailureKind,
      detail: "Circuit breaker is OPEN; skipping backfill poll and preserving persisted checkpoint.",
    };
  }

  if (isIndexing) {
    return {
      ok: true,
      statusCode: 200,
      outcome: "transient_delay",
      code: "NO_NEW_LEDGERS",
      startLedger: targetStartLedger,
      latestLedger: lastObservedLedger,
      persistedCheckpointBefore: persistedBefore,
      persistedCheckpointAfter: persistedBefore,
      eventsIndexed: 0,
      pagesFetched: 0,
      truncated: false,
      failureKind: null,
      detail: "An indexer poll is already in flight; persisted checkpoint remains unchanged.",
    };
  }

  isIndexing = true;
  const previousProcessedLedger = lastProcessedLedger;
  lastProcessedLedger = effectiveBaseLedger;

  try {
    const latestLedger = await rpcServer.getLatestLedger();
    const currentLedger = latestLedger.sequence;
    lastObservedLedger = currentLedger;
    indexerLatestLedger.set(currentLedger);
    indexerLedgerLag.set(Math.max(0, currentLedger - lastProcessedLedger));

    // Boundary case: configured startLedger (or current checkpoint) is at or ahead of RPC head
    if (currentLedger <= lastProcessedLedger) {
      // Restore in-memory checkpoint to persisted state if an override was beyond chain head
      lastProcessedLedger = persistedBefore > 0 ? persistedBefore : previousProcessedLedger;
      lastIndexedLedger.set(lastProcessedLedger);
      recordPollSuccess();
      return {
        ok: true,
        statusCode: 200,
        outcome: "success",
        code: "NO_NEW_LEDGERS",
        startLedger: targetStartLedger,
        latestLedger: currentLedger,
        persistedCheckpointBefore: persistedBefore,
        persistedCheckpointAfter: readPersistedCheckpoint(db),
        eventsIndexed: 0,
        pagesFetched: 0,
        truncated: false,
        failureKind: null,
        detail: `RPC head (${currentLedger}) has not advanced past configured base ledger (${effectiveBaseLedger}); persisted checkpoint (${persistedBefore}) was not modified.`,
      };
    }

    const summary = isFallbackPollingEnabled()
      ? await indexEventsWithFallback(db, currentLedger)
      : await indexEventsWithCursorPagination(db, currentLedger);

    recordPollSuccess();

    const persistedAfter = readPersistedCheckpoint(db);
    if (summary.truncated) {
      return {
        ok: true,
        statusCode: 200,
        outcome: "transient_delay",
        code: "TRUNCATED_PAGE",
        startLedger: targetStartLedger,
        latestLedger: currentLedger,
        persistedCheckpointBefore: persistedBefore,
        persistedCheckpointAfter: persistedAfter,
        eventsIndexed: summary.eventsIndexed,
        pagesFetched: summary.pagesFetched,
        truncated: true,
        failureKind: null,
        detail: `RPC truncated or delayed pagination after ${summary.pagesFetched} page(s); persisted checkpoint held at ${persistedAfter} until remaining ledgers up to ${currentLedger} are confirmed.`,
      };
    }

    return {
      ok: true,
      statusCode: 200,
      outcome: "success",
      code: "OK",
      startLedger: targetStartLedger,
      latestLedger: currentLedger,
      persistedCheckpointBefore: persistedBefore,
      persistedCheckpointAfter: persistedAfter,
      eventsIndexed: summary.eventsIndexed,
      pagesFetched: summary.pagesFetched,
      truncated: false,
      failureKind: null,
      detail: `Backfill completed from ledger ${targetStartLedger} to ${persistedAfter} (${summary.eventsIndexed} event(s) across ${summary.pagesFetched} page(s)).`,
    };
  } catch (err) {
    // Restore in-memory checkpoint so a failed override or first-page RPC error
    // never silently advances or corrupts the checkpoint.
    lastProcessedLedger = persistedBefore;
    lastIndexedLedger.set(lastProcessedLedger);
    circuitBreaker.onFailure();
    const kind = classifyRpcFailure(err);
    lastFailureKind = kind;
    indexerErrorsTotal.inc();
    logger.error({ err }, "failed to index events");

    const persistedAfter = readPersistedCheckpoint(db);
    return {
      ok: false,
      statusCode: mapFailureKindToStatusCode(kind),
      outcome: "rpc_error",
      code: mapFailureKindToCode(kind),
      startLedger: targetStartLedger,
      latestLedger: lastObservedLedger,
      persistedCheckpointBefore: persistedBefore,
      persistedCheckpointAfter: persistedAfter,
      eventsIndexed: 0,
      pagesFetched: 0,
      truncated: false,
      failureKind: kind,
      detail: `Backfill RPC request failed (${kind}); persisted checkpoint (${persistedAfter}) was not modified.`,
    };
  } finally {
    isIndexing = false;
  }
}

async function indexEvents(): Promise<void> {
  await runIndexerBackfill();
}

async function indexEventsWithFallback(db: any, currentLedger: number): Promise<ScanBatchSummary> {
  const startLedger = lastProcessedLedger + 1;
  let events;

  try {
    events = await rpcServer!.getEvents({
      startLedger,
      filters: [
        {
          type: "contract",
          contractIds: [contractId!],
        },
      ],
    });
  } catch (err) {
    logger.error({ err }, "RPC getEvents failed in fallback mode");
    throw err;
  }

  const startLedgerForMetrics = lastProcessedLedger;
  const eventList = events.events ?? [];
  const eventCount = eventList.length;
  let maxLedgerSeen = lastProcessedLedger;

  if (eventCount > 0) {
    db.transaction(() => {
      for (const event of eventList) {
        processEvent(db, event);
        eventsIndexedTotal.inc();
        if (typeof event.ledger === "number" && event.ledger > maxLedgerSeen) {
          maxLedgerSeen = event.ledger;
        }
      }
    })();
  }

  // Detect truncated or delayed response in fallback mode:
  // 1) RPC returned a non-empty cursor alongside an empty first page, or
  // 2) RPC returned a continuation cursor indicating more events exist beyond this page.
  const hasMoreCursor = Boolean(events.cursor);
  if (hasMoreCursor && (eventCount === 0 || maxLedgerSeen < currentLedger)) {
    const persistedCheckpoint = readPersistedCheckpoint(db);
    lastProcessedLedger = persistedCheckpoint;
    lastIndexedLedger.set(lastProcessedLedger);
    indexerLedgerLag.set(Math.max(0, currentLedger - lastProcessedLedger));
    logger.warn(
      { startLedger, currentLedger, eventCount },
      "RPC delayed or truncated page in fallback mode; preserving persisted checkpoint",
    );
    return {
      eventsIndexed: eventCount,
      pagesFetched: 1,
      truncated: true,
      checkpoint: persistedCheckpoint,
    };
  }

  const checkpoint = Math.max(currentLedger, maxLedgerSeen);
  saveCheckpoint(db, checkpoint);
  lastProcessedLedger = checkpoint;
  lastIndexedLedger.set(lastProcessedLedger);
  indexerLedgerLag.set(Math.max(0, currentLedger - lastProcessedLedger));
  ledgersScannedTotal.inc(lastProcessedLedger - startLedgerForMetrics);

  return {
    eventsIndexed: eventCount,
    pagesFetched: 1,
    truncated: false,
    checkpoint,
  };
}

async function indexEventsWithCursorPagination(db: any, currentLedger: number): Promise<ScanBatchSummary> {
  const startLedger = lastProcessedLedger + 1;
  let cursor: string | undefined;
  let maxLedgerSeen = lastProcessedLedger;
  const startLedgerForMetrics = lastProcessedLedger;
  let pagesFetched = 0;
  let eventsIndexed = 0;
  let truncated = false;
  const seenCursors = new Set<string>();

  while (true) {
    let request: rpc.Api.GetEventsRequest;

    if (cursor === undefined) {
      request = {
        startLedger,
        filters: [
          {
            type: "contract",
            contractIds: [contractId!],
          },
        ],
      };
    } else {
      request = {
        cursor,
        filters: [
          {
            type: "contract",
            contractIds: [contractId!],
          },
        ],
      };
    }

    let eventsResponse: rpc.Api.GetEventsResponse;

    try {
      eventsResponse = await rpcServer!.getEvents(request);
    } catch (err) {
      logger.error({ err }, "RPC getEvents failed during cursor pagination");
      throw err;
    }

    pagesFetched += 1;
    const events = eventsResponse.events ?? [];
    const nextCursor = eventsResponse.cursor;

    if (events.length === 0) {
      // If the RPC returns an empty page (especially on the first page at startLedger)
      // *with* a continuation cursor, or repeats the same cursor, the RPC page was
      // truncated or delayed before reaching currentLedger.
      if (nextCursor && (pagesFetched === 1 || nextCursor === cursor || seenCursors.has(nextCursor))) {
        truncated = true;
        logger.warn(
          { startLedger, currentLedger, pagesFetched },
          "RPC returned truncated or delayed empty page with continuation cursor; holding checkpoint",
        );
      }
      break;
    }

    db.transaction(() => {
      for (const event of events) {
        processEvent(db, event);
        eventsIndexedTotal.inc();
        eventsIndexed += 1;

        if (event.ledger > maxLedgerSeen) {
          maxLedgerSeen = event.ledger;
        }
      }
    })();

    if (!nextCursor) {
      break;
    }

    // Guard against stalled / repeating cursor truncation from RPC
    if (nextCursor === cursor || seenCursors.has(nextCursor)) {
      truncated = true;
      logger.warn(
        { startLedger, currentLedger, pagesFetched },
        "RPC returned repeating cursor during pagination; treating page as truncated",
      );
      break;
    }

    seenCursors.add(nextCursor);
    cursor = nextCursor;
  }

  if (truncated) {
    // Do not silently change the persisted backfill checkpoint when RPC
    // truncates or delays a page; hold the checkpoint until a complete scan
    // reaches the target ledger.
    const persistedCheckpoint = readPersistedCheckpoint(db);
    lastProcessedLedger = persistedCheckpoint;
    lastIndexedLedger.set(lastProcessedLedger);
    indexerLedgerLag.set(Math.max(0, currentLedger - lastProcessedLedger));
    return {
      eventsIndexed,
      pagesFetched,
      truncated: true,
      checkpoint: persistedCheckpoint,
    };
  }

  const checkpoint = Math.max(currentLedger, maxLedgerSeen);
  saveCheckpoint(db, checkpoint);
  lastProcessedLedger = checkpoint;
  lastIndexedLedger.set(lastProcessedLedger);
  indexerLedgerLag.set(Math.max(0, currentLedger - lastProcessedLedger));
  ledgersScannedTotal.inc(lastProcessedLedger - startLedgerForMetrics);

  return {
    eventsIndexed,
    pagesFetched,
    truncated: false,
    checkpoint,
  };
}

function processEvent(db: any, event: rpc.Api.EventResponse): void {
  try {
    const topic = event.topic.map((t: any) => scValToNative(t));
    const value = scValToNative(event.value);

    if (topic.length < 2) return;

    const eventName = topic[1];
    const timestamp = Math.floor(new Date(event.ledgerClosedAt).getTime() / 1000);

    switch (eventName) {
      case "Created":
        recordEventWithDb(
          db,
          value.stream_id.toString(),
          "created",
          timestamp,
          // actor == sender for Created events
          value.actor ?? value.sender,
          value.total_amount,
          {
            recipient: value.recipient,
            token: value.token,
            startTime: value.start_time,
            endTime: value.end_time,
          },
          event.ledger,
        );
        break;

      case "Claimed":
        recordEventWithDb(
          db,
          value.stream_id.toString(),
          "claimed",
          timestamp,
          // actor == recipient for Claimed events
          value.actor ?? value.recipient,
          value.amount,
          { claimed_amount: value.claimed_amount },
          event.ledger,
        );
        break;

      case "Completed":
        recordEventWithDb(
          db,
          value.stream_id.toString(),
          "completed",
          timestamp,
          value.actor,
          value.total_amount,
          undefined,
          event.ledger,
        );
        break;

      case "Canceled":
        recordEventWithDb(
          db,
          value.stream_id.toString(),
          "canceled",
          timestamp,
          // actor == sender for Canceled events
          value.actor ?? value.sender,
          value.refunded_amount,
          undefined,
          event.ledger,
        );
        break;

      case "Paused":
        recordEventWithDb(
          db,
          value.stream_id.toString(),
          "paused",
          timestamp,
          // actor == sender for Paused events
          value.actor ?? value.sender,
          undefined,
          { paused_at: value.paused_at },
          event.ledger,
        );
        break;

      case "Resumed":
        recordEventWithDb(
          db,
          value.stream_id.toString(),
          "resumed",
          timestamp,
          // actor == sender for Resumed events
          value.actor ?? value.sender,
          undefined,
          { resumed_at: value.resumed_at },
          event.ledger,
        );
        break;

      case "Transfer":
        recordEventWithDb(
          db,
          value.stream_id.toString(),
          "transferred",
          timestamp,
          // actor == old_recipient (the one who authorized the transfer)
          value.actor ?? value.old_recipient,
          undefined,
          { new_recipient: value.new_recipient },
          event.ledger,
        );
        break;

      case "Clawback":
        recordEventWithDb(
          db,
          value.stream_id.toString(),
          "clawback",
          timestamp,
          // actor == admin address
          value.actor,
          value.amount,
          { recipient: value.recipient },
          event.ledger,
        );
        break;

      default:
        logger.warn({ eventName }, "unknown contract event type — skipped");
        break;
    }
  } catch (err) {
    logger.error({ err }, "failed to process event");
  }
}