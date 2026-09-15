import type { Redis } from "ioredis";

/**
 * Configuration for a write-only distributed map publisher.
 *
 * @typeParam T - The value type stored by the map.
 */
export interface DistributedMapWriterOptions<T> {
  /** A connected ioredis client. The writer does not duplicate it. */
  client: Redis;
  /** Converts a value into the string stored in Redis. Defaults to JSON.stringify. */
  serialize?: (value: T) => string;
  /** Maximum delay before buffered mutations are persisted. Defaults to 50ms. */
  flushIntervalMs?: number;
  /** How long update batches remain in the Redis Stream. Defaults to 10000ms. */
  historyMs?: number;
  /** Receives recoverable background write errors. */
  onError?: (error: unknown) => void;
}

/**
 * Configuration for a distributed map.
 *
 * @typeParam T - The value type stored by the map.
 */
export interface DistributedMapOptions<T>
  extends DistributedMapWriterOptions<T> {
  /** Restores a value from Redis. Defaults to JSON.parse. */
  deserialize?: (value: string) => T;
  /** How long the update listener blocks per Redis XREAD call. Defaults to 1000ms. */
  blockTimeoutMs?: number;
  /** Receives recoverable listener and stream-event errors. */
  onError?: (error: unknown) => void;
}

/**
 * Configuration for an atomic Redis-backed shared counter.
 */
export interface SharedCounterOptions {
  /** A connected ioredis client. The counter does not duplicate it. */
  client: Redis;
  /**
   * How long each counter key lives, starting with its first increment.
   * Omit for counters that should not expire.
   */
  ttlMs?: number;
}

/**
 * An atomic counter namespace backed directly by Redis.
 *
 * Unlike DistributedMap mutations, increments are never buffered or cached.
 */
export interface SharedCounter {
  readonly name: string;
  /** Atomically increments one key and returns its new value. */
  inc(key: string): Promise<number>;
}

export type DistributedMapChangeSource =
  | "local"
  | "remote"
  | "synchronize";

export interface DistributedMapChange<T> {
  readonly key: string;
  readonly operation: "set" | "delete";
  readonly value: T | undefined;
  readonly previousValue: T | undefined;
  readonly source: DistributedMapChangeSource;
}

export type DistributedMapChangeListener<T> = (
  change: DistributedMapChange<T>,
) => void;

export type DistributedMapKeyChangeListener<T> = (
  value: T | undefined,
  change: DistributedMapChange<T>,
) => void;

/**
 * A local Map-like view that stays synchronized through Redis.
 *
 * Reads, writes, and iteration update the in-process cache synchronously.
 * Buffered writes become durable when the automatic timer or flush() runs.
 */
export interface DistributedMap<T> extends Iterable<[string, T]> {
  readonly name: string;
  readonly size: number;
  get(key: string): T | undefined;
  has(key: string): boolean;
  entries(): IterableIterator<[string, T]>;
  keys(): IterableIterator<string>;
  values(): IterableIterator<T>;
  forEach(callback: (value: T, key: string) => void): void;
  set(key: string, value: T): void;
  delete(key: string): boolean;
  clear(): void;
  /** Subscribes to changes for every key. Returns an idempotent unsubscribe function. */
  onChange(listener: DistributedMapChangeListener<T>): () => void;
  /** Subscribes to changes for one key. Returns an idempotent unsubscribe function. */
  onChange(key: string, listener: DistributedMapKeyChangeListener<T>): () => void;
  /** Persists and broadcasts every mutation queued before this call. */
  flush(): Promise<void>;
  /** Reloads an authoritative snapshot from Redis. */
  synchronize(): Promise<void>;
  /** Stops timers/listening without flushing and releases the duplicated connection. */
  destroy(): Promise<void>;
}

/**
 * A write-only publisher for a Redis-backed distributed map.
 *
 * Mutations are coalesced and persisted with the same atomic Hash + Stream
 * patches as DistributedMap, without loading or synchronizing local state.
 */
export interface DistributedMapWriter<T> {
  readonly name: string;
  set(key: string, value: T): void;
  /** Queues a deletion without checking whether the remote key exists. */
  delete(key: string): void;
  clear(): void;
  /** Persists and broadcasts every mutation queued before this call. */
  flush(): Promise<void>;
  /** Stops automatic flushing without flushing pending mutations. */
  destroy(): Promise<void>;
}

type TransactionResults = [error: Error | null, result: unknown][] | null;

type PendingMutation<T> =
  | {
    operation: "set";
    value: T;
    serialized: string;
    revision: number;
  }
  | {
    operation: "delete";
    revision: number;
  };

interface StreamPatch {
  clear: boolean;
  sets: [string, string][];
  deletes: string[];
}

const SHARED_COUNTER_INCREMENT_SCRIPT = `
local current = redis.call("INCR", KEYS[1])
if current == 1 and ARGV[1] ~= "" then
  redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return current
`;

const DISTRIBUTED_MAP_FLUSH_SCRIPT = `
local argument = 6

if ARGV[3] == "1" then
  redis.call("DEL", KEYS[1])
else
  for _ = 1, tonumber(ARGV[5]) do
    redis.call("HDEL", KEYS[1], ARGV[argument])
    argument = argument + 1
  end
end

for _ = 1, tonumber(ARGV[4]) do
  redis.call("HSET", KEYS[1], ARGV[argument], ARGV[argument + 1])
  argument = argument + 2
end

local revision = redis.call("INCR", KEYS[3])
local streamId = redis.call(
  "XADD",
  KEYS[2],
  "MINID",
  ARGV[1],
  "*",
  "operation",
  "patch",
  "revision",
  tostring(revision),
  "patch",
  ARGV[2]
)
return { streamId, revision }
`;

function transactionResult(results: TransactionResults, index: number): unknown {
  if (results === null) {
    throw new Error("Redis transaction returned no results");
  }

  for (const [error] of results) {
    if (error !== null) throw error;
  }

  const entry = results[index];
  if (entry === undefined) {
    throw new Error(`Redis transaction result ${index} is missing`);
  }
  return entry[1];
}

function serializeJson<T>(value: T): string {
  const serialized = JSON.stringify(value);
  if (serialized === undefined) {
    throw new TypeError("Distributed map values must be JSON serializable");
  }
  return serialized;
}

function deserializeJson<T>(value: string): T {
  return JSON.parse(value) as T;
}

function compareStreamIds(left: string, right: string): number {
  const leftMatch = /^(\d+)-(\d+)$/.exec(left);
  const rightMatch = /^(\d+)-(\d+)$/.exec(right);
  if (leftMatch === null || rightMatch === null) {
    throw new Error(
      `Invalid Redis stream ID: ${leftMatch === null ? left : right}`,
    );
  }

  const leftTime = BigInt(leftMatch[1] ?? "0");
  const rightTime = BigInt(rightMatch[1] ?? "0");
  if (leftTime !== rightTime) return leftTime < rightTime ? -1 : 1;

  const leftSequence = BigInt(leftMatch[2] ?? "0");
  const rightSequence = BigInt(rightMatch[2] ?? "0");
  return leftSequence === rightSequence
    ? 0
    : leftSequence < rightSequence
      ? -1
      : 1;
}

function streamFieldsToRecord(fields: string[]): Record<string, string> {
  if (fields.length % 2 !== 0) {
    throw new Error("Distributed map stream event has an unmatched field");
  }

  const record: Record<string, string> = {};
  for (let index = 0; index < fields.length; index += 2) {
    const field = fields[index];
    const value = fields[index + 1];
    if (field === undefined || value === undefined) {
      throw new Error("Distributed map stream event is malformed");
    }
    record[field] = value;
  }
  return record;
}

function parseStreamPatch(value: string): StreamPatch {
  const parsed: unknown = JSON.parse(value);
  if (parsed === null || typeof parsed !== "object") {
    throw new Error("Distributed map patch is not an object");
  }

  const candidate = parsed as Record<string, unknown>;
  if (
    typeof candidate.clear !== "boolean" ||
    !Array.isArray(candidate.sets) ||
    !Array.isArray(candidate.deletes)
  ) {
    throw new Error("Distributed map patch is malformed");
  }

  const sets: [string, string][] = [];
  for (const entry of candidate.sets) {
    if (
      !Array.isArray(entry) ||
      entry.length !== 2 ||
      typeof entry[0] !== "string" ||
      typeof entry[1] !== "string"
    ) {
      throw new Error("Distributed map patch contains an invalid set");
    }
    sets.push([entry[0], entry[1]]);
  }

  const deletes: string[] = [];
  for (const key of candidate.deletes) {
    if (typeof key !== "string") {
      throw new Error("Distributed map patch contains an invalid delete");
    }
    deletes.push(key);
  }

  return { clear: candidate.clear, sets, deletes };
}

class RedisSharedCounter implements SharedCounter {
  private readonly keyPrefix: string;

  constructor(
    readonly name: string,
    private readonly client: Redis,
    private readonly ttlMs: number | undefined,
  ) {
    const encodedName = Buffer.from(name, "utf8").toString("base64url");
    this.keyPrefix = `redis-dist-map:counter:${encodedName}:`;
  }

  async inc(key: string): Promise<number> {
    const result = await this.client.eval(
      SHARED_COUNTER_INCREMENT_SCRIPT,
      1,
      `${this.keyPrefix}${key}`,
      this.ttlMs === undefined ? "" : String(this.ttlMs),
    );
    if (typeof result !== "number") {
      throw new Error("Redis INCR returned a non-numeric value");
    }
    return result;
  }
}

class RedisDistributedMapWriter<T> implements DistributedMapWriter<T> {
  protected readonly pending = new Map<string, PendingMutation<T>>();
  protected readonly streamKey: string;
  protected readonly revisionKey: string;
  protected readonly serialize: (value: T) => string;
  protected running = true;
  protected revision = 0;
  protected pendingClearRevision: number | undefined;
  private readonly flushIntervalMs: number;
  private readonly historyMs: number;
  private readonly onError: (error: unknown) => void;
  private flushTimer: ReturnType<typeof setTimeout> | undefined;
  private flushChain: Promise<void> = Promise.resolve();
  private destroyPromise: Promise<void> | undefined;

  constructor(
    readonly name: string,
    protected readonly client: Redis,
    options: DistributedMapWriterOptions<T>,
  ) {
    this.streamKey = `${name}:stream`;
    this.revisionKey = `${name}:revision`;
    this.serialize = options.serialize ?? serializeJson;
    this.flushIntervalMs = options.flushIntervalMs ?? 50;
    this.historyMs = options.historyMs ?? 10_000;
    this.onError =
      options.onError ??
      ((error) => {
        console.error(`[DistributedMap:${name}] Background error:`, error);
      });
  }

  set(key: string, value: T): void {
    this.assertRunning();
    this.queueSet(key, value, this.serialize(value));
  }

  delete(key: string): void {
    this.assertRunning();
    this.queueDelete(key);
  }

  clear(): void {
    this.assertRunning();
    this.queueClear();
  }

  async flush(): Promise<void> {
    this.assertRunning();
    this.clearFlushTimer();
    const targetRevision = this.revision;
    const task = this.flushChain.then(() =>
      this.flushThrough(targetRevision),
    );
    this.flushChain = task.catch(() => undefined);
    return task;
  }

  destroy(): Promise<void> {
    if (this.destroyPromise !== undefined) return this.destroyPromise;

    this.running = false;
    this.clearFlushTimer();
    this.destroyPromise = this.flushChain.then(() => undefined);
    return this.destroyPromise;
  }

  protected queueSet(key: string, value: T, serialized: string): void {
    this.pending.set(key, {
      operation: "set",
      value,
      serialized,
      revision: ++this.revision,
    });
    this.scheduleFlush();
  }

  protected queueDelete(key: string): void {
    this.pending.set(key, {
      operation: "delete",
      revision: ++this.revision,
    });
    this.scheduleFlush();
  }

  protected queueClear(): void {
    this.pending.clear();
    this.pendingClearRevision = ++this.revision;
    this.scheduleFlush();
  }

  protected reportError(error: unknown): void {
    try {
      this.onError(error);
    } catch {
      // Error reporters must never take down background work.
    }
  }

  protected assertRunning(): void {
    if (!this.running) {
      throw new Error(`Distributed map ${this.name} is destroyed`);
    }
  }

  private async flushThrough(targetRevision: number): Promise<void> {
    while (this.hasPendingThrough(targetRevision)) {
      await this.flushOnce();
    }
  }

  private async flushOnce(): Promise<void> {
    const clearRevision = this.pendingClearRevision;
    const mutations = new Map(this.pending);
    if (clearRevision === undefined && mutations.size === 0) return;

    const patch: StreamPatch = {
      clear: clearRevision !== undefined,
      sets: [],
      deletes: [],
    };
    const hashSetArguments: string[] = [];

    for (const [key, mutation] of mutations) {
      if (mutation.operation === "set") {
        patch.sets.push([key, mutation.serialized]);
        hashSetArguments.push(key, mutation.serialized);
      } else if (clearRevision === undefined) {
        patch.deletes.push(key);
      }
    }

    const cutoffId = `${Math.max(0, Date.now() - this.historyMs)}-0`;
    const result = await this.client.eval(
      DISTRIBUTED_MAP_FLUSH_SCRIPT,
      3,
      this.name,
      this.streamKey,
      this.revisionKey,
      cutoffId,
      JSON.stringify(patch),
      clearRevision === undefined ? "0" : "1",
      String(patch.sets.length),
      String(patch.deletes.length),
      ...patch.deletes,
      ...hashSetArguments,
    );
    if (
      !Array.isArray(result) ||
      typeof result[0] !== "string" ||
      typeof result[1] !== "number" ||
      !Number.isSafeInteger(result[1])
    ) {
      throw new Error("Redis map flush returned an invalid result");
    }
    if (this.pendingClearRevision === clearRevision) {
      this.pendingClearRevision = undefined;
    }
    for (const [key, mutation] of mutations) {
      if (this.pending.get(key)?.revision === mutation.revision) {
        this.pending.delete(key);
      }
    }
  }

  private hasPendingThrough(targetRevision: number): boolean {
    if (
      this.pendingClearRevision !== undefined &&
      this.pendingClearRevision <= targetRevision
    ) {
      return true;
    }
    for (const mutation of this.pending.values()) {
      if (mutation.revision <= targetRevision) return true;
    }
    return false;
  }

  private scheduleFlush(delayMs = this.flushIntervalMs): void {
    if (!this.running || this.flushTimer !== undefined) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined;
      void this.flush().catch((error: unknown) => {
        if (!this.running) return;
        this.reportError(error);
        this.scheduleFlush(250);
      });
    }, delayMs);
  }

  private clearFlushTimer(): void {
    if (this.flushTimer === undefined) return;
    clearTimeout(this.flushTimer);
    this.flushTimer = undefined;
  }
}

class RedisDistributedMap<T>
  extends RedisDistributedMapWriter<T>
  implements DistributedMap<T> {
  private readonly cache = new Map<string, T>();
  private readonly serializedCache = new Map<string, string>();
  private readonly listeners = new Set<DistributedMapChangeListener<T>>();
  private readonly keyListeners = new Map<
    string,
    Set<DistributedMapKeyChangeListener<T>>
  >();
  private readonly subscriber: Redis;
  private readonly deserialize: (value: string) => T;
  private readonly blockTimeoutMs: number;
  private lastReadId = "0-0";
  private lastAppliedId = "0-0";
  private lastAppliedRevision = 0;
  private listenerPromise: Promise<void> = Promise.resolve();
  private mapDestroyPromise: Promise<void> | undefined;

  constructor(
    name: string,
    client: Redis,
    options: DistributedMapOptions<T>,
  ) {
    super(name, client, options);
    this.subscriber = client.duplicate();
    this.deserialize = options.deserialize ?? deserializeJson;
    this.blockTimeoutMs = options.blockTimeoutMs ?? 1_000;
  }

  async initialize(): Promise<void> {
    try {
      const { cursor, revision, values, serializedValues } =
        await this.loadSnapshot();
      this.replaceCache(values, serializedValues, cursor, revision);
      this.lastReadId = cursor;
      this.listenerPromise = this.listenForUpdates();
    } catch (error) {
      this.subscriber.disconnect();
      await super.destroy();
      throw error;
    }
  }

  get size(): number {
    return this.cache.size;
  }

  get(key: string): T | undefined {
    return this.cache.get(key);
  }

  has(key: string): boolean {
    return this.cache.has(key);
  }

  entries(): IterableIterator<[string, T]> {
    return this.cache.entries();
  }

  keys(): IterableIterator<string> {
    return this.cache.keys();
  }

  values(): IterableIterator<T> {
    return this.cache.values();
  }

  forEach(callback: (value: T, key: string) => void): void {
    this.cache.forEach(callback);
  }

  [Symbol.iterator](): IterableIterator<[string, T]> {
    return this.entries();
  }

  onChange(listener: DistributedMapChangeListener<T>): () => void;
  onChange(
    key: string,
    listener: DistributedMapKeyChangeListener<T>,
  ): () => void;
  onChange(
    keyOrListener: string | DistributedMapChangeListener<T>,
    keyListener?: DistributedMapKeyChangeListener<T>,
  ): () => void {
    this.assertRunning();

    if (typeof keyOrListener === "function") {
      const listener = keyOrListener;
      this.listeners.add(listener);
      return this.createUnsubscribe(() => {
        this.listeners.delete(listener);
      });
    }

    if (keyListener === undefined) {
      throw new TypeError("A key change listener is required");
    }
    const key = keyOrListener;
    const listeners =
      this.keyListeners.get(key) ??
      new Set<DistributedMapKeyChangeListener<T>>();
    listeners.add(keyListener);
    this.keyListeners.set(key, listeners);
    return this.createUnsubscribe(() => {
      listeners.delete(keyListener);
      if (listeners.size === 0) this.keyListeners.delete(key);
    });
  }

  set(key: string, value: T): void {
    this.assertRunning();
    const serialized = this.serialize(value);
    if (this.serializedCache.get(key) === serialized) return;

    const previousValue = this.cache.get(key);
    this.cache.set(key, value);
    this.serializedCache.set(key, serialized);
    this.queueSet(key, value, serialized);
    this.emitChange({
      key,
      operation: "set",
      value,
      previousValue,
      source: "local",
    });
  }

  delete(key: string): boolean {
    this.assertRunning();
    const previousValue = this.cache.get(key);
    const deleted = this.cache.delete(key);
    this.serializedCache.delete(key);
    this.queueDelete(key);
    if (deleted) {
      this.emitChange({
        key,
        operation: "delete",
        value: undefined,
        previousValue,
        source: "local",
      });
    }
    return deleted;
  }

  clear(): void {
    this.assertRunning();
    const previousValues = new Map(this.cache);
    this.cache.clear();
    this.serializedCache.clear();
    this.queueClear();
    for (const [key, previousValue] of previousValues) {
      this.emitChange({
        key,
        operation: "delete",
        value: undefined,
        previousValue,
        source: "local",
      });
    }
  }

  async synchronize(): Promise<void> {
    this.assertRunning();
    const { cursor, revision, values, serializedValues } =
      await this.loadSnapshot();

    if (revision >= this.lastAppliedRevision) {
      this.replaceCache(
        values,
        serializedValues,
        cursor,
        revision,
        "synchronize",
      );
      this.lastReadId = cursor;
    }
  }

  destroy(): Promise<void> {
    if (this.mapDestroyPromise !== undefined) return this.mapDestroyPromise;

    const writerDestroy = super.destroy();
    this.subscriber.disconnect();
    this.listeners.clear();
    this.keyListeners.clear();
    this.mapDestroyPromise = Promise.all([
      this.listenerPromise,
      writerDestroy,
    ]).then(() => undefined);
    return this.mapDestroyPromise;
  }

  private async loadSnapshot(): Promise<{
    cursor: string;
    revision: number;
    values: Map<string, T>;
    serializedValues: Map<string, string>;
  }> {
    const results = await this.client
      .multi()
      .get(this.revisionKey)
      .xrevrange(this.streamKey, "+", "-", "COUNT", 1)
      .hgetall(this.name)
      .exec();
    const rawRevision = transactionResult(results, 0);
    const streamEntries = transactionResult(results, 1);
    const hash = transactionResult(results, 2);
    if (
      (rawRevision !== null && typeof rawRevision !== "string") ||
      !Array.isArray(streamEntries) ||
      hash === null ||
      typeof hash !== "object"
    ) {
      throw new Error("Redis returned an invalid distributed map snapshot");
    }
    const revision = rawRevision === null ? 0 : Number(rawRevision);
    if (!Number.isSafeInteger(revision) || revision < 0) {
      throw new Error("Redis returned an invalid distributed map revision");
    }

    const newestEntry: unknown = streamEntries[0];
    const cursor =
      newestEntry === undefined
        ? "0-0"
        : Array.isArray(newestEntry) && typeof newestEntry[0] === "string"
          ? newestEntry[0]
          : undefined;
    if (cursor === undefined) {
      throw new Error("Redis returned an invalid distributed map stream entry");
    }

    const values = new Map<string, T>();
    const serializedValues = new Map<string, string>();
    for (const [key, serialized] of Object.entries(hash)) {
      if (typeof serialized !== "string") {
        throw new Error(`Redis hash value for ${key} is not a string`);
      }
      values.set(key, this.deserialize(serialized));
      serializedValues.set(key, serialized);
    }
    return { cursor, revision, values, serializedValues };
  }

  private async listenForUpdates(): Promise<void> {
    while (this.running) {
      try {
        const results = await this.subscriber.xread(
          "BLOCK",
          this.blockTimeoutMs,
          "STREAMS",
          this.streamKey,
          this.lastReadId,
        );
        if (!this.running || results === null) continue;

        for (const [, messages] of results) {
          for (const [id, fields] of messages) {
            if (compareStreamIds(id, this.lastReadId) > 0) {
              this.lastReadId = id;
            }
            try {
              const applied = this.applyStreamEvent(id, fields);
              if (!applied) await this.synchronize();
            } catch (error) {
              this.reportError(error);
              await this.synchronize();
            }
          }
        }
      } catch (error) {
        if (!this.running) break;
        this.reportError(error);
        await new Promise((resolve) => setTimeout(resolve, 250));
        try {
          await this.synchronize();
        } catch (synchronizeError) {
          if (this.running) this.reportError(synchronizeError);
        }
      }
    }
  }

  private applyStreamEvent(id: string, fields: string[]): boolean {
    if (compareStreamIds(id, this.lastAppliedId) <= 0) return true;

    const event = streamFieldsToRecord(fields);
    if (event.operation !== "patch" || event.patch === undefined) {
      throw new Error("Distributed map stream event is not a patch");
    }
    const revision = Number(event.revision);
    if (!Number.isSafeInteger(revision) || revision <= 0) {
      throw new Error("Distributed map stream event has an invalid revision");
    }
    if (revision <= this.lastAppliedRevision) return true;
    if (revision !== this.lastAppliedRevision + 1) return false;

    const patch = parseStreamPatch(event.patch);
    const affectedKeys = patch.clear
      ? undefined
      : new Set([
        ...patch.deletes,
        ...patch.sets.map(([key]) => key),
      ]);
    this.applyRemoteMutation(id, revision, affectedKeys, () =>
      this.applyPatch(patch),
    );
    return true;
  }

  private applyRemoteMutation(
    id: string,
    revision: number,
    affectedKeys: ReadonlySet<string> | undefined,
    apply: () => void,
  ): void {
    const previousValues = new Map<string, T>();
    const previousSerialized = new Map<string, string>();

    if (affectedKeys === undefined) {
      for (const [key, value] of this.cache) previousValues.set(key, value);
      for (const [key, value] of this.serializedCache) {
        previousSerialized.set(key, value);
      }
    } else {
      for (const key of affectedKeys) {
        if (!this.cache.has(key)) continue;
        previousValues.set(key, this.cache.get(key) as T);
        previousSerialized.set(key, this.serializedCache.get(key) as string);
      }
    }

    apply();
    this.lastAppliedId = id;
    this.lastAppliedRevision = revision;
    this.emitCacheDiff(
      previousValues,
      previousSerialized,
      "remote",
      affectedKeys,
    );
  }

  private applyPatch(patch: StreamPatch): void {
    if (patch.clear) {
      this.cache.clear();
      this.serializedCache.clear();
    }

    for (const key of patch.deletes) {
      if (!this.isLocallyShadowed(key)) {
        this.cache.delete(key);
        this.serializedCache.delete(key);
      }
    }
    for (const [key, serialized] of patch.sets) {
      if (!this.isLocallyShadowed(key)) {
        this.cache.set(key, this.deserialize(serialized));
        this.serializedCache.set(key, serialized);
      }
    }
    if (patch.clear) this.applyPendingOverlay();
  }

  private isLocallyShadowed(key: string): boolean {
    return this.pendingClearRevision !== undefined || this.pending.has(key);
  }

  private applyPendingOverlay(): void {
    if (this.pendingClearRevision !== undefined) {
      this.cache.clear();
      this.serializedCache.clear();
    }
    for (const [key, mutation] of this.pending) {
      if (mutation.operation === "set") {
        this.cache.set(key, mutation.value);
        this.serializedCache.set(key, mutation.serialized);
      } else {
        this.cache.delete(key);
        this.serializedCache.delete(key);
      }
    }
  }

  private replaceCache(
    values: Map<string, T>,
    serializedValues: Map<string, string>,
    cursor: string,
    revision: number,
    source?: DistributedMapChangeSource,
  ): void {
    const previousValues = source === undefined ? undefined : new Map(this.cache);
    const previousSerialized =
      source === undefined ? undefined : new Map(this.serializedCache);
    this.cache.clear();
    this.serializedCache.clear();
    for (const [key, value] of values) this.cache.set(key, value);
    for (const [key, value] of serializedValues) {
      this.serializedCache.set(key, value);
    }
    this.applyPendingOverlay();
    this.lastAppliedId = cursor;
    this.lastAppliedRevision = revision;
    if (
      source !== undefined &&
      previousValues !== undefined &&
      previousSerialized !== undefined
    ) {
      this.emitCacheDiff(previousValues, previousSerialized, source);
    }
  }

  private emitCacheDiff(
    previousValues: Map<string, T>,
    previousSerialized: Map<string, string>,
    source: DistributedMapChangeSource,
    affectedKeys?: ReadonlySet<string>,
  ): void {
    const keys =
      affectedKeys ??
      new Set([...previousValues.keys(), ...this.cache.keys()]);
    for (const key of keys) {
      const existed = previousValues.has(key);
      const exists = this.cache.has(key);
      if (!exists) {
        if (existed) {
          this.emitChange({
            key,
            operation: "delete",
            value: undefined,
            previousValue: previousValues.get(key),
            source,
          });
        }
        continue;
      }

      if (
        !existed ||
        previousSerialized.get(key) !== this.serializedCache.get(key)
      ) {
        this.emitChange({
          key,
          operation: "set",
          value: this.cache.get(key),
          previousValue: previousValues.get(key),
          source,
        });
      }
    }
  }

  private emitChange(change: DistributedMapChange<T>): void {
    for (const listener of [...this.listeners]) {
      try {
        listener(change);
      } catch (error) {
        this.reportError(error);
      }
    }
    for (const listener of [...(this.keyListeners.get(change.key) ?? [])]) {
      try {
        listener(change.value, change);
      } catch (error) {
        this.reportError(error);
      }
    }
  }

  private createUnsubscribe(remove: () => void): () => void {
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      remove();
    };
  }
}

function validateWriterOptions<T>(
  name: string,
  options: DistributedMapWriterOptions<T>,
): void {
  if (name.length === 0) {
    throw new TypeError("Distributed map name cannot be empty");
  }
  if (!options?.client) {
    throw new TypeError("Redis client is required to create a distributed map");
  }
  if (options.flushIntervalMs !== undefined && options.flushIntervalMs <= 0) {
    throw new RangeError("flushIntervalMs must be greater than zero");
  }
  if (options.historyMs !== undefined && options.historyMs <= 0) {
    throw new RangeError("historyMs must be greater than zero");
  }
}

/**
 * Creates an atomic Redis-backed counter namespace.
 *
 * Each inc() call is an immediate Redis round trip. When ttlMs is configured,
 * the expiry is set atomically on the first increment and is not extended by
 * later increments.
 */
export function createSharedCounter(
  name: string,
  options: SharedCounterOptions,
): SharedCounter {
  if (name.length === 0) {
    throw new TypeError("Shared counter name cannot be empty");
  }
  if (!options?.client) {
    throw new TypeError("Redis client is required to create a shared counter");
  }
  if (
    options.ttlMs !== undefined &&
    (!Number.isSafeInteger(options.ttlMs) || options.ttlMs <= 0)
  ) {
    throw new RangeError("ttlMs must be a positive safe integer");
  }
  return new RedisSharedCounter(name, options.client, options.ttlMs);
}

/**
 * Creates a write-only publisher without loading a snapshot or opening a
 * duplicated Redis connection.
 */
export function createDistributedMapWriter<T>(
  name: string,
  options: DistributedMapWriterOptions<T>,
): DistributedMapWriter<T> {
  validateWriterOptions(name, options);
  return new RedisDistributedMapWriter(name, options.client, options);
}

/**
 * Creates a Map-like local cache backed by a Redis hash and synchronized with
 * other instances through a Redis Stream.
 */
export async function createDistributedMap<T>(
  name: string,
  options: DistributedMapOptions<T>,
): Promise<DistributedMap<T>> {
  validateWriterOptions(name, options);
  if (options.blockTimeoutMs !== undefined && options.blockTimeoutMs <= 0) {
    throw new RangeError("blockTimeoutMs must be greater than zero");
  }

  const map = new RedisDistributedMap(name, options.client, options);
  await map.initialize();
  return map;
}
