import type { Redis } from "ioredis";

/**
 * Configuration for a distributed map.
 *
 * @typeParam T - The value type stored by the map.
 */
export interface DistributedMapOptions<T> {
  /** A connected ioredis client. A duplicate connection is created for updates. */
  client: Redis;
  /** Converts a value into the string stored in Redis. Defaults to JSON.stringify. */
  serialize?: (value: T) => string;
  /** Restores a value from Redis. Defaults to JSON.parse. */
  deserialize?: (value: string) => T;
  /** How long the update listener blocks per Redis XREAD call. Defaults to 1000ms. */
  blockTimeoutMs?: number;
  /** Maximum delay before buffered mutations are persisted. Defaults to 50ms. */
  flushIntervalMs?: number;
  /** How long update batches remain in the Redis Stream. Defaults to 10000ms. */
  historyMs?: number;
  /** How often the Hash snapshot repairs trimmed Stream gaps. Defaults to half of historyMs. */
  synchronizeIntervalMs?: number;
  /** Receives recoverable listener and stream-event errors. */
  onError?: (error: unknown) => void;
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

class RedisDistributedMap<T> implements DistributedMap<T> {
  private readonly cache = new Map<string, T>();
  private readonly serializedCache = new Map<string, string>();
  private readonly pending = new Map<string, PendingMutation<T>>();
  private readonly listeners = new Set<DistributedMapChangeListener<T>>();
  private readonly keyListeners = new Map<
    string,
    Set<DistributedMapKeyChangeListener<T>>
  >();
  private readonly streamKey: string;
  private readonly subscriber: Redis;
  private readonly serialize: (value: T) => string;
  private readonly deserialize: (value: string) => T;
  private readonly blockTimeoutMs: number;
  private readonly flushIntervalMs: number;
  private readonly historyMs: number;
  private readonly synchronizeIntervalMs: number;
  private readonly onError: (error: unknown) => void;
  private running = true;
  private revision = 0;
  private pendingClearRevision: number | undefined;
  private lastReadId = "0-0";
  private lastAppliedId = "0-0";
  private flushTimer: ReturnType<typeof setTimeout> | undefined;
  private synchronizeTimer: ReturnType<typeof setInterval> | undefined;
  private flushChain: Promise<void> = Promise.resolve();
  private listenerPromise: Promise<void> = Promise.resolve();
  private destroyPromise: Promise<void> | undefined;

  constructor(
    readonly name: string,
    private readonly client: Redis,
    options: DistributedMapOptions<T>,
  ) {
    this.streamKey = `${name}:stream`;
    this.subscriber = client.duplicate();
    this.serialize = options.serialize ?? serializeJson;
    this.deserialize = options.deserialize ?? deserializeJson;
    this.blockTimeoutMs = options.blockTimeoutMs ?? 1_000;
    this.flushIntervalMs = options.flushIntervalMs ?? 50;
    this.historyMs = options.historyMs ?? 10_000;
    this.synchronizeIntervalMs =
      options.synchronizeIntervalMs ??
      Math.max(1, Math.floor(this.historyMs / 2));
    this.onError =
      options.onError ??
      ((error) => {
        console.error(`[DistributedMap:${name}] Sync error:`, error);
      });
  }

  async initialize(): Promise<void> {
    try {
      const { cursor, values, serializedValues } = await this.loadSnapshot();
      this.replaceCache(values, serializedValues, cursor);
      this.lastReadId = cursor;
      this.listenerPromise = this.listenForUpdates();
      this.synchronizeTimer = setInterval(() => {
        void this.synchronize().catch((error: unknown) => {
          if (this.running) this.reportError(error);
        });
      }, this.synchronizeIntervalMs);
    } catch (error) {
      this.running = false;
      this.subscriber.disconnect();
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
    const revision = ++this.revision;
    const previousValue = this.cache.get(key);
    const changed = this.serializedCache.get(key) !== serialized;
    this.cache.set(key, value);
    this.serializedCache.set(key, serialized);
    this.pending.set(key, {
      operation: "set",
      value,
      serialized,
      revision,
    });
    if (changed) {
      this.emitChange({
        key,
        operation: "set",
        value,
        previousValue,
        source: "local",
      });
    }
    this.scheduleFlush();
  }

  delete(key: string): boolean {
    this.assertRunning();
    const previousValue = this.cache.get(key);
    const deleted = this.cache.delete(key);
    this.serializedCache.delete(key);
    this.pending.set(key, {
      operation: "delete",
      revision: ++this.revision,
    });
    if (deleted) {
      this.emitChange({
        key,
        operation: "delete",
        value: undefined,
        previousValue,
        source: "local",
      });
    }
    this.scheduleFlush();
    return deleted;
  }

  clear(): void {
    this.assertRunning();
    const previousValues = new Map(this.cache);
    this.cache.clear();
    this.serializedCache.clear();
    this.pending.clear();
    this.pendingClearRevision = ++this.revision;
    for (const [key, previousValue] of previousValues) {
      this.emitChange({
        key,
        operation: "delete",
        value: undefined,
        previousValue,
        source: "local",
      });
    }
    this.scheduleFlush();
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

  async synchronize(): Promise<void> {
    this.assertRunning();
    const { cursor, values, serializedValues } = await this.loadSnapshot();

    if (compareStreamIds(cursor, this.lastAppliedId) >= 0) {
      this.replaceCache(values, serializedValues, cursor, "synchronize");
    }
  }

  destroy(): Promise<void> {
    if (this.destroyPromise !== undefined) return this.destroyPromise;

    this.running = false;
    this.clearFlushTimer();
    if (this.synchronizeTimer !== undefined) {
      clearInterval(this.synchronizeTimer);
      this.synchronizeTimer = undefined;
    }
    this.subscriber.disconnect();
    this.listeners.clear();
    this.keyListeners.clear();
    this.destroyPromise = Promise.all([
      this.listenerPromise,
      this.flushChain,
    ]).then(() => undefined);
    return this.destroyPromise;
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

    let transaction = this.client.multi();
    let resultIndex = 0;
    if (clearRevision !== undefined) {
      transaction = transaction.del(this.name);
      resultIndex += 1;
    } else if (patch.deletes.length > 0) {
      transaction = transaction.hdel(this.name, ...patch.deletes);
      resultIndex += 1;
    }
    if (hashSetArguments.length > 0) {
      transaction = transaction.hset(this.name, ...hashSetArguments);
      resultIndex += 1;
    }

    const cutoffId = `${Math.max(0, Date.now() - this.historyMs)}-0`;
    transaction = transaction.xadd(
      this.streamKey,
      "MINID",
      cutoffId,
      "*",
      "operation",
      "patch",
      "patch",
      JSON.stringify(patch),
    );
    const results = await transaction.exec();
    const streamId = transactionResult(results, resultIndex);
    if (typeof streamId !== "string") {
      throw new Error("Redis XADD did not return a stream ID");
    }

    if (this.pendingClearRevision === clearRevision) {
      this.pendingClearRevision = undefined;
    }
    for (const [key, mutation] of mutations) {
      if (this.pending.get(key)?.revision === mutation.revision) {
        this.pending.delete(key);
      }
    }
    this.advanceAppliedId(streamId);
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

  private async loadSnapshot(): Promise<{
    cursor: string;
    values: Map<string, T>;
    serializedValues: Map<string, string>;
  }> {
    const results = await this.client
      .multi()
      .xrevrange(this.streamKey, "+", "-", "COUNT", 1)
      .hgetall(this.name)
      .exec();
    const streamEntries = transactionResult(results, 0);
    const hash = transactionResult(results, 1);
    if (
      !Array.isArray(streamEntries) ||
      hash === null ||
      typeof hash !== "object"
    ) {
      throw new Error("Redis returned an invalid distributed map snapshot");
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
    return { cursor, values, serializedValues };
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
            this.lastReadId = id;
            try {
              this.applyStreamEvent(id, fields);
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

  private applyStreamEvent(id: string, fields: string[]): void {
    if (compareStreamIds(id, this.lastAppliedId) <= 0) return;

    const previousValues = new Map(this.cache);
    const previousSerialized = new Map(this.serializedCache);
    const event = streamFieldsToRecord(fields);
    const operation = event.operation ?? (event.value === "" ? "delete" : "set");

    if (operation === "patch") {
      if (event.patch === undefined) {
        throw new Error("Patch event has no patch");
      }
      this.applyPatch(parseStreamPatch(event.patch));
    } else if (operation === "clear") {
      this.cache.clear();
      this.serializedCache.clear();
      this.applyPendingOverlay();
    } else if (operation === "delete") {
      if (event.key === undefined) throw new Error("Delete event has no key");
      if (!this.isLocallyShadowed(event.key)) {
        this.cache.delete(event.key);
        this.serializedCache.delete(event.key);
      }
    } else if (operation === "set") {
      if (event.key === undefined || event.value === undefined) {
        throw new Error("Set event has no key or value");
      }
      if (!this.isLocallyShadowed(event.key)) {
        this.cache.set(event.key, this.deserialize(event.value));
        this.serializedCache.set(event.key, event.value);
      }
    } else {
      throw new Error(`Unknown distributed map operation: ${operation}`);
    }

    this.lastAppliedId = id;
    this.emitCacheDiff(previousValues, previousSerialized, "remote");
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

  private advanceAppliedId(id: string): void {
    if (compareStreamIds(id, this.lastAppliedId) <= 0) return;
    this.lastAppliedId = id;
  }

  private replaceCache(
    values: Map<string, T>,
    serializedValues: Map<string, string>,
    cursor: string,
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
  ): void {
    const keys = new Set([...previousValues.keys(), ...this.cache.keys()]);
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

  private reportError(error: unknown): void {
    try {
      this.onError(error);
    } catch {
      // Error reporters must never take down the synchronization loop.
    }
  }

  private assertRunning(): void {
    if (!this.running) {
      throw new Error(`Distributed map ${this.name} is destroyed`);
    }
  }
}

/**
 * Creates a Map-like local cache backed by a Redis hash and synchronized with
 * other instances through a Redis Stream.
 */
export async function createDistributedMap<T>(
  name: string,
  options: DistributedMapOptions<T>,
): Promise<DistributedMap<T>> {
  if (name.length === 0) {
    throw new TypeError("Distributed map name cannot be empty");
  }
  if (!options?.client) {
    throw new TypeError("Redis client is required to create a distributed map");
  }
  if (options.blockTimeoutMs !== undefined && options.blockTimeoutMs <= 0) {
    throw new RangeError("blockTimeoutMs must be greater than zero");
  }
  if (options.flushIntervalMs !== undefined && options.flushIntervalMs <= 0) {
    throw new RangeError("flushIntervalMs must be greater than zero");
  }
  if (options.historyMs !== undefined && options.historyMs <= 0) {
    throw new RangeError("historyMs must be greater than zero");
  }
  if (
    options.synchronizeIntervalMs !== undefined &&
    options.synchronizeIntervalMs <= 0
  ) {
    throw new RangeError("synchronizeIntervalMs must be greater than zero");
  }

  const map = new RedisDistributedMap(name, options.client, options);
  await map.initialize();
  return map;
}
