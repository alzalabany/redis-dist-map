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
  /** Receives recoverable listener and stream-event errors. */
  onError?: (error: unknown) => void;
}

/**
 * A local Map-like view that stays synchronized through Redis.
 *
 * Writes are asynchronous because they are committed to Redis first. Reads and
 * iteration are synchronous because they use the in-process cache.
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
  set(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
  clear(): Promise<void>;
  /** Reloads an authoritative snapshot from Redis. */
  synchronize(): Promise<void>;
  /** Stops the update listener and releases its duplicated Redis connection. */
  destroy(): Promise<void>;
}

type TransactionResults = [error: Error | null, result: unknown][] | null;

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

class RedisDistributedMap<T> implements DistributedMap<T> {
  private readonly cache = new Map<string, T>();
  private readonly streamKey: string;
  private readonly subscriber: Redis;
  private readonly serialize: (value: T) => string;
  private readonly deserialize: (value: string) => T;
  private readonly blockTimeoutMs: number;
  private readonly onError: (error: unknown) => void;
  private running = true;
  private lastReadId = "0-0";
  private lastAppliedId = "0-0";
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
    this.onError =
      options.onError ??
      ((error) => {
        console.error(`[DistributedMap:${name}] Sync error:`, error);
      });
  }

  async initialize(): Promise<void> {
    try {
      const { cursor, values } = await this.loadSnapshot();
      this.replaceCache(values, cursor);
      this.lastReadId = cursor;
      this.listenerPromise = this.listenForUpdates();
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

  async set(key: string, value: T): Promise<void> {
    this.assertRunning();
    const serialized = this.serialize(value);
    const results = await this.client
      .multi()
      .hset(this.name, key, serialized)
      .xadd(
        this.streamKey,
        "*",
        "operation",
        "set",
        "key",
        key,
        "value",
        serialized,
      )
      .exec();
    const streamId = transactionResult(results, 1);
    if (typeof streamId !== "string") {
      throw new Error("Redis XADD did not return a stream ID");
    }
    this.applyIfNewer(streamId, () => this.cache.set(key, value));
  }

  async delete(key: string): Promise<boolean> {
    this.assertRunning();
    const results = await this.client
      .multi()
      .hdel(this.name, key)
      .xadd(this.streamKey, "*", "operation", "delete", "key", key)
      .exec();
    const deleted = transactionResult(results, 0);
    const streamId = transactionResult(results, 1);
    if (typeof deleted !== "number" || typeof streamId !== "string") {
      throw new Error("Redis returned an invalid distributed map delete result");
    }
    this.applyIfNewer(streamId, () => this.cache.delete(key));
    return deleted > 0;
  }

  async clear(): Promise<void> {
    this.assertRunning();
    const results = await this.client
      .multi()
      .del(this.name)
      .xadd(this.streamKey, "*", "operation", "clear")
      .exec();
    const streamId = transactionResult(results, 1);
    if (typeof streamId !== "string") {
      throw new Error("Redis XADD did not return a stream ID");
    }
    this.applyIfNewer(streamId, () => this.cache.clear());
  }

  async synchronize(): Promise<void> {
    this.assertRunning();
    const { cursor, values } = await this.loadSnapshot();

    if (compareStreamIds(cursor, this.lastAppliedId) >= 0) {
      this.replaceCache(values, cursor);
    }
  }

  destroy(): Promise<void> {
    if (this.destroyPromise !== undefined) return this.destroyPromise;

    this.running = false;
    this.subscriber.disconnect();
    this.destroyPromise = this.listenerPromise;
    return this.destroyPromise;
  }

  private async loadSnapshot(): Promise<{
    cursor: string;
    values: Map<string, T>;
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
    for (const [key, serialized] of Object.entries(hash)) {
      if (typeof serialized !== "string") {
        throw new Error(`Redis hash value for ${key} is not a string`);
      }
      values.set(key, this.deserialize(serialized));
    }
    return { cursor, values };
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
      }
    }
  }

  private applyStreamEvent(id: string, fields: string[]): void {
    if (compareStreamIds(id, this.lastAppliedId) <= 0) return;

    const event = streamFieldsToRecord(fields);
    const operation = event.operation ?? (event.value === "" ? "delete" : "set");

    if (operation === "clear") {
      this.cache.clear();
    } else if (operation === "delete") {
      if (event.key === undefined) throw new Error("Delete event has no key");
      this.cache.delete(event.key);
    } else if (operation === "set") {
      if (event.key === undefined || event.value === undefined) {
        throw new Error("Set event has no key or value");
      }
      this.cache.set(event.key, this.deserialize(event.value));
    } else {
      throw new Error(`Unknown distributed map operation: ${operation}`);
    }

    this.lastAppliedId = id;
  }

  private applyIfNewer(id: string, apply: () => unknown): void {
    if (compareStreamIds(id, this.lastAppliedId) <= 0) return;
    apply();
    this.lastAppliedId = id;
  }

  private replaceCache(values: Map<string, T>, cursor: string): void {
    this.cache.clear();
    for (const [key, value] of values) this.cache.set(key, value);
    this.lastAppliedId = cursor;
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

  const map = new RedisDistributedMap(name, options.client, options);
  await map.initialize();
  return map;
}
