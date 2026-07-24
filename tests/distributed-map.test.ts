import { once } from "node:events";
import { createServer } from "node:net";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { Redis } from "ioredis";
import { autorun, isObservable } from "mobx";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  createDistributedMap,
  type DistributedMap,
  type DistributedMapOptions,
} from "../src/index.js";
import { createMobxDistributedMap } from "../src/mobx.js";

let server: ChildProcessWithoutNullStreams;
let client: Redis;
let port: number;
const maps: DistributedMap<unknown>[] = [];

async function freePort(): Promise<number> {
  const socket = createServer();
  socket.listen(0, "127.0.0.1");
  await once(socket, "listening");
  const address = socket.address();
  if (address === null || typeof address === "string") {
    throw new Error("Could not reserve a Redis port");
  }
  socket.close();
  await once(socket, "close");
  return address.port;
}

async function waitFor(
  condition: () => boolean,
  timeoutMs = 3_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error("Timed out waiting for distributed map synchronization");
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

async function makeMap<T>(
  name: string,
  options: Partial<DistributedMapOptions<T>> = {},
): Promise<DistributedMap<T>> {
  const map = await createDistributedMap<T>(name, {
    client,
    blockTimeoutMs: 50,
    ...options,
  });
  maps.push(map as DistributedMap<unknown>);
  return map;
}

beforeAll(async () => {
  port = await freePort();
  server = spawn(
    "redis-server",
    [
      "--port",
      String(port),
      "--save",
      "",
      "--appendonly",
      "no",
      "--bind",
      "127.0.0.1",
    ],
    { stdio: ["pipe", "pipe", "pipe"] },
  );
  await new Promise<void>((resolve, reject) => {
    server.stdout.on("data", (chunk: Buffer) => {
      if (chunk.toString().includes("Ready to accept connections")) resolve();
    });
    server.once("exit", (code) => {
      reject(new Error(`redis-server exited before startup with code ${code}`));
    });
  });
  client = new Redis(port, "127.0.0.1", {
    lazyConnect: false,
    maxRetriesPerRequest: 1,
  });
  await client.ping();
});

afterAll(async () => {
  await Promise.all(maps.map((map) => map.destroy()));
  if (client) await client.quit();
  if (server && !server.killed) {
    server.kill("SIGTERM");
    await once(server, "exit");
  }
});

describe("createDistributedMap", () => {
  it("behaves like a synchronous Map for reads and iteration", async () => {
    const map = await makeMap<{ score: number }>("test:map-interface");

    await map.set("ada", { score: 42 });
    await map.set("grace", { score: 99 });

    expect(map.size).toBe(2);
    expect(map.get("ada")).toEqual({ score: 42 });
    expect(map.has("grace")).toBe(true);
    expect([...map.keys()]).toEqual(["ada", "grace"]);
    expect([...map.values()]).toEqual([{ score: 42 }, { score: 99 }]);
    expect([...map]).toEqual([
      ["ada", { score: 42 }],
      ["grace", { score: 99 }],
    ]);

    const visited: string[] = [];
    map.forEach((value, key) => visited.push(`${key}:${value.score}`));
    expect(visited).toEqual(["ada:42", "grace:99"]);
  });

  it("coalesces local writes into one durable patch on flush", async () => {
    const map = await makeMap<number>("test:write-behind", {
      flushIntervalMs: 10_000,
    });

    map.set("a", 1);
    map.set("a", 2);
    map.set("b", 3);

    expect(map.get("a")).toBe(2);
    expect(await client.hgetall("test:write-behind")).toEqual({});

    await map.flush();

    expect(await client.hgetall("test:write-behind")).toEqual({
      a: "2",
      b: "3",
    });
    expect(await client.xlen("test:write-behind:stream")).toBe(1);

    const entries = await client.xrevrange(
      "test:write-behind:stream",
      "+",
      "-",
      "COUNT",
      1,
    );
    const fields = entries[0]?.[1] ?? [];
    const patchIndex = fields.lastIndexOf("patch");
    expect(JSON.parse(fields[patchIndex + 1] ?? "")).toEqual({
      clear: false,
      sets: [
        ["a", "2"],
        ["b", "3"],
      ],
      deletes: [],
    });
  });

  it("synchronizes writes, deletes, and clears across instances", async () => {
    const first = await makeMap<number>("test:replication");
    const second = await makeMap<number>("test:replication");

    await first.set("requests", 7);
    await waitFor(() => second.get("requests") === 7);

    expect(await second.delete("requests")).toBe(true);
    await waitFor(() => !first.has("requests"));
    expect(await second.delete("requests")).toBe(false);

    await first.set("a", 1);
    await first.set("b", 2);
    await waitFor(() => second.size === 2);
    await second.clear();
    await waitFor(() => first.size === 0);
  });

  it("notifies global and key listeners for local, remote, and repaired changes", async () => {
    const writer = await makeMap<number>("test:change-listeners", {
      flushIntervalMs: 10_000,
    });
    const reader = await makeMap<number>("test:change-listeners");
    const localValues: Array<number | undefined> = [];
    const keyChanges: Array<{
      value: number | undefined;
      previousValue: number | undefined;
      source: string;
    }> = [];
    const globalKeys: string[] = [];

    const unsubscribeLocal = writer.onChange("price", (value, change) => {
      expect(change.operation).toBe("set");
      localValues.push(value);
    });
    const unsubscribeKey = reader.onChange("price", (value, change) => {
      keyChanges.push({
        value,
        previousValue: change.previousValue,
        source: change.source,
      });
    });
    const unsubscribeGlobal = reader.onChange((change) => {
      globalKeys.push(change.key);
    });

    writer.set("price", 1);
    writer.set("price", 2);
    writer.set("price", 2);
    expect(localValues).toEqual([1, 2]);

    await writer.flush();
    await waitFor(() => keyChanges.length === 1);
    expect(keyChanges[0]).toEqual({
      value: 2,
      previousValue: undefined,
      source: "remote",
    });
    expect(globalKeys).toEqual(["price"]);

    await client.hset("test:change-listeners", "price", JSON.stringify(3));
    await reader.synchronize();
    expect(keyChanges[1]).toEqual({
      value: 3,
      previousValue: 2,
      source: "synchronize",
    });

    unsubscribeLocal();
    unsubscribeKey();
    unsubscribeKey();
    unsubscribeGlobal();

    writer.set("price", 4);
    await writer.flush();
    await waitFor(() => reader.get("price") === 4);
    expect(localValues).toEqual([1, 2]);
    expect(keyChanges).toHaveLength(2);
    expect(globalKeys).toEqual(["price", "price"]);
  });

  it("isolates listener failures through onError", async () => {
    const errors: unknown[] = [];
    const map = await makeMap<number>("test:listener-errors", {
      onError: (error) => errors.push(error),
    });
    map.onChange(() => {
      throw new Error("listener failed");
    });

    expect(() => map.set("safe", 1)).not.toThrow();
    expect(errors).toHaveLength(1);
    expect(errors[0]).toEqual(new Error("listener failed"));
  });

  it("provides MobX-tracked reads and batches each remote patch", async () => {
    const writer = await makeMap<number>("test:mobx", {
      flushIntervalMs: 10_000,
    });
    const reader = await createMobxDistributedMap<number>("test:mobx", {
      client,
      blockTimeoutMs: 50,
    });
    maps.push(reader as DistributedMap<unknown>);
    const snapshots: Array<[number | undefined, number | undefined]> = [];
    const stop = autorun(() => {
      snapshots.push([reader.get("a"), reader.get("b")]);
    });

    expect(snapshots).toEqual([[undefined, undefined]]);
    writer.set("a", 1);
    writer.set("b", 2);
    await writer.flush();
    await waitFor(() => snapshots.length === 2);
    expect(snapshots).toEqual([
      [undefined, undefined],
      [1, 2],
    ]);

    reader.set("a", 3);
    expect(snapshots.at(-1)).toEqual([3, 2]);
    stop();

    expect(reader.name).toBe("test:mobx");
    expect(reader.size).toBe(2);
    expect(reader.has("a")).toBe(true);
    expect([...reader.entries()]).toEqual([
      ["a", 3],
      ["b", 2],
    ]);
    expect([...reader.keys()]).toEqual(["a", "b"]);
    expect([...reader.values()]).toEqual([3, 2]);
    expect([...reader]).toEqual([
      ["a", 3],
      ["b", 2],
    ]);
    const visited: string[] = [];
    reader.forEach((value, key) => visited.push(`${key}:${value}`));
    expect(visited).toEqual(["a:3", "b:2"]);

    const changes: string[] = [];
    const unsubscribeGlobal = reader.onChange((change) => {
      changes.push(`all:${change.key}:${change.operation}`);
    });
    const unsubscribeKey = reader.onChange("c", (_value, change) => {
      changes.push(`key:${change.key}:${change.operation}`);
    });
    reader.set("c", 4);
    expect(reader.delete("c")).toBe(true);
    expect(changes).toEqual([
      "all:c:set",
      "key:c:set",
      "all:c:delete",
      "key:c:delete",
    ]);
    unsubscribeGlobal();
    unsubscribeKey();
    expect(() =>
      Reflect.apply(reader.onChange, reader, ["missing-listener"]),
    ).toThrow("A key change listener is required");

    reader.clear();
    expect(reader.size).toBe(0);
    await reader.flush();
    await client.hset("test:mobx", "recovered", JSON.stringify(5));
    await reader.synchronize();
    expect(reader.get("recovered")).toBe(5);

    const quotes = await createMobxDistributedMap<{ bid: number }>(
      "test:mobx-shallow",
      { client, blockTimeoutMs: 50 },
    );
    maps.push(quotes as DistributedMap<unknown>);
    quotes.set("XAUUSD.m", { bid: 3_400 });
    expect(isObservable(quotes.get("XAUUSD.m"))).toBe(false);
    await quotes.destroy();
  });

  it("keeps pending local state over older remote updates", async () => {
    const map = await makeMap<number>("test:local-overlay", {
      flushIntervalMs: 10_000,
    });

    map.set("price", 42);
    await client
      .multi()
      .hset("test:local-overlay", "price", JSON.stringify(10))
      .xadd(
        "test:local-overlay:stream",
        "*",
        "operation",
        "set",
        "key",
        "price",
        "value",
        JSON.stringify(10),
      )
      .exec();
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(map.get("price")).toBe(42);
    await map.flush();
    expect(await client.hget("test:local-overlay", "price")).toBe("42");
  });

  it("trims stream entries older than historyMs", async () => {
    await client.xadd(
      "test:retention:stream",
      "1-0",
      "operation",
      "clear",
    );
    const map = await makeMap<number>("test:retention", {
      flushIntervalMs: 10_000,
      historyMs: 10_000,
    });

    map.set("latest", 1);
    await map.flush();

    const entries = await client.xrange("test:retention:stream", "-", "+");
    expect(entries).toHaveLength(1);
    expect(entries[0]?.[0]).not.toBe("1-0");
  });

  it("periodically repairs state from the authoritative Hash", async () => {
    const map = await makeMap<number>("test:periodic-snapshot", {
      synchronizeIntervalMs: 25,
    });

    await client.hset(
      "test:periodic-snapshot",
      "recovered",
      JSON.stringify(99),
    );
    await waitFor(() => map.get("recovered") === 99);
  });

  it("does not make unflushed mutations durable during destroy", async () => {
    const map = await makeMap<number>("test:destroy-with-pending", {
      flushIntervalMs: 10_000,
    });

    map.set("local-only", 1);
    await map.destroy();

    expect(await client.hget("test:destroy-with-pending", "local-only")).toBeNull();
    expect(await client.xlen("test:destroy-with-pending:stream")).toBe(0);
  });

  it("loads an existing snapshot and can reload authoritative state", async () => {
    await client.hset("test:snapshot", "ready", JSON.stringify(true));
    const streamId = await client.xadd(
      "test:snapshot:stream",
      "*",
      "operation",
      "set",
      "key",
      "ready",
      "value",
      JSON.stringify(true),
    );
    expect(streamId).toBeTypeOf("string");

    const map = await makeMap<boolean>("test:snapshot");
    expect(map.get("ready")).toBe(true);

    await client
      .multi()
      .hset("test:snapshot", "ready", JSON.stringify(false))
      .xadd(
        "test:snapshot:stream",
        "*",
        "operation",
        "set",
        "key",
        "ready",
        "value",
        JSON.stringify(false),
      )
      .exec();
    await map.synchronize();
    expect(map.get("ready")).toBe(false);
  });

  it("supports custom serialization", async () => {
    const map = await createDistributedMap<Date>("test:dates", {
      client,
      blockTimeoutMs: 50,
      serialize: (date) => date.toISOString(),
      deserialize: (value) => new Date(value),
    });
    maps.push(map as DistributedMap<unknown>);

    const date = new Date("2026-01-02T03:04:05.000Z");
    await map.set("launch", date);
    expect(map.get("launch")).toEqual(date);
  });

  it("recovers from malformed stream events even when the error hook throws", async () => {
    let errorCount = 0;
    const map = await createDistributedMap<number>("test:recovery", {
      client,
      blockTimeoutMs: 50,
      onError: () => {
        errorCount += 1;
        throw new Error("observer failure");
      },
    });
    maps.push(map as DistributedMap<unknown>);

    await client.xadd(
      "test:recovery:stream",
      "*",
      "operation",
      "set",
      "key",
      "missing-value",
    );
    await waitFor(() => errorCount === 1);

    await client
      .multi()
      .hset("test:recovery", "healthy", JSON.stringify(42))
      .xadd(
        "test:recovery:stream",
        "*",
        "operation",
        "set",
        "key",
        "healthy",
        "value",
        JSON.stringify(42),
      )
      .exec();
    await waitFor(() => map.get("healthy") === 42);
  });

  it("validates options and rejects writes after destroy", async () => {
    await expect(
      createDistributedMap("test:no-client", undefined as never),
    ).rejects.toThrow("Redis client is required");
    await expect(
      createDistributedMap("", { client }),
    ).rejects.toThrow("name cannot be empty");
    await expect(
      createDistributedMap("test:timeout", {
        client,
        blockTimeoutMs: 0,
      }),
    ).rejects.toThrow("greater than zero");
    await expect(
      createDistributedMap("test:flush-timeout", {
        client,
        flushIntervalMs: 0,
      }),
    ).rejects.toThrow("flushIntervalMs");
    await expect(
      createDistributedMap("test:history", {
        client,
        historyMs: 0,
      }),
    ).rejects.toThrow("historyMs");
    await expect(
      createDistributedMap("test:synchronize-timeout", {
        client,
        synchronizeIntervalMs: 0,
      }),
    ).rejects.toThrow("synchronizeIntervalMs");

    const map = await makeMap<string>("test:destroy");
    await map.destroy();
    expect(() => map.set("nope", "value")).toThrow("is destroyed");
    await expect(map.flush()).rejects.toThrow("is destroyed");
  });

  it("rejects values JSON cannot serialize", async () => {
    const map = await makeMap<undefined>("test:serialization");
    expect(() => map.set("undefined", undefined)).toThrow(
      "must be JSON serializable",
    );
  });
});
