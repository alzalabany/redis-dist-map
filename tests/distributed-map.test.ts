import { Redis } from "ioredis";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:net";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import {
  createDistributedMap,
  createDistributedMapWriter,
  createSharedCounter,
  type DistributedMap,
  type DistributedMapChangeSource,
  type DistributedMapOptions,
  type DistributedMapWriter,
} from "../src/index.js";

let server: ChildProcessWithoutNullStreams;
let client: Redis;
let port: number;
const maps: DistributedMap<unknown>[] = [];
const writers: DistributedMapWriter<unknown>[] = [];

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
  await Promise.all([
    ...maps.map((map) => map.destroy()),
    ...writers.map((writer) => writer.destroy()),
  ]);
  if (client) await client.quit();
  if (server && !server.killed) {
    server.kill("SIGTERM");
    await once(server, "exit");
  }
});

describe("createDistributedMap", () => {
  it("publishes through a writer without loading or subscribing", async () => {
    const duplicate = vi.spyOn(client, "duplicate");
    const multi = vi.spyOn(client, "multi");
    const writer = createDistributedMapWriter<number>("test:writer", {
      client,
      flushIntervalMs: 10_000,
    });
    writers.push(writer as DistributedMapWriter<unknown>);

    expect(duplicate).not.toHaveBeenCalled();
    expect(multi).not.toHaveBeenCalled();
    expect("get" in writer).toBe(false);
    expect("size" in writer).toBe(false);
    duplicate.mockRestore();
    multi.mockRestore();

    const reader = await makeMap<number>("test:writer");
    writer.set("price", 1);
    writer.set("price", 2);
    writer.set("volume", 3);
    await writer.flush();

    await waitFor(() => reader.get("price") === 2);
    expect(reader.get("volume")).toBe(3);
    expect(await client.hgetall("test:writer")).toEqual({
      price: "2",
      volume: "3",
    });
    expect(await client.xlen("test:writer:stream")).toBe(1);

    expect(writer.delete("price")).toBeUndefined();
    await writer.flush();
    await waitFor(() => !reader.has("price"));

    writer.clear();
    writer.set("ready", 4);
    await writer.flush();
    await waitFor(() => reader.size === 1 && reader.get("ready") === 4);
  });

  it("does not flush pending writer mutations during destroy", async () => {
    const writer = createDistributedMapWriter<number>(
      "test:writer-destroy",
      {
        client,
        flushIntervalMs: 10_000,
      },
    );
    writers.push(writer as DistributedMapWriter<unknown>);

    writer.set("local-only", 1);
    await writer.destroy();

    expect(
      await client.hget("test:writer-destroy", "local-only"),
    ).toBeNull();
    expect(() => writer.set("nope", 2)).toThrow("is destroyed");
    await expect(writer.flush()).rejects.toThrow("is destroyed");
  });

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
    const revisionIndex = fields.indexOf("revision");
    expect(fields[revisionIndex + 1]).toBe("1");
    expect(await client.get("test:write-behind:revision")).toBe("1");
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

  it("does not skip remote revisions when a readable map also flushes", async () => {
    const name = "test:concurrent-readable-writer";
    const reader = await makeMap<number>(name, {
      flushIntervalMs: 10_000,
    });
    const writer = createDistributedMapWriter<number>(name, {
      client,
      flushIntervalMs: 10_000,
    });
    writers.push(writer as DistributedMapWriter<unknown>);

    writer.set("remote", 1);
    reader.set("local", 2);
    await Promise.all([writer.flush(), reader.flush()]);

    await waitFor(() => reader.get("remote") === 1);
    expect(reader.get("local")).toBe(2);
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

  it("keeps pending local state over older remote updates", async () => {
    const map = await makeMap<number>("test:local-overlay", {
      flushIntervalMs: 10_000,
    });

    map.set("price", 42);
    await client
      .multi()
      .hset("test:local-overlay", "price", JSON.stringify(10))
      .set("test:local-overlay:revision", "1")
      .xadd(
        "test:local-overlay:stream",
        "*",
        "operation",
        "patch",
        "revision",
        "1",
        "patch",
        JSON.stringify({
          clear: false,
          sets: [["price", JSON.stringify(10)]],
          deletes: [],
        }),
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

  it("does not poll the authoritative Hash while the stream is healthy", async () => {
    const map = await makeMap<number>("test:no-periodic-snapshot", {
      historyMs: 20,
    });

    await client.hset(
      "test:no-periodic-snapshot",
      "unannounced",
      JSON.stringify(99),
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(map.get("unannounced")).toBeUndefined();

    await map.synchronize();
    expect(map.get("unannounced")).toBe(99);
  });

  it("repairs from the Hash when a Stream revision is skipped", async () => {
    const name = "test:revision-gap";
    const map = await makeMap<number>(name);
    const changes: DistributedMapChangeSource[] = [];
    map.onChange((change) => changes.push(change.source));

    await client
      .multi()
      .hset(name, "missed", JSON.stringify(1))
      .hset(name, "latest", JSON.stringify(2))
      .set(`${name}:revision`, "2")
      .xadd(
        `${name}:stream`,
        "*",
        "operation",
        "patch",
        "revision",
        "2",
        "patch",
        JSON.stringify({
          clear: false,
          sets: [["latest", JSON.stringify(2)]],
          deletes: [],
        }),
      )
      .exec();

    await waitFor(() => map.get("missed") === 1);
    expect(map.get("latest")).toBe(2);
    expect(changes).toEqual(["synchronize", "synchronize"]);

    const writer = createDistributedMapWriter<number>(name, { client });
    writers.push(writer as DistributedMapWriter<unknown>);
    writer.set("next", 3);
    await writer.flush();
    await waitFor(() => map.get("next") === 3);
    expect(changes.at(-1)).toBe("remote");
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
    const writer = createDistributedMapWriter<boolean>("test:snapshot", {
      client,
    });
    writers.push(writer as DistributedMapWriter<unknown>);
    writer.set("ready", true);
    await writer.flush();

    const map = await makeMap<boolean>("test:snapshot");
    expect(map.get("ready")).toBe(true);

    await client.hset("test:snapshot", "ready", JSON.stringify(false));
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

    const writer = createDistributedMapWriter<number>("test:recovery", {
      client,
    });
    writers.push(writer as DistributedMapWriter<unknown>);
    writer.set("healthy", 42);
    await writer.flush();
    await waitFor(() => map.get("healthy") === 42);
  });

  it("validates options and rejects writes after destroy", async () => {
    expect(() =>
      createDistributedMapWriter("", { client }),
    ).toThrow("name cannot be empty");
    expect(() =>
      createDistributedMapWriter("test:writer-no-client", undefined as never),
    ).toThrow("Redis client is required");
    expect(() =>
      createDistributedMapWriter("test:writer-flush-timeout", {
        client,
        flushIntervalMs: 0,
      }),
    ).toThrow("flushIntervalMs");
    expect(() =>
      createDistributedMapWriter("test:writer-history", {
        client,
        historyMs: 0,
      }),
    ).toThrow("historyMs");

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

describe("createSharedCounter", () => {
  it("atomically increments shared and independent keys", async () => {
    const first = createSharedCounter("test:counter:atomic", {
      client,
      ttlMs: 2_000,
    });
    const second = createSharedCounter("test:counter:atomic", {
      client,
      ttlMs: 2_000,
    });

    const values = await Promise.all(
      Array.from({ length: 100 }, (_, index) =>
        (index % 2 === 0 ? first : second).inc("ada"),
      ),
    );

    expect([...values].sort((left, right) => left - right)).toEqual(
      Array.from({ length: 100 }, (_, index) => index + 1),
    );
    await expect(first.inc("grace")).resolves.toBe(1);
  });

  it("expires from the first increment without extending the window", async () => {
    const counter = createSharedCounter("test:counter:expiry", {
      client,
      ttlMs: 120,
    });

    await expect(counter.inc("user")).resolves.toBe(1);
    await new Promise((resolve) => setTimeout(resolve, 80));
    await expect(counter.inc("user")).resolves.toBe(2);
    await new Promise((resolve) => setTimeout(resolve, 70));
    await expect(counter.inc("user")).resolves.toBe(1);
  });

  it("supports persistent counters when ttlMs is omitted", async () => {
    const counter = createSharedCounter("test:counter:persistent", {
      client,
    });

    await expect(counter.inc("user")).resolves.toBe(1);
    await expect(counter.inc("user")).resolves.toBe(2);
  });

  it("validates its name, client, and ttl", () => {
    expect(() => createSharedCounter("", { client })).toThrow(
      "name cannot be empty",
    );
    expect(() =>
      createSharedCounter("test:counter:no-client", undefined as never),
    ).toThrow("Redis client is required");
    expect(() =>
      createSharedCounter("test:counter:zero-ttl", {
        client,
        ttlMs: 0,
      }),
    ).toThrow("ttlMs");
    expect(() =>
      createSharedCounter("test:counter:fractional-ttl", {
        client,
        ttlMs: 1.5,
      }),
    ).toThrow("ttlMs");
  });
});
