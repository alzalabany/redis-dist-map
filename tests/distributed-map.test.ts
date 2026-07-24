import { once } from "node:events";
import { createServer } from "node:net";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createDistributedMap, type DistributedMap } from "../src/index.js";

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

async function makeMap<T>(name: string): Promise<DistributedMap<T>> {
  const map = await createDistributedMap<T>(name, {
    client,
    blockTimeoutMs: 50,
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

    const map = await makeMap<string>("test:destroy");
    await map.destroy();
    await expect(map.set("nope", "value")).rejects.toThrow("is destroyed");
  });

  it("rejects values JSON cannot serialize", async () => {
    const map = await makeMap<undefined>("test:serialization");
    await expect(map.set("undefined", undefined)).rejects.toThrow(
      "must be JSON serializable",
    );
  });
});
