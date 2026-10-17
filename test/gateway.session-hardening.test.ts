import { describe, it, expect, afterEach } from "vitest";
import { randomUUID } from "node:crypto";
import Redis from "ioredis";
import { startGateway } from "../src/gateway.ts";
import type { GatewayEvent, GatewayServer } from "../src/gateway.ts";

import { listen } from "./listen.ts";

function parseSseData(raw: string): string[] {
  return raw
    .split("\n\n")
    .filter((chunk) => chunk.startsWith("data: "))
    .map((chunk) => chunk.slice("data: ".length));
}

async function chat(
  port: number,
  body: { sessionId?: string; prompt: string },
): Promise<string> {
  const res = await fetch(`http://localhost:${port}/chat`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = parseSseData(await res.text());
  expect(data.at(-1)).toBe("[DONE]");
  return data.slice(0, -1).join("");
}

describe("gateway session hardening: context trimming + session affinity", () => {
  let server: GatewayServer | undefined;
  const cleanup = new Redis("redis://127.0.0.1:6379");
  const usedSessions: string[] = [];

  function newSession(): string {
    const id = `test-${randomUUID()}`;
    usedSessions.push(id);
    return id;
  }

  afterEach(async () => {
    server?.close();
    server = undefined;
    for (const id of usedSessions.splice(0)) {
      await cleanup.del(`session:${id}:history`);
    }
  });

  it("trims an over-long history to the most recent maxHistoryTurns turns", async () => {
    server = startGateway({ maxHistoryTurns: 3 }) as GatewayServer;
    const port = await listen(server);
    const sessionId = newSession();

    for (const prompt of ["alpha", "bravo", "charlie", "delta", "echo"]) {
      await chat(port, { sessionId, prompt });
    }
    const reply = await chat(port, { sessionId, prompt: "what do you remember?" });

    // 只保留最近 3 轮：最早的两轮被裁掉，推理看到的上下文是有上限的。
    expect(reply).toContain("3 earlier turn(s)");
    expect(reply).toContain("charlie | delta | echo");
    expect(reply).not.toContain("alpha");
    expect(reply).not.toContain("bravo");
    // 存储本身也有上限，而不是只在读的时候截断、底下的 list 继续无限增长。
    expect(await cleanup.llen(`session:${sessionId}:history`)).toBe(3);
  });

  it("routes every request of the same session to the same worker, while different sessions spread out", async () => {
    let lastClaim: GatewayEvent | undefined;
    server = startGateway({
      workerCount: 3,
      onEvent: (e) => {
        if (e.type === "claimed") lastClaim = e;
      },
    }) as GatewayServer;
    const port = await listen(server);

    const workersBySession = new Map<string, Set<string>>();
    for (let s = 0; s < 8; s += 1) {
      const sessionId = newSession();
      const workers = new Set<string>();
      for (let turn = 0; turn < 3; turn += 1) {
        lastClaim = undefined;
        await chat(port, { sessionId, prompt: `turn ${turn}` });
        workers.add(lastClaim!.consumerName!);
      }
      workersBySession.set(sessionId, workers);
    }

    for (const workers of workersBySession.values()) {
      expect(workers.size).toBe(1);
    }
    const allWorkers = new Set(
      [...workersBySession.values()].flatMap((w) => [...w]),
    );
    expect(allWorkers.size).toBeGreaterThan(1);
  });

  it("serializes concurrent requests of one session on its worker, so each sees the turns before it", async () => {
    server = startGateway({ workerCount: 3, tokenDelayMs: 20 }) as GatewayServer;
    const port = await listen(server);
    const sessionId = newSession();

    const replies = await Promise.all(
      ["one", "two", "three"].map((prompt) => chat(port, { sessionId, prompt })),
    );

    // 三个请求同时到达。如果它们被不同 worker 并行处理，都会读到"0 轮历史"；
    // 亲和性让它们排在同一个 worker 上依次执行，看到的历史分别是 0、1、2 轮。
    const seen = replies
      .map((r) => Number(/Seen (\d+) earlier/.exec(r)![1]))
      .sort();
    expect(seen).toEqual([0, 1, 2]);
  });

  it("moves a session to another live worker when its worker dies, without losing its history", async () => {
    let lastClaim: GatewayEvent | undefined;
    server = startGateway({
      workerCount: 3,
      onEvent: (e) => {
        if (e.type === "claimed") lastClaim = e;
      },
    }) as GatewayServer;
    const port = await listen(server);
    const sessionId = newSession();

    await chat(port, { sessionId, prompt: "remember-me" });
    const original = lastClaim!.consumerName!;
    server.killWorker(Number(original.split("-")[1]));

    lastClaim = undefined;
    const reply = await chat(port, { sessionId, prompt: "still there?" });

    expect(lastClaim!.consumerName).not.toBe(original);
    // 历史在 Redis 里而不在 worker 内存里，换了 worker 照样能读到。
    expect(reply).toContain("remember-me");
  });
});
