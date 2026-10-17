import { describe, it, expect, afterEach } from "vitest";
import { randomUUID } from "node:crypto";
import Redis from "ioredis";
import { startGateway } from "../src/gateway.ts";
import type { GatewayServer } from "../src/gateway.ts";

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

describe("gateway session state", () => {
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
    for (const id of usedSessions.splice(0)) {
      await cleanup.del(`session:${id}:history`);
    }
  });

  it("makes the first request's history visible to the second request of the same session", async () => {
    server = startGateway() as GatewayServer;
    const port = await listen(server);
    const sessionId = newSession();

    const first = await chat(port, { sessionId, prompt: "my name is Alice" });
    const second = await chat(port, { sessionId, prompt: "what is my name?" });

    // 第一轮没有历史，第二轮的 mock 回复必须体现出"记得第一轮说过什么"。
    expect(first).not.toContain("Alice");
    expect(second).toContain("my name is Alice");
  });

  it("does not leak history across different sessions", async () => {
    server = startGateway() as GatewayServer;
    const port = await listen(server);
    const sessionA = newSession();
    const sessionB = newSession();

    await chat(port, { sessionId: sessionA, prompt: "secret-of-A" });
    const reply = await chat(port, { sessionId: sessionB, prompt: "hello" });

    expect(reply).not.toContain("secret-of-A");
  });

  it("keeps session history outside the queue's stream, as its own Redis key", async () => {
    const streamKey = `chat:jobs:${randomUUID()}`;
    server = startGateway({ streamKey }) as GatewayServer;
    const port = await listen(server);
    const sessionId = newSession();

    await chat(port, { sessionId, prompt: "one" });
    await chat(port, { sessionId, prompt: "two" });

    // 会话历史是自己的 list，不是队列 stream 里的消息，也不借用 stream key。
    expect(await cleanup.type(`session:${sessionId}:history`)).toBe("list");
    expect(await cleanup.llen(`session:${sessionId}:history`)).toBe(2);
    expect(await cleanup.type(streamKey)).toBe("stream");
    await cleanup.del(streamKey);
  });

  it("still serves requests without a sessionId statelessly", async () => {
    server = startGateway() as GatewayServer;
    const port = await listen(server);

    const reply = await chat(port, { prompt: "hi" });
    expect(reply).toBe("Hello, world!");
  });
});
