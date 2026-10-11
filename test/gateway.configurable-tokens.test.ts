import { describe, it, expect, afterEach } from "vitest";
import { startGateway } from "../src/gateway.ts";
import type { Server } from "node:http";

function listen(server: Server): Promise<number> {
  return new Promise((resolve) => {
    server.listen(0, () => {
      const address = server.address();
      if (address && typeof address === "object") {
        resolve(address.port);
      }
    });
  });
}

function parseSseEvents(raw: string): string[] {
  return raw
    .split("\n\n")
    .filter((chunk) => chunk.startsWith("data: "))
    .map((chunk) => chunk.slice("data: ".length));
}

// 验证"分片策略可配置"这条验收标准：mock 的 token 内容不是写死的，
// 后面要复现队列堆积、长对话等场景时可以传不同的 tokens 进来。
describe("gateway configurable mock chunking", () => {
  let server: Server | undefined;

  afterEach(() => {
    server?.close();
  });

  it("emits whatever custom token list is configured, so chunking strategy can vary per scenario", async () => {
    server = startGateway({ tokens: ["foo", "bar", "baz"] });
    const port = await listen(server);

    const res = await fetch(`http://localhost:${port}/chat`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ prompt: "hi" }),
    });

    const events = parseSseEvents(await res.text());
    expect(events).toEqual(["foo", "bar", "baz", "[DONE]"]);
  });
});
