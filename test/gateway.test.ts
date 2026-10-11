import { describe, it, expect, afterEach } from "vitest";
import { startGateway } from "../src/gateway.ts";
import type { Server } from "node:http";

// 监听随机端口（传 0 让系统分配），避免测试之间抢占固定端口导致冲突。
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

// SSE 的原始文本格式是 "data: xxx\n\n"（每条事件后面跟两个换行分隔），
// 这里把原始响应体按这个格式拆开，只留下每条事件 data 后面的内容，方便断言。
function parseSseEvents(raw: string): string[] {
  return raw
    .split("\n\n")
    .filter((chunk) => chunk.startsWith("data: "))
    .map((chunk) => chunk.slice("data: ".length));
}

// 只测公开接口（真实发 HTTP 请求、读真实响应），不摸 startGateway 内部实现，
// 这样即便后面把 mock 推理换成走 Redis 队列，这个测试依然有效。
describe("gateway public SSE endpoint", () => {
  let server: Server | undefined;

  afterEach(() => {
    server?.close();
  });

  it("streams the full mocked response as ordered SSE events over POST /chat", async () => {
    server = startGateway();
    const port = await listen(server);

    const res = await fetch(`http://localhost:${port}/chat`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ prompt: "hi" }),
    });

    expect(res.headers.get("content-type")).toBe("text/event-stream");

    const raw = await res.text();
    const events = parseSseEvents(raw);

    // 预期值是独立写死的字面量（不是从实现里反推出来的），
    // 这样如果实现改错了，测试才有意义地失败，而不是"抄实现抄一遍"。
    expect(events).toEqual(["Hello", ", ", "world", "!", "[DONE]"]);
  });
});
