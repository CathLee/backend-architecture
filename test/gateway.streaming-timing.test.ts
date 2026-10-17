import { describe, it, expect, afterEach } from "vitest";
import { startGateway } from "../src/gateway.ts";
import type { Server } from "node:http";

import { listen } from "./listen.ts";

// 这组测试要证明"流式"不是嘴上说说：token 之间真的有时间间隔，
// 不是服务端在内存里拼好完整字符串后一次性吐给客户端。
describe("gateway token streaming timing", () => {
  let server: Server | undefined;

  afterEach(() => {
    server?.close();
  });

  it("flushes mock tokens incrementally, spaced by the configured delay, instead of dumping them all at once", async () => {
    const tokenDelayMs = 30;
    server = startGateway({ tokenDelayMs });
    const port = await listen(server);

    const res = await fetch(`http://localhost:${port}/chat`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ prompt: "hi" }),
    });

    // 不用 res.text()（它会等整个响应结束才返回），而是用底层的流式 reader
    // 逐块读取，并记录每一块实际到达的时间点，这样才能测出"块与块之间有没有间隔"。
    const reader = res.body!.getReader();
    const arrivalTimestamps: number[] = [];

    while (true) {
      const { done } = await reader.read();
      if (done) break;
      arrivalTimestamps.push(performance.now());
    }

    expect(arrivalTimestamps.length).toBeGreaterThanOrEqual(2);

    const elapsed =
      arrivalTimestamps[arrivalTimestamps.length - 1] - arrivalTimestamps[0];

    // 配置的是 4 个 token 之间各等 tokenDelayMs，理论总耗时接近 3 * tokenDelayMs；
    // 这里只断言"至少过了 2 个 delay"，留出余量，避免测试环境的抖动导致误判为失败。
    expect(elapsed).toBeGreaterThanOrEqual(tokenDelayMs * 2);
  });
});
