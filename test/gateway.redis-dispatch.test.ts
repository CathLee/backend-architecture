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

// 这组测试验证"请求确实经过了 Redis"，但不直接在测试里连 Redis 断言内部状态
// （ticket 明确要求不要那样做）。换成通过应用自己暴露的可观测事件（onEvent 钩子，
// 相当于日志/指标）来验证"派发→消费"这条路径真的走了一遍，而不是内存里直接跑完。
describe("gateway dispatches chat requests through a Redis Stream", () => {
  let server: Server | undefined;

  afterEach(() => {
    server?.close();
  });

  it("keeps the public SSE behaviour unchanged while routing the job through Redis, observable via onEvent", async () => {
    const events: Array<{ type: string; jobId: string }> = [];

    // streamKey 不传时内部会生成一个随机 key，保证并行跑的测试文件之间不会
    // 共用同一个 Redis Stream 互相偷对方的任务。
    server = startGateway({ onEvent: (e) => events.push(e) });
    const port = await listen(server);

    const res = await fetch(`http://localhost:${port}/chat`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ prompt: "hi" }),
    });

    // 外部可见行为必须和 ticket #2 时一样：客户端照样拿到完整的流式序列。
    const parsedEvents = parseSseEvents(await res.text());
    expect(parsedEvents).toEqual(["Hello", ", ", "world", "!", "[DONE]"]);

    // 同时证明这趟请求确实走了"派发到 Redis Stream -> worker 消费"这条路径，
    // 而不是内存里直接跑完：应该先有一个 dispatched 事件，再有一个 completed 事件，
    // 且两者的 jobId 一致。
    expect(events.map((e) => e.type)).toEqual(["dispatched", "completed"]);
    expect(events[0].jobId).toBe(events[1].jobId);
    expect(events[0].jobId).toBeTruthy();
  });
});
