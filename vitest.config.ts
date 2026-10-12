import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // 每个测试文件的 startGateway() 都会真实连接 Docker 里跑的那个 Redis
    // （不是 mock），并发开好几个 worker 连接。多个测试文件并行跑的话，
    // 短时间内打开的真实连接数会叠加，在本机资源紧张时偶尔导致某个
    // XREADGROUP/XADD 延迟变大，让对时间敏感的流式测试偶发超时。
    // 这里把测试文件串行执行，用真实时间换稳定性，不去追求虚拟/并行加速。
    fileParallelism: false,
  },
});
