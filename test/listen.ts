import { createServer, type Server } from "node:http";

// fetch 按规范拒绝连接一批"危险端口"（如 IRC 的 6665-6669），报 "bad port"。
// listen(0) 是随机分配端口，偶尔会撞上，导致测试随机失败。
// 不能对被测的网关 close 再重新 listen（close 会让它的 worker 一起停掉），
// 所以先用一个临时 server 探出一个安全端口，再让网关监听这个端口。
const FETCH_BLOCKED_PORTS = new Set([
  1, 7, 9, 11, 13, 15, 17, 19, 20, 21, 22, 23, 25, 37, 42, 43, 53, 69, 77, 79,
  87, 95, 101, 102, 103, 104, 109, 110, 111, 113, 115, 117, 119, 123, 135, 137,
  139, 143, 161, 179, 389, 427, 465, 512, 513, 514, 515, 526, 530, 531, 532,
  540, 548, 554, 556, 563, 587, 601, 636, 989, 990, 993, 995, 1719, 1720, 1723,
  2049, 3659, 4045, 4190, 5060, 5061, 6000, 6566, 6665, 6666, 6667, 6668, 6669,
  6679, 6697, 10080,
]);

function probePort(): Promise<number> {
  return new Promise((resolve) => {
    const probe = createServer();
    probe.listen(0, () => {
      const address = probe.address();
      const port = address && typeof address === "object" ? address.port : 0;
      probe.close(() => resolve(port));
    });
  });
}

export async function listen(server: Server): Promise<number> {
  let port = await probePort();
  while (FETCH_BLOCKED_PORTS.has(port)) port = await probePort();
  return new Promise((resolve) => {
    server.listen(port, () => resolve(port));
  });
}
