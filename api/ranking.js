// Pódio global do jogo. Usa Upstash Redis (Vercel Marketplace) via REST, sem dependências.
const U = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const K = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const KEY = "detetive:ranking", MAX = 1600, TOP = 10, KEEP = 200;

async function pipe(cmds) {
  const r = await fetch(U + "/pipeline", {
    method: "POST",
    headers: { Authorization: "Bearer " + K, "Content-Type": "application/json" },
    body: JSON.stringify(cmds)
  });
  if (!r.ok) throw new Error("redis " + r.status);
  return r.json();
}
async function top() {
  const [r] = await pipe([["ZREVRANGE", KEY, 0, TOP - 1, "WITHSCORES"]]);
  const a = r.result || [], out = [];
  for (let i = 0; i < a.length; i += 2) out.push({ n: String(a[i]).split("~")[0], s: Number(a[i + 1]) });
  return out;
}

module.exports = async (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  if (!U || !K) return res.status(503).json({ erro: "Banco de dados não configurado" });
  try {
    if (req.method === "GET") return res.status(200).json({ top: await top() });
    if (req.method === "POST") {
      const b = typeof req.body === "string" ? JSON.parse(req.body) : (req.body || {});
      const n = String(b.n || "").normalize("NFC").replace(/[^\p{L}\p{N} ._-]/gu, "").trim().slice(0, 16) || "Anônimo(a)";
      const s = Math.floor(Number(b.s));
      if (!Number.isFinite(s) || s < 0 || s > MAX) return res.status(400).json({ erro: "Pontuação inválida" });
      const ip = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim() || "x";
      const [rl] = await pipe([["SET", "rl:" + ip, 1, "NX", "EX", 20]]);
      if (rl.result === null) return res.status(429).json({ erro: "Aguarde alguns segundos", top: await top() });
      const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 5);
      await pipe([["ZADD", KEY, s, n + "~" + id], ["ZREMRANGEBYRANK", KEY, 0, -(KEEP + 1)]]);
      return res.status(200).json({ top: await top() });
    }
    res.setHeader("Allow", "GET, POST");
    return res.status(405).json({ erro: "Método não permitido" });
  } catch (e) {
    return res.status(500).json({ erro: "Falha ao acessar o pódio" });
  }
};
