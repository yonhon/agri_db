// MCP（Model Context Protocol）サーバー。Streamable HTTP トランスポートのステートレス実装。
// クライアントが POST した JSON-RPC メッセージに、JSON で応答する（SSE は使わない）。
import { TOOLS, callTool } from "./tools.js";

const SUPPORTED_VERSIONS = ["2025-06-18", "2025-03-26", "2024-11-05"];
const SERVER_INFO = { name: "agri-db-okinawa-market", title: "沖縄市況データ", version: "0.1.0" };

const INSTRUCTIONS = [
  "沖縄協同青果の市況PDFを毎日取り込んだデータ（品目ごとの日次の入荷量と販売価格）を返すサーバーです。",
  "価格はすべて「販売価格中値」（市況PDFの中値、円）。入荷量は市況PDFの数量。日付は取引日（日本時間）。",
  "品目名が分からないときは list_items で確認してください。ひらがなや部分一致でも照合します。",
  "割高・割安は get_price_ranking、前週との比較は get_weekly_change、値動きの局面は get_phase_state、日々の推移は get_item_history を使います。",
  "相図（get_phase_state）は移動平均のため約1週間前の状態を表します。足元の状況はランキングや日次の推移と合わせて判断してください。",
  "データは相場判断の参考情報であり、将来の価格を保証するものではありません。",
].join("\n");

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, GET, OPTIONS, DELETE",
  "Access-Control-Allow-Headers": "Content-Type, Accept, Authorization, Mcp-Session-Id, Mcp-Protocol-Version",
  "Access-Control-Expose-Headers": "Mcp-Session-Id",
};

function json(body, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { ...CORS, "Content-Type": "application/json" } });
}

function rpcError(id, code, message) {
  return { jsonrpc: "2.0", id: id ?? null, error: { code, message } };
}

const publicTool = ({ run, ...rest }) => rest;

async function handleMessage(msg, getRows) {
  if (!msg || msg.jsonrpc !== "2.0" || typeof msg.method !== "string") {
    return rpcError(msg && msg.id, -32600, "Invalid Request");
  }
  const isNotification = msg.id === undefined || msg.id === null;
  if (isNotification) {
    return null; // notifications/initialized など。応答しない
  }
  const { id, method, params = {} } = msg;
  switch (method) {
    case "initialize": {
      const requested = params.protocolVersion;
      const protocolVersion = SUPPORTED_VERSIONS.includes(requested) ? requested : SUPPORTED_VERSIONS[0];
      return {
        jsonrpc: "2.0",
        id,
        result: { protocolVersion, capabilities: { tools: { listChanged: false } }, serverInfo: SERVER_INFO, instructions: INSTRUCTIONS },
      };
    }
    case "ping":
      return { jsonrpc: "2.0", id, result: {} };
    case "tools/list":
      return { jsonrpc: "2.0", id, result: { tools: TOOLS.map(publicTool) } };
    case "tools/call": {
      const result = await callTool(params.name, params.arguments, getRows);
      if (!result) {
        return rpcError(id, -32602, `Unknown tool: ${params.name}`);
      }
      return { jsonrpc: "2.0", id, result };
    }
    case "resources/list":
      return { jsonrpc: "2.0", id, result: { resources: [] } };
    case "prompts/list":
      return { jsonrpc: "2.0", id, result: { prompts: [] } };
    default:
      return rpcError(id, -32601, `Method not found: ${method}`);
  }
}

export function createMcpHandler({ getRows }) {
  return async function handle(req) {
    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS });
    }
    if (req.method !== "POST") {
      // ステートレスなので、サーバーから送る SSE ストリーム（GET）やセッション削除（DELETE）は提供しない
      return new Response("Method Not Allowed", { status: 405, headers: { ...CORS, Allow: "POST, OPTIONS" } });
    }
    let body;
    try {
      body = await req.json();
    } catch (_) {
      return json(rpcError(null, -32700, "Parse error"), 400);
    }
    const batch = Array.isArray(body);
    const messages = batch ? body : [body];
    const responses = (await Promise.all(messages.map((m) => handleMessage(m, getRows)))).filter(Boolean);
    if (!responses.length) {
      return new Response(null, { status: 202, headers: CORS });
    }
    return json(batch ? responses : responses[0]);
  };
}
