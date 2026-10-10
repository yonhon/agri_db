// MCP の利用を usage_events テーブルに記録する（ダッシュボードの利用状況ログと同じテーブル）。
// 利用者は区別しない: visitor_id は固定値。claude.ai などのコネクタは各社のサーバーから呼ばれるため、
// IP や User-Agent では利用者を区別できない。

export const MCP_VISITOR_ID = "mcp-server";

// 接続元アプリの表示名（User-Agent と、接続時に送られるクライアント名から判定）
export function clientLabel(userAgent, clientName) {
  const text = `${clientName || ""} ${userAgent || ""}`;
  if (/claude|anthropic/i.test(text)) return "Claude";
  if (/openai|chatgpt/i.test(text)) return "ChatGPT";
  if (/cursor/i.test(text)) return "Cursor";
  if (/vscode|visual studio code|copilot/i.test(text)) return "VS Code";
  if (/gemini|google/i.test(text)) return "Gemini";
  return "その他";
}

const clip = (value, max) => (value == null ? null : String(value).replace(/\s+/g, " ").slice(0, max));

export function createUsageLogger({ supabaseUrl, anonKey, fetchImpl = fetch }) {
  if (!supabaseUrl || !anonKey) {
    return null;
  }
  const endpoint = `${supabaseUrl.replace(/\/$/, "")}/rest/v1/usage_events`;
  return async function logEvent(event) {
    try {
      const res = await fetchImpl(endpoint, {
        method: "POST",
        headers: {
          apikey: anonKey,
          Authorization: `Bearer ${anonKey}`,
          "Content-Type": "application/json",
          Prefer: "return=minimal",
        },
        body: JSON.stringify({
          visitor_id: MCP_VISITOR_ID,
          event_type: event.eventType,
          page_path: clip(event.pagePath, 255),
          error_code: event.errorCode || null,
          message_summary: clip(event.message, 200),
          metadata: event.metadata || {},
        }),
      });
      if (!res.ok) {
        console.warn(`usage log failed: HTTP ${res.status}`);
      }
    } catch (error) {
      // 記録の失敗で応答を止めない
      console.warn(`usage log failed: ${error && error.message}`);
    }
  };
}

export { clip };
