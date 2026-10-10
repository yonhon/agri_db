// Supabase の REST API から市況データ（market_daily_item_stats ビュー）を読む。
// 読み取りは anon キーのみ（ビューには anon への select 権限がある）。

const PAGE_SIZE = 1000;
const CACHE_TTL_MS = 10 * 60 * 1000;

function asNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function createDataSource({ supabaseUrl, anonKey, fetchImpl = fetch, now = () => Date.now() }) {
  if (!supabaseUrl || !anonKey) {
    throw new Error("SUPABASE_URL / SUPABASE_ANON_KEY が設定されていません。");
  }
  let cache = null;
  let inflight = null;

  async function fetchAll() {
    const rows = [];
    for (let from = 0; ; from += PAGE_SIZE) {
      const url =
        `${supabaseUrl.replace(/\/$/, "")}/rest/v1/market_daily_item_stats` +
        "?select=sale_date,item_name,quantity,avg_price,high_price,low_price" +
        "&order=sale_date.asc,item_name.asc";
      const res = await fetchImpl(url, {
        headers: {
          apikey: anonKey,
          Authorization: `Bearer ${anonKey}`,
          Range: `${from}-${from + PAGE_SIZE - 1}`,
          "Range-Unit": "items",
        },
      });
      if (!res.ok) {
        throw new Error(`市況データの取得に失敗しました（HTTP ${res.status}）。`);
      }
      const page = await res.json();
      page.forEach((r) => {
        rows.push({
          sale_date: r.sale_date,
          item_name: r.item_name,
          quantity: asNumber(r.quantity),
          avg_price: asNumber(r.avg_price),
          high_price: asNumber(r.high_price),
          low_price: asNumber(r.low_price),
        });
      });
      if (page.length < PAGE_SIZE) {
        break;
      }
    }
    return rows;
  }

  return {
    async getRows() {
      if (cache && now() - cache.at < CACHE_TTL_MS) {
        return cache.rows;
      }
      if (!inflight) {
        inflight = fetchAll()
          .then((rows) => {
            cache = { at: now(), rows };
            return rows;
          })
          .finally(() => {
            inflight = null;
          });
      }
      return inflight;
    },
  };
}
