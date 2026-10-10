import {
  buildSeriesByItem,
  computeDeviations,
  computePhase,
  computeWeekly,
  latestDate,
  nameKeys,
  normalizeName,
  periodRange,
  priceQuadrant,
  quantityQuadrant,
  shiftYmd,
  weeklyLabel,
} from "./analytics.js";

const round = (v, digits = 1) => (Number.isFinite(v) ? Number(v.toFixed(digits)) : null);
const yen = (v) => (Number.isFinite(v) ? Math.round(v) : null);

class ToolInputError extends Error {}

function intArg(value, { def, min, max, name }) {
  if (value == null) {
    return def;
  }
  const n = Number(value);
  if (!Number.isInteger(n) || n < min || n > max) {
    throw new ToolInputError(`${name} は ${min}〜${max} の整数で指定してください。`);
  }
  return n;
}

function buildContext(rows) {
  if (!rows.length) {
    throw new ToolInputError("市況データがありません。");
  }
  const seriesByItem = buildSeriesByItem(rows);
  return {
    rows,
    seriesByItem,
    latest: latestDate(rows),
    earliest: rows.reduce((a, r) => (a && a < r.sale_date ? a : r.sale_date), null),
    names: [...seriesByItem.keys()],
    // 呼び出しで照合された品目名（利用状況ログの「よく聞かれた品目」に使う）
    resolved: new Set(),
  };
}

// 品目名を照合する。完全一致 → 表記ゆれ（ひらがな・カタカナ等）の一致 → 部分一致の順。
function resolveItem(ctx, query) {
  if (typeof query !== "string" || !query.trim()) {
    throw new ToolInputError("品目名を指定してください。");
  }
  const found = (name) => {
    ctx.resolved.add(name);
    return name;
  };
  if (ctx.seriesByItem.has(query)) {
    return found(query);
  }
  const q = normalizeName(query);
  const exact = ctx.names.filter((n) => nameKeys(n).includes(q));
  if (exact.length === 1) {
    return found(exact[0]);
  }
  const partial = ctx.names.filter((n) => nameKeys(n).some((k) => k.includes(q)));
  if (partial.length === 1) {
    return found(partial[0]);
  }
  const hint = (partial.length ? partial : exact).slice(0, 10);
  throw new ToolInputError(
    hint.length
      ? `品目「${query}」が一意に決まりません。候補: ${hint.join("、")}`
      : `品目「${query}」が見つかりません。list_items で品目名を確認してください。`
  );
}

const ITEM_PROP = { type: "string", description: "品目名（例: レタス、島バナナ）。ひらがな・部分一致でも可" };

export const TOOLS = [
  {
    name: "list_items",
    description:
      "沖縄協同青果の市況に出ている品目の一覧。直近 days 日に取引のあった品目を、最新の販売価格中値・入荷量・取引日数とともに返す。品目名の確認や検索に使う。",
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string", description: "品目名の絞り込み（部分一致、ひらがな可）。省略で全品目" },
        days: { type: "integer", minimum: 1, maximum: 365, default: 30, description: "対象期間（日数）" },
      },
    },
    annotations: { readOnlyHint: true, openWorldHint: false },
    run(ctx, args) {
      const days = intArg(args.days, { def: 30, min: 1, max: 365, name: "days" });
      const since = shiftYmd(ctx.latest, -(days - 1));
      const q = args.query ? normalizeName(args.query) : "";
      const items = [];
      ctx.seriesByItem.forEach((series, name) => {
        if (q && !nameKeys(name).some((k) => k.includes(q))) {
          return;
        }
        const recent = series.filter((r) => r.sale_date >= since);
        if (!recent.length) {
          return;
        }
        const last = recent[recent.length - 1];
        items.push({
          item: name,
          last_date: last.sale_date,
          last_price_yen: yen(last.avg_price),
          last_quantity: last.quantity,
          trading_days: recent.length,
        });
      });
      items.sort((a, b) => (b.last_quantity || 0) - (a.last_quantity || 0));
      return { latest_data_date: ctx.latest, period: { from: since, to: ctx.latest }, count: items.length, items };
    },
  },
  {
    name: "get_item_history",
    description:
      "1品目の日ごとの市況（入荷量、販売価格中値・高値・安値）。価格推移や入荷量の確認に使う。価格は円、入荷量は市況PDFの数量。",
    inputSchema: {
      type: "object",
      properties: {
        item: ITEM_PROP,
        days: { type: "integer", minimum: 1, maximum: 400, default: 30, description: "最新日から遡る日数" },
      },
      required: ["item"],
    },
    annotations: { readOnlyHint: true, openWorldHint: false },
    run(ctx, args) {
      const item = resolveItem(ctx, args.item);
      const days = intArg(args.days, { def: 30, min: 1, max: 400, name: "days" });
      const since = shiftYmd(ctx.latest, -(days - 1));
      const rows = ctx.seriesByItem
        .get(item)
        .filter((r) => r.sale_date >= since)
        .map((r) => ({
          date: r.sale_date,
          quantity: r.quantity,
          price_median_yen: yen(r.avg_price),
          price_high_yen: yen(r.high_price),
          price_low_yen: yen(r.low_price),
        }));
      return { item, latest_data_date: ctx.latest, period: { from: since, to: ctx.latest }, trading_days: rows.length, rows };
    },
  },
  {
    name: "get_price_ranking",
    description:
      "最新日の販売価格中値が、その品目の過去14日（最新日を含まない）の中央値からどれだけ離れているかのランキング（ダッシュボードのKPIと同じ）。割高・割安の品目を探すのに使う。過去14日に7日以上取引があり、最新日に入荷のある品目が対象。",
    inputSchema: {
      type: "object",
      properties: {
        top: { type: "integer", minimum: 1, maximum: 30, default: 5, description: "割高・割安それぞれの件数" },
        item: { type: "string", description: "指定すると、その品目の値も返す（ランキング外でも）" },
      },
    },
    annotations: { readOnlyHint: true, openWorldHint: false },
    run(ctx, args) {
      const top = intArg(args.top, { def: 5, min: 1, max: 30, name: "top" });
      const dev = computeDeviations(ctx.seriesByItem, ctx.latest);
      const fmt = (d) => ({
        item: d.item,
        price_yen: yen(d.price),
        median_14d_yen: yen(d.baseline),
        deviation_pct: round(d.deviationPct),
        day_change_pct: round(d.dayChangePct),
        quantity: d.quantity,
      });
      const result = {
        latest_data_date: ctx.latest,
        baseline_period: { from: dev.baseStart, to: dev.baseEnd },
        eligible_items: dev.items.length,
        overpriced: dev.items.filter((d) => d.deviationPct > 0).sort((a, b) => b.deviationPct - a.deviationPct).slice(0, top).map(fmt),
        underpriced: dev.items.filter((d) => d.deviationPct < 0).sort((a, b) => a.deviationPct - b.deviationPct).slice(0, top).map(fmt),
      };
      if (args.item) {
        const name = resolveItem(ctx, args.item);
        const found = dev.items.find((d) => d.item === name);
        result.requested_item = found ? fmt(found) : { item: name, note: "最新日に入荷がないか、過去14日の取引日数が7日未満のため対象外" };
      }
      return result;
    },
  },
  {
    name: "get_weekly_change",
    description:
      "直近7日と前の7日を比べた、入荷量の変化率と販売価格中値（入荷量加重平均）の変化率（ダッシュボードの前週比マップと同じ）。品薄で値上がり／出回り増で値下がり、などの判定付き。",
    inputSchema: {
      type: "object",
      properties: {
        top: { type: "integer", minimum: 0, maximum: 100, default: 20, description: "14日間の入荷量上位の件数" },
        items: { type: "array", items: { type: "string" }, maxItems: 20, description: "必ず含める品目" },
      },
    },
    annotations: { readOnlyHint: true, openWorldHint: false },
    run(ctx, args) {
      const top = intArg(args.top, { def: 20, min: 0, max: 100, name: "top" });
      const wanted = (args.items || []).map((q) => resolveItem(ctx, q));
      const weekly = computeWeekly(ctx.seriesByItem, ctx.latest);
      const shown = [...weekly.points.slice(0, top)];
      const missing = [];
      wanted.forEach((name) => {
        if (shown.some((p) => p.item === name)) {
          return;
        }
        const p = weekly.points.find((x) => x.item === name);
        if (p) shown.push(p);
        else missing.push(name);
      });
      return {
        latest_data_date: ctx.latest,
        recent_period: { from: weekly.recentStart, to: weekly.recentEnd },
        prior_period: { from: weekly.priorStart, to: weekly.priorEnd },
        items: shown.map((p) => ({
          item: p.item,
          quantity_change_pct: round(p.quantityChangePct),
          price_change_pct: round(p.priceChangePct),
          judgement: weeklyLabel(p.quantityChangePct, p.priceChangePct),
          quantity_recent: p.quantityRecent,
          quantity_prior: p.quantityPrior,
          price_recent_yen: yen(p.priceRecent),
          price_prior_yen: yen(p.pricePrior),
        })),
        not_comparable: missing.length ? { items: missing, reason: "どちらかの7日間に入荷がない" } : undefined,
      };
    },
  },
  {
    name: "get_phase_state",
    description:
      "販売価格中値と入荷量の相図（水準 × 前週比）の状態（ダッシュボードの相図と同じ計算）。品目が値動きの周期のどの局面にいるか（例: 高値圏から下落、出回りのピークへ）を返す。7日→3日の二段移動平均を使い、最新の点はデータ最新日の約1週間前の状態。",
    inputSchema: {
      type: "object",
      properties: {
        items: { type: "array", items: { type: "string" }, minItems: 1, maxItems: 10, description: "品目名のリスト" },
        period_days: {
          type: "integer",
          enum: [7, 30, 90, 0],
          default: 90,
          description: "水準の基準にする表示期間（0 = 全期間）",
        },
        path_points: { type: "integer", minimum: 0, maximum: 30, default: 8, description: "過去の軌跡として返す点の数（7日おき）" },
      },
      required: ["items"],
    },
    annotations: { readOnlyHint: true, openWorldHint: false },
    run(ctx, args) {
      if (!Array.isArray(args.items) || !args.items.length) {
        throw new ToolInputError("items に品目名を1つ以上指定してください。");
      }
      const periodDays = args.period_days == null ? 90 : Number(args.period_days);
      if (![7, 30, 90, 0].includes(periodDays)) {
        throw new ToolInputError("period_days は 7, 30, 90, 0 のいずれかを指定してください。");
      }
      const pathPoints = intArg(args.path_points, { def: 8, min: 0, max: 30, name: "path_points" });
      const range = periodRange(ctx.latest, ctx.earliest, periodDays);
      const results = args.items.slice(0, 10).map((q) => {
        const item = resolveItem(ctx, q);
        const series = ctx.seriesByItem.get(item);
        const price = computePhase(series, range, "p");
        const qty = computePhase(series, range, "q");
        if (!price || !qty) {
          return { item, note: "表示期間に7日間で3日以上の取引がある日が少なく、相図を計算できません" };
        }
        const last = (ph) => ph.points[ph.points.length - 1];
        const lp = last(price);
        const lq = last(qty);
        const qq = quantityQuadrant(lq.levelRatio, lq.changePct);
        const step = (points) => {
          const out = [];
          for (let i = points.length - 1; i >= 0 && out.length < pathPoints; i -= 7) {
            out.unshift(points[i]);
          }
          return out;
        };
        const byAsOf = new Map(qty.points.map((p) => [p.asOfFrom, p]));
        return {
          item,
          latest_point_as_of: `${lp.asOfFrom}〜${lp.asOfTo}頃`,
          price: {
            level_yen: yen(lp.level),
            period_median_yen: yen(price.median),
            level_ratio: round(lp.levelRatio, 2),
            level_z: round(lp.levelZ, 2),
            change_7d_pct: round(lp.changePct),
            change_z: round(lp.changeZ, 2),
            change_window: `${lp.changeFrom}頃→${lp.changeTo}頃`,
            phase: priceQuadrant(lp.levelRatio, lp.changePct),
          },
          quantity: {
            level_per_trading_day: round(lq.level, 0),
            period_median_per_trading_day: round(qty.median, 0),
            level_ratio: round(lq.levelRatio, 2),
            level_z: round(lq.levelZ, 2),
            change_7d_pct: round(lq.changePct),
            change_z: round(lq.changeZ, 2),
            phase: qq.phase,
            price_pressure: qq.pricePressure,
          },
          path: step(price.points).map((p) => {
            const qp = byAsOf.get(p.asOfFrom);
            return {
              as_of: `${p.asOfFrom}〜${p.asOfTo}頃`,
              price_level_ratio: round(p.levelRatio, 2),
              price_change_7d_pct: round(p.changePct),
              quantity_level_ratio: qp ? round(qp.levelRatio, 2) : null,
              quantity_change_7d_pct: qp ? round(qp.changePct) : null,
            };
          }),
        };
      });
      return {
        latest_data_date: ctx.latest,
        period: { from: range.start, to: range.end, days: periodDays || "all" },
        notes: [
          "level_ratio は表示期間の中央値との比（1 = 中央値）。level_z / change_z は中央値・MAD基準のZスコア。",
          "change_7d_pct は前週比（7日前からの変化）。最新の点では get_weekly_change の変化率とほぼ同じ量だが、移動平均と1取引日平均を使うため数値は少しずれる。",
          "相図は通常、右上→右下→左下→左上と時計回りに進む。入荷量が価格に先行する品目では、入荷量の局面が価格の局面に先立って現れる。",
          "移動平均と時点合わせのため、最新の点は約1週間前の状態。足元の値は get_price_ranking や get_item_history で確認する。",
        ],
        items: results,
      };
    },
  },
];

// ツールを実行し、MCP の結果と、利用状況ログ用の情報（照合された品目名・エラーの種類）を返す
export async function runTool(name, args, getRows) {
  const tool = TOOLS.find((t) => t.name === name);
  if (!tool) {
    return null;
  }
  let ctx = null;
  try {
    ctx = buildContext(await getRows());
    const result = tool.run(ctx, args || {});
    return {
      result: { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] },
      items: [...ctx.resolved],
      errorKind: null,
    };
  } catch (error) {
    const isInput = error instanceof ToolInputError;
    const message = isInput ? error.message : `内部エラー: ${error.message || String(error)}`;
    return {
      result: { content: [{ type: "text", text: message }], isError: true },
      items: ctx ? [...ctx.resolved] : [],
      errorKind: isInput ? "mcp_input_error" : "mcp_internal_error",
      message,
    };
  }
}

export async function callTool(name, args, getRows) {
  const run = await runTool(name, args, getRows);
  return run ? run.result : null;
}
