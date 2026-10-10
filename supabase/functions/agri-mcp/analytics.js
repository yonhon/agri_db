// ダッシュボード（dashboard/app.js）と同じ計算を、MCP サーバー用に切り出したもの。
// 数値がダッシュボードと一致するよう、計算手順を変えるときは両方を合わせて変更する。
// avg_price は市況PDFの「中値」列（販売価格中値）。

export function parseISODate(value) {
  return new Date(`${value}T00:00:00Z`);
}

export function shiftYmd(ymd, days) {
  const d = parseISODate(ymd);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

export function isPositive(value) {
  return Number.isFinite(value) && value > 0;
}

export function median(values) {
  if (!values.length) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

export function weightedPrice(rows) {
  let qty = 0;
  let sum = 0;
  rows.forEach((r) => {
    if (isPositive(r.avg_price) && isPositive(r.quantity)) {
      qty += r.quantity;
      sum += r.avg_price * r.quantity;
    }
  });
  return qty > 0 ? sum / qty : null;
}

export function sumQuantity(rows) {
  return rows.reduce((acc, r) => acc + (isPositive(r.quantity) ? r.quantity : 0), 0);
}

export function buildSeriesByItem(rows) {
  const map = new Map();
  rows.forEach((row) => {
    if (!map.has(row.item_name)) {
      map.set(row.item_name, []);
    }
    map.get(row.item_name).push(row);
  });
  map.forEach((series) => series.sort((a, b) => a.sale_date.localeCompare(b.sale_date)));
  return map;
}

export function latestDate(rows) {
  let latest = null;
  rows.forEach((r) => {
    if (!latest || r.sale_date > latest) {
      latest = r.sale_date;
    }
  });
  return latest;
}

// ---- KPI: 本日の中値を、過去 baseDays 日（本日を含まない）の中値の中央値と比べる ----
export function computeDeviations(seriesByItem, latest, { baseDays = 14, minBaseDays = 7 } = {}) {
  const baseStart = shiftYmd(latest, -baseDays);
  const baseEnd = shiftYmd(latest, -1);
  const out = [];
  seriesByItem.forEach((series, itemName) => {
    const today = series.find((r) => r.sale_date === latest);
    if (!today || !isPositive(today.avg_price) || !isPositive(today.quantity)) {
      return;
    }
    const baseRows = series.filter((r) => r.sale_date >= baseStart && r.sale_date <= baseEnd && isPositive(r.avg_price));
    if (baseRows.length < minBaseDays) {
      return;
    }
    const baseline = median(baseRows.map((r) => r.avg_price));
    const idx = series.indexOf(today);
    const prev = idx > 0 ? series[idx - 1] : null;
    out.push({
      item: itemName,
      price: today.avg_price,
      quantity: today.quantity,
      baseline,
      baselineDays: baseRows.length,
      deviationPct: ((today.avg_price - baseline) / baseline) * 100,
      dayChangePct: prev && isPositive(prev.avg_price) ? ((today.avg_price - prev.avg_price) / prev.avg_price) * 100 : null,
      previousDate: prev ? prev.sale_date : null,
    });
  });
  return { baseStart, baseEnd, items: out };
}

// ---- 前週比: 直近7日と前の7日の入荷量合計・入荷量加重平均の中値 ----
export function computeWeekly(seriesByItem, latest) {
  const recentStart = shiftYmd(latest, -6);
  const priorStart = shiftYmd(latest, -13);
  const priorEnd = shiftYmd(latest, -7);
  const points = [];
  seriesByItem.forEach((series, itemName) => {
    const recent = series.filter((r) => r.sale_date >= recentStart && r.sale_date <= latest);
    const prior = series.filter((r) => r.sale_date >= priorStart && r.sale_date <= priorEnd);
    const qRecent = sumQuantity(recent);
    const qPrior = sumQuantity(prior);
    const pRecent = weightedPrice(recent);
    const pPrior = weightedPrice(prior);
    if (!(qRecent > 0) || !(qPrior > 0) || pRecent == null || pPrior == null) {
      return;
    }
    points.push({
      item: itemName,
      quantityRecent: qRecent,
      quantityPrior: qPrior,
      priceRecent: pRecent,
      pricePrior: pPrior,
      quantityChangePct: ((qRecent - qPrior) / qPrior) * 100,
      priceChangePct: ((pRecent - pPrior) / pPrior) * 100,
      total: qRecent + qPrior,
    });
  });
  points.sort((a, b) => b.total - a.total);
  return { recentStart, recentEnd: latest, priorStart, priorEnd, points };
}

// ---- 相図: 「7日 → 3日」の二段の移動平均 ----
function rolling7(byDate, d, minDays) {
  const rows = [];
  for (let k = 6; k >= 0; k -= 1) {
    const r = byDate.get(shiftYmd(d, -k));
    if (r && isPositive(r.quantity) && isPositive(r.avg_price)) {
      rows.push(r);
    }
  }
  if (rows.length < minDays) {
    return null;
  }
  const total = sumQuantity(rows);
  return { q: total / rows.length, p: weightedPrice(rows), total };
}

export function rollingSeries(series, fromYmd, toYmd, minDays = 3) {
  const byDate = new Map((series || []).map((r) => [r.sale_date, r]));
  const first = new Map();
  for (let d = shiftYmd(fromYmd, -2); d <= toYmd; d = shiftYmd(d, 1)) {
    first.set(d, rolling7(byDate, d, minDays));
  }
  const out = [];
  for (let d = fromYmd; d <= toYmd; d = shiftYmd(d, 1)) {
    const parts = [0, 1, 2].map((k) => first.get(shiftYmd(d, -k)));
    if (parts.some((x) => x == null)) {
      out.push({ date: d, q: null, p: null });
      continue;
    }
    const total = parts.reduce((a, x) => a + x.total, 0);
    out.push({
      date: d,
      q: parts.reduce((a, x) => a + x.q, 0) / 3,
      p: parts.reduce((a, x) => a + x.p * x.total, 0) / total,
    });
  }
  return out;
}

// 頑健なZスコア（中央値・MAD）。center を渡すとその値を中心にする。ダッシュボードと同じ下限付き。
export function makeZScaler(values, center) {
  const med = center == null ? median(values) : center;
  const mad = median(values.map((v) => Math.abs(v - med)));
  const floor = center === 0 ? 0.01 : Math.abs(med) * 0.02;
  const scale = Math.max(mad * 1.4826, floor) || 1;
  return (v) => (v - med) / scale;
}

export function periodRange(latest, earliest, periodDays) {
  return { start: periodDays > 0 ? shiftYmd(latest, -(periodDays - 1)) : earliest, end: latest };
}

// 相図の点。key は "p"（販売価格中値）または "q"（入荷量・1取引日平均）。
// 縦軸の前週比（7日前からの変化）と同じ時点にそろえるため、水準は t−4 と t−3 の平均を使う。
export function computePhase(series, range, key, minDays = 3) {
  const rolled = rollingSeries(series, shiftYmd(range.start, -7), range.end, minDays);
  const inPeriod = rolled.slice(7);
  const raw = inPeriod.map((r, i) => {
    const prev = rolled[i];
    const midA = rolled[i + 3];
    const midB = rolled[i + 4];
    if (r[key] == null || prev[key] == null || midA[key] == null || midB[key] == null) {
      return null;
    }
    return {
      date: r.date,
      level: (midA[key] + midB[key]) / 2,
      change: r[key] / prev[key] - 1,
    };
  });
  const valid = raw.filter(Boolean);
  if (valid.length < 2) {
    return null;
  }
  const levels = valid.map((r) => r.level);
  const med = median(levels);
  const zLevel = makeZScaler(levels);
  const zChange = makeZScaler(valid.map((r) => r.change), 0);
  const points = valid.map((r) => ({
    ...actualTime(r.date),
    level: r.level,
    levelRatio: r.level / med,
    levelZ: zLevel(r.level),
    changePct: r.change * 100,
    changeZ: zChange(r.change),
  }));
  return { median: med, points };
}

// 二段の移動平均（9日窓）の値は計算上の日付 d より約4日前が中心。水準をさらに3.5日前にそろえるので、
// 点は d の約7.5日前（d−8〜d−7）の状態。変化は約 d−11 から約 d−4 への変化。
export function actualTime(d) {
  return {
    asOfFrom: shiftYmd(d, -8),
    asOfTo: shiftYmd(d, -7),
    changeFrom: shiftYmd(d, -11),
    changeTo: shiftYmd(d, -4),
  };
}

const PRICE_QUADRANTS = {
  upperRight: "高値圏で上昇中",
  lowerRight: "高値圏から下落",
  lowerLeft: "安値圏で下落中",
  upperLeft: "安値圏から回復",
};
const QUANTITY_QUADRANTS = {
  upperRight: "出回りのピークへ",
  lowerRight: "ピークを過ぎた",
  lowerLeft: "品薄が深まる",
  upperLeft: "出回りが回復",
};
// 入荷量の局面が価格に与える圧力（ダッシュボードの背景色と同じ考え方）
const QUANTITY_PRESSURE = {
  upperRight: "値下がり方向（強い）",
  lowerRight: "値下がり方向（弱まりつつある）",
  lowerLeft: "値上がり方向（強い）",
  upperLeft: "値上がり方向（弱まりつつある）",
};

export function quadrantKey(levelRatio, changePct) {
  const right = levelRatio >= 1;
  const up = changePct >= 0;
  return right ? (up ? "upperRight" : "lowerRight") : up ? "upperLeft" : "lowerLeft";
}

export function priceQuadrant(levelRatio, changePct) {
  return PRICE_QUADRANTS[quadrantKey(levelRatio, changePct)];
}

export function quantityQuadrant(levelRatio, changePct) {
  const k = quadrantKey(levelRatio, changePct);
  return { phase: QUANTITY_QUADRANTS[k], pricePressure: QUANTITY_PRESSURE[k] };
}

export function weeklyLabel(quantityChangePct, priceChangePct) {
  const more = quantityChangePct >= 0;
  const up = priceChangePct >= 0;
  if (!more && up) return "品薄・値上がり";
  if (more && up) return "出回り増・値上がり";
  if (!more && !up) return "品薄・値下がり";
  return "出回り増・値下がり";
}

// 品目名の読み（漢字表記のみ）。ダッシュボード（dashboard/app.js）の KANJI_READINGS と同じ内容。
export const KANJI_READINGS = {
  大根: "だいこん", 人参: "にんじん", 白菜: "はくさい", 玉葱: "たまねぎ", 玉: "たま", 葱: "ねぎ",
  胡瓜: "きゅうり", 茄子: "なす", 南瓜: "かぼちゃ", 冬瓜: "とうがん", 牛蒡: "ごぼう", 生姜: "しょうが",
  大蒜: "にんにく", 里芋: "さといも", 田芋: "たいも", 紅芋: "べにいも", 甘藷: "かんしょ",
  馬鈴薯: "ばれいしょ", 薩摩芋: "さつまいも", 山芋: "やまいも", 長芋: "ながいも", 芋: "いも",
  青梗菜: "ちんげんさい", 小松菜: "こまつな", 水菜: "みずな", 春菊: "しゅんぎく", 法蓮草: "ほうれんそう",
  菠薐草: "ほうれんそう", 草: "そう", 分葱: "わけぎ", 韮: "にら", 蓮根: "れんこん", 筍: "たけのこ",
  枝豆: "えだまめ", 豆: "まめ", 隠元: "いんげん", 蕪: "かぶ", 大葉: "おおば", 紫蘇: "しそ",
  三つ葉: "みつば", 芹: "せり", 芥子菜: "からしな", 菜: "な", 長命草: "ちょうめいそう",
  唐辛子: "とうがらし", 落花生: "らっかせい", 西瓜: "すいか", 蜜柑: "みかん", 林檎: "りんご",
  梨: "なし", 苺: "いちご", 桃: "もも", 柿: "かき", 葡萄: "ぶどう", 甘夏: "あまなつ", 檸檬: "れもん",
  新興: "しんこう", 豊水: "ほうすい", 巨峰: "きょほう", 温州: "うんしゅう", 豆苗: "とうみょう",
  山菜: "さんさい", 山東菜: "さんとうさい", 軟皮: "なんぴ", 果実: "かじつ", 柑橘: "かんきつ",
  香辛: "こうしん", 根菜: "こんさい", 土物: "つちもの", 物: "もの", 野菜: "やさい",
  葉茎菜: "ようけいさい", 類: "るい", その他: "そのた",
  島: "しま", 紅: "べに", 赤: "あか", 青: "あお", 白: "しろ", 黄: "き", 黒: "くろ", 紫: "むらさき",
  新: "しん", 小: "こ", 大: "おお", 長: "なが", 丸: "まる", 花: "はな", 実: "み", 葉: "は", 生: "なま",
};
const KANJI_READING_KEYS = Object.keys(KANJI_READINGS).sort((a, b) => b.length - a.length);

export function itemReading(name) {
  let reading = String(name).normalize("NFKC");
  KANJI_READING_KEYS.forEach((kanji) => {
    reading = reading.split(kanji).join(KANJI_READINGS[kanji]);
  });
  return reading;
}

// 品目名の照合用（全角半角・ひらがな／カタカナ・空白の違いを吸収）
export function normalizeName(value) {
  return String(value)
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[ぁ-ゖ]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) + 0x60))
    .replace(/\s+/g, "");
}

// 品目名の照合キー（表記そのものと、漢字を読みに変えたもの）
export function nameKeys(name) {
  return [normalizeName(name), normalizeName(itemReading(name))];
}
