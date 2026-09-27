(function () {
  const config = window.DASHBOARD_CONFIG || {};
  const statusEl = document.getElementById("statusMessage");
  const recentUpdatesSectionEl = document.getElementById("recentUpdatesSection");
  const recentUpdatesListEl = document.getElementById("recentUpdatesList");
  const kpiCardsEl = document.getElementById("kpiCards");
  const kpiDateLabelEl = document.getElementById("kpiDateLabel");
  const trendChipsEl = document.getElementById("trendChips");
  const trendAddBtn = document.getElementById("trendAddBtn");
  const trendDialogEl = document.getElementById("trendDialog");
  const trendSearchEl = document.getElementById("trendSearch");
  const trendOptionsEl = document.getElementById("trendOptions");
  const trendSelCountEl = document.getElementById("trendSelCount");
  const trendNoMatchEl = document.getElementById("trendNoMatch");
  const focusItemEl = document.getElementById("focusItem");
  const corrFocusItemEl = document.getElementById("corrFocusItem");
  const corrSectionEl = document.getElementById("corrSection");
  const corrMetaLabelEl = document.getElementById("corrMetaLabel");
  const unitPriceMetaLabelEl = document.getElementById("unitPriceMetaLabel");
  const weeklyMapMetaLabelEl = document.getElementById("weeklyMapMetaLabel");
  const weeklyMapNoticeEl = document.getElementById("weeklyMapNotice");
  const corrTopPairsBodyEl = document.getElementById("corrTopPairsBody");
  const corrBottomPairsBodyEl = document.getElementById("corrBottomPairsBody");
  const corrFocusRankingBodyEl = document.getElementById("corrFocusRankingBody");
  const reloadBtn = document.getElementById("reloadBtn");
  const periodButtons = Array.from(document.querySelectorAll(".period-btn"));

  const state = {
    rows: [],
    periodRows: [],
    seriesByItem: new Map(),
    // 0 = 全期間
    periodDays: Number(config.defaultDays) || 30,
    trendItems: [],
    focusItem: "",
    corrFocusItem: "",
    analyticsClient: null,
  };
  const UPDATES_JSON_PATH = "./updates.json";
  // 品目チップの色をグラフの線と一致させるため、ECharts既定パレットを明示する
  const TREND_COLORS = ["#5470c6", "#91cc75", "#fac858", "#ee6666", "#73c0de", "#3ba272", "#fc8452", "#9a60b4", "#ea7ccc"];

  const trendChart = echarts.init(document.getElementById("trendChart"));
  const comboChart = echarts.init(document.getElementById("comboChart"));
  const weeklyMapChart = echarts.init(document.getElementById("weeklyMapChart"));
  const unitPriceChart = echarts.init(document.getElementById("unitPriceChart"));
  const VISITOR_ID_KEY = "agri_dashboard_visitor_id";
  const SELECTOR_PREFS_KEY = "agri_dashboard_selector_prefs_v1";

  function setStatus(message) {
    statusEl.textContent = message;
  }

  function clearChildren(el) {
    while (el.firstChild) {
      el.removeChild(el.firstChild);
    }
  }

  function appendOption(selectEl, value, label) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    selectEl.appendChild(option);
  }

  function parseLocalYmd(value) {
    if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
      return null;
    }
    const d = new Date(`${value}T00:00:00`);
    if (Number.isNaN(d.getTime())) {
      return null;
    }
    d.setHours(0, 0, 0, 0);
    return d;
  }

  function filterRecentUpdates(items, maxDays) {
    if (!Array.isArray(items) || !Number.isFinite(maxDays) || maxDays < 0) {
      return [];
    }
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const msPerDay = 24 * 60 * 60 * 1000;
    return items.filter((item) => {
      if (!item || typeof item !== "object") {
        return false;
      }
      const posted = parseLocalYmd(item.posted_at);
      if (!posted || typeof item.message !== "string" || !item.message.trim()) {
        return false;
      }
      const diffDays = Math.floor((today.getTime() - posted.getTime()) / msPerDay);
      return diffDays >= 0 && diffDays <= maxDays;
    });
  }

  async function loadUpdatesFromJson() {
    try {
      const response = await fetch(UPDATES_JSON_PATH, { cache: "no-store" });
      if (!response.ok) {
        return [];
      }
      const data = await response.json();
      return Array.isArray(data) ? data : [];
    } catch (_) {
      return [];
    }
  }

  async function renderRecentUpdates() {
    if (!recentUpdatesSectionEl || !recentUpdatesListEl) {
      return;
    }
    const loaded = await loadUpdatesFromJson();
    const recentUpdates = filterRecentUpdates(loaded, 7);
    if (!recentUpdates.length) {
      clearChildren(recentUpdatesListEl);
      recentUpdatesSectionEl.hidden = true;
      return;
    }
    clearChildren(recentUpdatesListEl);
    recentUpdates.forEach((item) => {
      const li = document.createElement("li");
      li.textContent = item.message;
      recentUpdatesListEl.appendChild(li);
    });
    recentUpdatesSectionEl.hidden = false;
  }

  function appendTableCell(rowEl, text, className, title) {
    const td = document.createElement("td");
    td.textContent = text;
    if (className) {
      td.className = className;
    }
    if (title) {
      td.title = title;
    }
    rowEl.appendChild(td);
  }

  function renderEmptyTableRow(targetEl, colspan, message) {
    clearChildren(targetEl);
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = colspan;
    td.textContent = message;
    tr.appendChild(td);
    targetEl.appendChild(tr);
  }

  function asNumber(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }

  function parseISODate(value) {
    return new Date(`${value}T00:00:00Z`);
  }

  function formatYmd(value) {
    const d = parseISODate(value);
    return d.toLocaleDateString("ja-JP", { month: "2-digit", day: "2-digit" });
  }

  function fmtPrice(value) {
    if (value == null) {
      return "-";
    }
    return `${Math.round(value).toLocaleString("ja-JP")}円`;
  }

  function fmtRate(rate) {
    const sign = rate > 0 ? "+" : "";
    return `${sign}${rate.toFixed(1)}%`;
  }

  function fmtCorr(value) {
    if (!Number.isFinite(value)) {
      return "-";
    }
    return value.toFixed(3);
  }

  function createVisitorId() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return window.crypto.randomUUID();
    }
    return `v_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
  }

  function getVisitorId() {
    try {
      const current = localStorage.getItem(VISITOR_ID_KEY);
      if (current && current.length >= 8 && current.length <= 64) {
        return current;
      }
      const next = createVisitorId();
      localStorage.setItem(VISITOR_ID_KEY, next);
      return next;
    } catch (_) {
      return createVisitorId();
    }
  }

  function sanitizeMessage(message) {
    if (!message) {
      return "";
    }
    return String(message).replace(/\s+/g, " ").slice(0, 200);
  }

  function loadSelectorPrefs() {
    try {
      const raw = localStorage.getItem(SELECTOR_PREFS_KEY);
      if (!raw) {
        return;
      }
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") {
        return;
      }
      if (Array.isArray(parsed.trendItems)) {
        state.trendItems = parsed.trendItems.filter((v) => typeof v === "string");
      }
      if (typeof parsed.focusItem === "string") {
        state.focusItem = parsed.focusItem;
      }
      if (typeof parsed.corrFocusItem === "string") {
        state.corrFocusItem = parsed.corrFocusItem;
      }
      // 期間ボタンにある値（0 = 全期間）のときだけ復元する
      if (periodButtons.some((b) => parsePeriodDays(b.dataset.days) === parsed.periodDays)) {
        state.periodDays = parsed.periodDays;
      }
    } catch (_) {
      // No-op: local preference loading should not block UI rendering.
    }
  }

  function saveSelectorPrefs() {
    try {
      localStorage.setItem(
        SELECTOR_PREFS_KEY,
        JSON.stringify({
          trendItems: state.trendItems,
          focusItem: state.focusItem,
          corrFocusItem: state.corrFocusItem,
          periodDays: state.periodDays,
        })
      );
    } catch (_) {
      // No-op: local preference saving should not block UI rendering.
    }
  }

  async function logUsageEvent(client, payload) {
    if (!client) {
      return;
    }
    try {
      await client.from("usage_events").insert(payload);
    } catch (_) {
      // No-op: logging failure should not block UI rendering.
    }
  }

  function setupErrorLogging(client) {
    window.addEventListener("error", (event) => {
      void logUsageEvent(client, {
        visitor_id: getVisitorId(),
        event_type: "error",
        page_path: window.location.pathname || "/",
        error_code: "window_error",
        message_summary: sanitizeMessage(event.message),
        metadata: {
          filename: event.filename || "",
          line: event.lineno || null,
          col: event.colno || null,
        },
      });
    });

    window.addEventListener("unhandledrejection", (event) => {
      const reason = event.reason && event.reason.message ? event.reason.message : String(event.reason || "");
      void logUsageEvent(client, {
        visitor_id: getVisitorId(),
        event_type: "error",
        page_path: window.location.pathname || "/",
        error_code: "unhandled_rejection",
        message_summary: sanitizeMessage(reason),
        metadata: {},
      });
    });
  }

  function scoreBand(score) {
    if (score <= -0.8) {
      return { band: 1, label: "-1.00~-0.80" };
    }
    if (score <= -0.6) {
      return { band: 2, label: "-0.80~-0.60" };
    }
    if (score <= -0.4) {
      return { band: 3, label: "-0.60~-0.40" };
    }
    if (score <= -0.2) {
      return { band: 4, label: "-0.40~-0.20" };
    }
    if (score <= 0.19) {
      return { band: 5, label: "-0.19~0.19" };
    }
    if (score <= 0.4) {
      return { band: 6, label: "0.20~0.40" };
    }
    if (score <= 0.6) {
      return { band: 7, label: "0.40~0.60" };
    }
    if (score <= 0.8) {
      return { band: 8, label: "0.60~0.80" };
    }
    return { band: 9, label: "0.80~1.00" };
  }

  function getLatestDate(rows) {
    return rows.length ? rows[rows.length - 1].sale_date : null;
  }

  function parsePeriodDays(value) {
    if (value === "all") {
      return 0;
    }
    const days = Number(value);
    return Number.isFinite(days) && days > 0 ? days : null;
  }

  function periodLabel(periodDays) {
    return periodDays > 0 ? `直近 ${periodDays} 日` : "全期間";
  }

  function filterRowsByPeriod(rows, periodDays) {
    if (!rows.length) {
      return [];
    }
    if (!(periodDays > 0)) {
      return rows;
    }
    const latest = parseISODate(getLatestDate(rows));
    const cutoff = new Date(latest);
    cutoff.setUTCDate(cutoff.getUTCDate() - Math.max(0, periodDays - 1));
    return rows.filter((row) => parseISODate(row.sale_date) >= cutoff);
  }

  function buildSeriesByItem(rows) {
    const map = new Map();
    rows.forEach((row) => {
      if (!map.has(row.item_name)) {
        map.set(row.item_name, []);
      }
      map.get(row.item_name).push(row);
    });
    map.forEach((series) => {
      series.sort((a, b) => a.sale_date.localeCompare(b.sale_date));
    });
    return map;
  }

  async function fetchAllRows(client) {
    const pageSize = 1000;
    let from = 0;
    const rows = [];

    while (true) {
      const { data, error } = await client
        .from("market_daily_item_stats")
        .select("sale_date,item_name,quantity,avg_price,high_price,low_price")
        .order("sale_date", { ascending: true })
        .order("item_name", { ascending: true })
        .range(from, from + pageSize - 1);

      if (error) {
        throw error;
      }

      if (!data || data.length === 0) {
        break;
      }

      data.forEach((r) => {
        rows.push({
          sale_date: r.sale_date,
          item_name: r.item_name,
          quantity: asNumber(r.quantity),
          // avg_price はPDFの「中値」列（販売価格中値）
          avg_price: asNumber(r.avg_price),
          high_price: asNumber(r.high_price),
          low_price: asNumber(r.low_price),
        });
      });

      if (data.length < pageSize) {
        break;
      }
      from += pageSize;
    }

    return rows;
  }

  function getItemCandidates(rows) {
    const latestDate = getLatestDate(rows);
    const latestRows = rows.filter((r) => r.sale_date === latestDate);
    latestRows.sort((a, b) => (b.quantity || 0) - (a.quantity || 0));
    return latestRows.map((r) => r.item_name);
  }

  // 品目の並び替え・検索用の読み（漢字表記のみ）。長い語から順に置換するため、
  // 「島バナナ」のような複合名は「島」+「バナナ」として読める。
  // ここにない漢字を含む品目は候補リストの末尾に並ぶので、見つけたら追記する。
  const KANJI_READINGS = {
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
  const KANJI_PATTERN = /[\u3400-\u9fff]/;
  const OTHER_PREFIX = "その他";
  const jaCollator = new Intl.Collator("ja");

  function itemReading(name) {
    let reading = String(name).normalize("NFKC");
    KANJI_READING_KEYS.forEach((kanji) => {
      reading = reading.split(kanji).join(KANJI_READINGS[kanji]);
    });
    return reading;
  }

  // あいうえお順。「その他◯◯」は通常品目の後ろにまとめ、「その他」を除いた読みで並べる。
  // 読みの分からない漢字を含む品目は各グループの末尾。
  function sortItemsByReading(items) {
    return items
      .map((name) => {
        const isOther = name.startsWith(OTHER_PREFIX);
        const reading = itemReading(isOther ? name.slice(OTHER_PREFIX.length) : name);
        return { name, reading, isOther, unknown: KANJI_PATTERN.test(reading) };
      })
      .sort(
        (a, b) =>
          a.isOther - b.isOther ||
          a.unknown - b.unknown ||
          jaCollator.compare(a.reading, b.reading) ||
          jaCollator.compare(a.name, b.name)
      )
      .map((x) => x.name);
  }

  function toKatakana(value) {
    return value.replace(/[\u3041-\u3096]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) + 0x60));
  }

  function normalizeForSearch(value) {
    return toKatakana(String(value).normalize("NFKC").toLowerCase()).replace(/\s+/g, "");
  }

  function trendColor(index) {
    return TREND_COLORS[index % TREND_COLORS.length];
  }

  function renderTrendChips() {
    clearChildren(trendChipsEl);
    const removable = state.trendItems.length > 1;
    state.trendItems.forEach((item, idx) => {
      const li = document.createElement("li");
      li.className = "chip";
      li.style.setProperty("--chip-color", trendColor(idx));

      const dot = document.createElement("span");
      dot.className = "chip-dot";
      dot.setAttribute("aria-hidden", "true");
      li.appendChild(dot);

      const label = document.createElement("span");
      label.className = "chip-label";
      label.textContent = item;
      li.appendChild(label);

      if (removable) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "chip-remove";
        btn.dataset.item = item;
        btn.setAttribute("aria-label", `${item}を外す`);
        btn.textContent = "×";
        li.appendChild(btn);
      }
      trendChipsEl.appendChild(li);
    });
  }

  function syncTrendOptions() {
    const lastOne = state.trendItems.length <= 1;
    Array.from(trendOptionsEl.querySelectorAll("input[type=checkbox]")).forEach((cb) => {
      cb.checked = state.trendItems.includes(cb.value);
      // 最低1品目は残す
      cb.disabled = lastOne && cb.checked;
    });
    trendSelCountEl.textContent = `${state.trendItems.length}品目を選択中`;
  }

  function buildTrendOptions(orderedItems) {
    clearChildren(trendOptionsEl);
    orderedItems.forEach((item) => {
      const li = document.createElement("li");
      li.dataset.search = `${normalizeForSearch(item)}|${normalizeForSearch(itemReading(item))}`;
      const label = document.createElement("label");
      label.className = "item-option";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.value = item;
      label.appendChild(cb);
      const text = document.createElement("span");
      text.textContent = item;
      label.appendChild(text);
      li.appendChild(label);
      trendOptionsEl.appendChild(li);
    });
    syncTrendOptions();
    applyTrendSearch();
  }

  function applyTrendSearch() {
    const query = normalizeForSearch(trendSearchEl.value);
    let visible = 0;
    Array.from(trendOptionsEl.children).forEach((li) => {
      const match = !query || li.dataset.search.includes(query);
      li.hidden = !match;
      if (match) {
        visible += 1;
      }
    });
    trendNoMatchEl.hidden = visible > 0;
  }

  function updateTrendItems(nextItems) {
    if (!nextItems.length) {
      return;
    }
    state.trendItems = nextItems;
    saveSelectorPrefs();
    renderTrendChips();
    syncTrendOptions();
    renderTrendChart(state.periodRows);
    renderWeeklyMap();
  }

  function ensureSelectors(periodRows) {
    const items = Array.from(new Set(periodRows.map((r) => r.item_name)));
    const ranked = getItemCandidates(periodRows).filter((item) => items.includes(item));
    // 初期選択は入荷量の多い順、選択候補の表示はあいうえお順
    const orderedItems = [...ranked, ...items.filter((i) => !ranked.includes(i))];
    const sortedItems = sortItemsByReading(orderedItems);

    clearChildren(focusItemEl);
    clearChildren(corrFocusItemEl);
    sortedItems.forEach((item) => {
      appendOption(focusItemEl, item, item);
      appendOption(corrFocusItemEl, item, item);
    });

    const defaultTrendCount = Math.min(Number(config.trendDefaultItems) || 6, orderedItems.length);
    if (!state.trendItems.length) {
      state.trendItems = orderedItems.slice(0, defaultTrendCount);
    } else {
      state.trendItems = state.trendItems.filter((item) => orderedItems.includes(item));
      if (!state.trendItems.length) {
        state.trendItems = orderedItems.slice(0, defaultTrendCount);
      }
    }
    buildTrendOptions(sortedItems);
    renderTrendChips();

    if (!state.focusItem || !orderedItems.includes(state.focusItem)) {
      state.focusItem = orderedItems[0] || "";
    }
    focusItemEl.value = state.focusItem;

    if (!state.corrFocusItem || !orderedItems.includes(state.corrFocusItem)) {
      state.corrFocusItem = orderedItems[0] || "";
    }
    corrFocusItemEl.value = state.corrFocusItem;
    saveSelectorPrefs();
  }

  function shiftYmd(ymd, days) {
    const d = parseISODate(ymd);
    d.setUTCDate(d.getUTCDate() + days);
    return d.toISOString().slice(0, 10);
  }

  function isPositive(value) {
    return Number.isFinite(value) && value > 0;
  }

  // 本日の販売価格中値を、同じ品目の過去N日（本日を含まない）の中値の中央値と比べる。
  // 表示期間の切り替えとは連動せず、常に全データの最新日を基準にする。
  function renderKpiCards() {
    const latestDate = getLatestDate(state.rows);
    if (!latestDate) {
      clearChildren(kpiCardsEl);
      kpiDateLabelEl.textContent = "";
      return;
    }

    const baseDays = Number(config.kpiBaselineDays) || 14;
    const minBaseDays = Number(config.kpiMinBaselineDays) || 7;
    document.getElementById("kpiBaseDays").textContent = String(baseDays);
    const baseStart = shiftYmd(latestDate, -baseDays);
    const baseEnd = shiftYmd(latestDate, -1);
    const deviations = [];

    state.seriesByItem.forEach((series, itemName) => {
      const today = series.find((r) => r.sale_date === latestDate);
      if (!today || !isPositive(today.avg_price) || !isPositive(today.quantity)) {
        return;
      }
      const baseRows = series.filter(
        (r) => r.sale_date >= baseStart && r.sale_date <= baseEnd && isPositive(r.avg_price)
      );
      if (baseRows.length < minBaseDays) {
        return;
      }
      const baseline = median(baseRows.map((r) => r.avg_price));
      const idx = series.indexOf(today);
      const prev = idx > 0 ? series[idx - 1] : null;
      deviations.push({
        item_name: itemName,
        current: today.avg_price,
        baseline,
        deviation: ((today.avg_price - baseline) / baseline) * 100,
        dayChange: prev && isPositive(prev.avg_price) ? ((today.avg_price - prev.avg_price) / prev.avg_price) * 100 : null,
      });
    });

    const high = deviations
      .filter((d) => d.deviation > 0)
      .sort((a, b) => b.deviation - a.deviation)
      .slice(0, 3);
    const low = deviations
      .filter((d) => d.deviation < 0)
      .sort((a, b) => a.deviation - b.deviation)
      .slice(0, 3);

    clearChildren(kpiCardsEl);
    const appendCard = (d, index, type) => {
      const article = document.createElement("article");
      article.className = `kpi-card ${type}`;

      const rank = document.createElement("div");
      rank.className = "kpi-rank";
      rank.textContent = `${type === "up" ? "割高" : "割安"} ${index + 1}`;
      article.appendChild(rank);

      const item = document.createElement("div");
      item.className = "kpi-item";
      item.textContent = d.item_name;
      article.appendChild(item);

      const rate = document.createElement("div");
      rate.className = `kpi-rate ${type}`;
      rate.textContent = fmtRate(d.deviation);
      article.appendChild(rate);

      [
        ["kpi-sub", `本日 ${fmtPrice(d.current)}`],
        ["kpi-sub", `${baseDays}日中央値 ${fmtPrice(d.baseline)}`],
        ["kpi-sub kpi-sub-minor", `前日比 ${d.dayChange == null ? "-" : fmtRate(d.dayChange)}`],
      ].forEach(([className, text]) => {
        const sub = document.createElement("div");
        sub.className = className;
        sub.textContent = text;
        article.appendChild(sub);
      });

      kpiCardsEl.appendChild(article);
    };

    high.forEach((d, i) => appendCard(d, i, "up"));
    low.forEach((d, i) => appendCard(d, i, "down"));
    if (!high.length && !low.length) {
      const p = document.createElement("p");
      p.className = "muted";
      p.textContent = "比較できる品目がありません。";
      kpiCardsEl.appendChild(p);
    }
    kpiDateLabelEl.textContent = `基準日: ${latestDate} | 比較期間: ${formatYmd(baseStart)}〜${formatYmd(baseEnd)}（取引${minBaseDays}日以上の品目）`;
  }

  function renderTrendChart(periodRows) {
    const dates = Array.from(new Set(periodRows.map((r) => r.sale_date))).sort();
    const itemSet = new Set(state.trendItems);
    const valueMap = new Map();
    periodRows.forEach((r) => {
      if (!itemSet.has(r.item_name)) {
        return;
      }
      valueMap.set(`${r.item_name}__${r.sale_date}`, r.avg_price);
    });

    const series = state.trendItems.map((item) => ({
      name: item,
      type: "line",
      smooth: true,
      showSymbol: false,
      data: dates.map((d) => valueMap.get(`${item}__${d}`) ?? null),
    }));

    trendChart.setOption(
      {
        animationDuration: 400,
        tooltip: { trigger: "axis" },
        legend: { top: 0, type: "scroll" },
        grid: { left: 48, right: 22, top: 36, bottom: 44 },
        xAxis: { type: "category", data: dates.map(formatYmd), axisLabel: { color: "#516050" } },
        yAxis: {
          type: "value",
          axisLabel: { color: "#516050", formatter: "{value}円" },
          splitLine: { lineStyle: { color: "#e7eee4" } },
        },
        color: TREND_COLORS,
        series,
      },
      true
    );
  }

  function renderComboChart(periodRows) {
    const itemRows = periodRows.filter((r) => r.item_name === state.focusItem);
    itemRows.sort((a, b) => a.sale_date.localeCompare(b.sale_date));
    const xData = itemRows.map((r) => formatYmd(r.sale_date));

    comboChart.setOption(
      {
        animationDuration: 400,
        tooltip: { trigger: "axis" },
        legend: { top: 0, data: ["入荷量", "販売価格中値"] },
        grid: { left: 48, right: 52, top: 36, bottom: 44 },
        xAxis: { type: "category", data: xData, axisLabel: { color: "#516050" } },
        yAxis: [
          {
            type: "value",
            name: "入荷量",
            axisLabel: { color: "#516050" },
            splitLine: { lineStyle: { color: "#e7eee4" } },
          },
          {
            type: "value",
            name: "価格",
            axisLabel: { color: "#516050", formatter: "{value}円" },
          },
        ],
        series: [
          {
            name: "入荷量",
            type: "bar",
            yAxisIndex: 0,
            barMaxWidth: 18,
            itemStyle: { color: "#78b98b", borderRadius: [4, 4, 0, 0] },
            data: itemRows.map((r) => r.quantity),
          },
          {
            name: "販売価格中値",
            type: "line",
            yAxisIndex: 1,
            smooth: true,
            symbolSize: 6,
            itemStyle: { color: "#d06f3b" },
            data: itemRows.map((r) => r.avg_price),
          },
        ],
      },
      true
    );
  }

  function median(values) {
    if (!values.length) {
      return 0;
    }
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 0) {
      return (sorted[mid - 1] + sorted[mid]) / 2;
    }
    return sorted[mid];
  }

  function quantile(values, q) {
    if (!values.length) {
      return 0;
    }
    const sorted = [...values].sort((a, b) => a - b);
    const pos = (sorted.length - 1) * q;
    const base = Math.floor(pos);
    const rest = pos - base;
    const next = sorted[base + 1];
    return next == null ? sorted[base] : sorted[base] + rest * (next - sorted[base]);
  }

  function rankValues(values) {
    const pairs = values.map((v, idx) => ({ v, idx }));
    pairs.sort((a, b) => a.v - b.v);
    const ranks = new Array(values.length).fill(0);
    let i = 0;
    while (i < pairs.length) {
      let j = i + 1;
      while (j < pairs.length && pairs[j].v === pairs[i].v) {
        j += 1;
      }
      const rank = (i + j - 1) / 2 + 1;
      for (let k = i; k < j; k += 1) {
        ranks[pairs[k].idx] = rank;
      }
      i = j;
    }
    return ranks;
  }

  function pearson(x, y) {
    const n = x.length;
    if (n < 2) {
      return NaN;
    }
    const mx = x.reduce((a, b) => a + b, 0) / n;
    const my = y.reduce((a, b) => a + b, 0) / n;
    let num = 0;
    let sx = 0;
    let sy = 0;
    for (let i = 0; i < n; i += 1) {
      const dx = x[i] - mx;
      const dy = y[i] - my;
      num += dx * dy;
      sx += dx * dx;
      sy += dy * dy;
    }
    const den = Math.sqrt(sx * sy);
    return den === 0 ? NaN : num / den;
  }

  function spearman(x, y) {
    return pearson(rankValues(x), rankValues(y));
  }

  function computeCorrelationData(periodRows) {
    const uniqueDates = Array.from(new Set(periodRows.map((r) => r.sale_date)));
    const maxPossibleOverlap = Math.max(2, uniqueDates.length - 1);
    const byItem = buildSeriesByItem(periodRows);
    const rawReturnsByItem = new Map();
    const dayReturns = new Map();

    byItem.forEach((series, item) => {
      const returns = [];
      for (let i = 1; i < series.length; i += 1) {
        const prev = series[i - 1].avg_price;
        const curr = series[i].avg_price;
        if (!(prev > 0) || !(curr > 0)) {
          continue;
        }
        const date = series[i].sale_date;
        const r = Math.log(curr) - Math.log(prev);
        returns.push({ date, value: r });
        if (!dayReturns.has(date)) {
          dayReturns.set(date, []);
        }
        dayReturns.get(date).push(r);
      }
      if (returns.length) {
        rawReturnsByItem.set(item, returns);
      }
    });

    const dayMedian = new Map();
    dayReturns.forEach((vals, date) => {
      dayMedian.set(date, median(vals));
    });

    const standardizedByItem = new Map();
    rawReturnsByItem.forEach((series, item) => {
      const adjusted = series.map((p) => ({ date: p.date, value: p.value - (dayMedian.get(p.date) || 0) }));
      const arr = adjusted.map((p) => p.value);
      if (arr.length < 4) {
        return;
      }
      const q01 = quantile(arr, 0.01);
      const q99 = quantile(arr, 0.99);
      const clipped = adjusted.map((p) => ({
        date: p.date,
        value: Math.max(q01, Math.min(q99, p.value)),
      }));
      const clippedValues = clipped.map((p) => p.value);
      const med = median(clippedValues);
      const mad = median(clippedValues.map((v) => Math.abs(v - med)));
      const scale = mad > 1e-9 ? mad * 1.4826 : (Math.sqrt(clippedValues.reduce((a, v) => a + (v - med) ** 2, 0) / clippedValues.length) || 1);
      const zMap = new Map();
      clipped.forEach((p) => {
        zMap.set(p.date, (p.value - med) / scale);
      });
      standardizedByItem.set(item, zMap);
    });

    const items = Array.from(standardizedByItem.keys()).sort();
    const minOverlap = Math.min(Number(config.corrMinOverlapDays) || 30, maxPossibleOverlap);
    const pairs = [];

    for (let i = 0; i < items.length; i += 1) {
      for (let j = i + 1; j < items.length; j += 1) {
        const a = items[i];
        const b = items[j];
        const mapA = standardizedByItem.get(a);
        const mapB = standardizedByItem.get(b);
        const x = [];
        const y = [];
        mapA.forEach((va, date) => {
          if (mapB.has(date)) {
            x.push(va);
            y.push(mapB.get(date));
          }
        });
        if (x.length < minOverlap) {
          continue;
        }
        const p = pearson(x, y);
        const s = spearman(x, y);
        if (!Number.isFinite(p) || !Number.isFinite(s)) {
          continue;
        }
        pairs.push({
          itemA: a,
          itemB: b,
          pearson: p,
          spearman: s,
          score: 0.6 * s + 0.4 * p,
          overlap: x.length,
        });
      }
    }
    return { items, pairs, minOverlap };
  }

  function renderPairTable(targetEl, rows, emptyText) {
    if (!rows.length) {
      renderEmptyTableRow(targetEl, 4, emptyText);
      return;
    }
    clearChildren(targetEl);
    rows.forEach((row, idx) => {
      const zone = scoreBand(row.score);
      const tr = document.createElement("tr");
      appendTableCell(tr, String(idx + 1), "rank");
      appendTableCell(tr, `${row.itemA} × ${row.itemB}`);
      appendTableCell(tr, fmtCorr(row.score), `corr score-band-${zone.band}`, zone.label);
      appendTableCell(tr, String(row.overlap));
      targetEl.appendChild(tr);
    });
  }

  function renderUnitPriceLollipop(periodRows) {
    const latestDate = getLatestDate(periodRows);
    if (!latestDate) {
      unitPriceMetaLabelEl.textContent = "表示データがありません。";
      unitPriceChart.setOption(
        {
          animationDuration: 300,
          title: {
            text: "表示できるデータがありません。",
            left: "center",
            top: "middle",
            textStyle: { color: "#5b6c5a", fontSize: 14, fontWeight: 500 },
          },
        },
        true
      );
      return;
    }

    const latest = parseISODate(latestDate);
    const windowStartDate = new Date(latest);
    windowStartDate.setUTCDate(windowStartDate.getUTCDate() - 6);
    const windowStart = windowStartDate.toISOString().slice(0, 10);

    const windowRows = periodRows.filter(
      (r) =>
        r.sale_date >= windowStart &&
        r.sale_date <= latestDate &&
        Number.isFinite(r.avg_price) &&
        Number.isFinite(r.quantity) &&
        (r.quantity || 0) > 0
    );

    const aggByItem = new Map();
    windowRows.forEach((r) => {
      const key = r.item_name;
      if (!aggByItem.has(key)) {
        aggByItem.set(key, { weightedSum: 0, qtySum: 0 });
      }
      const acc = aggByItem.get(key);
      const qty = r.quantity || 0;
      const price = r.avg_price || 0;
      acc.weightedSum += price * qty;
      acc.qtySum += qty;
    });

    const weightedRows = Array.from(aggByItem.entries())
      .map(([item_name, acc]) => ({
        item_name,
        weighted_avg_price: acc.qtySum > 0 ? acc.weightedSum / acc.qtySum : null,
        total_quantity: acc.qtySum,
      }))
      .filter((r) => Number.isFinite(r.weighted_avg_price))
      .sort((a, b) => (b.weighted_avg_price || 0) - (a.weighted_avg_price || 0));

    if (!weightedRows.length) {
      unitPriceMetaLabelEl.textContent = `対象期間: ${windowStart}〜${latestDate} | 販売価格中値の加重平均データがありません。`;
      unitPriceChart.setOption(
        {
          animationDuration: 300,
          title: {
            text: "販売価格中値の7日加重平均データがありません。",
            left: "center",
            top: "middle",
            textStyle: { color: "#5b6c5a", fontSize: 14, fontWeight: 500 },
          },
        },
        true
      );
      return;
    }

    const prices = weightedRows.map((r) => r.weighted_avg_price || 0);
    const maxPrice = Math.max(...prices);
    const axisMax = maxPrice > 0 ? maxPrice * 1.08 : 1;
    const medianPrice = median(prices);
    const q75 = quantile(prices, 0.75);
    const q25 = quantile(prices, 0.25);

    unitPriceMetaLabelEl.textContent = `対象期間: ${windowStart}〜${latestDate} | 品目数: ${weightedRows.length} | 中央値: ${fmtPrice(medianPrice)}`;

    const yItems = weightedRows.map((r) => r.item_name);
    const values = weightedRows.map((r) => r.weighted_avg_price || 0);
    const initialVisibleCount = Math.min(26, weightedRows.length);
    const endValue = Math.max(0, initialVisibleCount - 1);

    unitPriceChart.setOption(
      {
        animationDuration: 400,
        tooltip: {
          trigger: "item",
          formatter: (p) => {
            const idx = p.dataIndex;
            const row = weightedRows[idx];
            return `${row.item_name}<br/>販売価格中値（7日加重平均）: ${fmtPrice(row.weighted_avg_price)}<br/>7日累計入荷量: ${(row.total_quantity || 0).toLocaleString("ja-JP")}`;
          },
        },
        grid: { left: 130, right: 48, top: 14, bottom: 58 },
        xAxis: {
          type: "value",
          min: 0,
          max: axisMax,
          axisLabel: { color: "#516050", formatter: (v) => `${Math.round(v).toLocaleString("ja-JP")}円` },
          splitLine: { lineStyle: { color: "#e7eee4" } },
        },
        yAxis: {
          type: "category",
          inverse: true,
          data: yItems,
          axisLabel: { color: "#2e3f2f", fontSize: 11 },
          axisTick: { show: false },
        },
        dataZoom: [
          {
            type: "inside",
            yAxisIndex: 0,
            startValue: 0,
            endValue,
            zoomOnMouseWheel: true,
            moveOnMouseMove: true,
          },
          {
            type: "slider",
            yAxisIndex: 0,
            right: 10,
            width: 10,
            top: 22,
            bottom: 64,
            startValue: 0,
            endValue,
          },
        ],
        series: [
          {
            type: "bar",
            data: values,
            barWidth: 3,
            itemStyle: { color: "#9ec8ac" },
            emphasis: { disabled: true },
            markLine: {
              symbol: "none",
              label: { show: false },
              lineStyle: { color: "#94a590", type: "dashed", width: 1 },
              data: [{ xAxis: q25 }, { xAxis: medianPrice }, { xAxis: q75 }],
            },
            z: 1,
          },
          {
            type: "scatter",
            data: values,
            symbolSize: 9,
            itemStyle: { color: "#d06f3b", borderColor: "#fff", borderWidth: 1.2 },
            z: 3,
          },
        ],
      },
      true
    );
  }

  function renderFocusRanking(corrData) {
    const focus = state.corrFocusItem;
    if (!focus) {
      renderEmptyTableRow(corrFocusRankingBodyEl, 4, "品目を選択してください。");
      return;
    }
    const related = corrData.pairs
      .filter((p) => p.itemA === focus || p.itemB === focus)
      .map((p) => ({
        other: p.itemA === focus ? p.itemB : p.itemA,
        score: p.score,
        overlap: p.overlap,
      }))
      .sort((a, b) => Math.abs(b.score) - Math.abs(a.score))
      .slice(0, 20);

    if (!related.length) {
      renderEmptyTableRow(corrFocusRankingBodyEl, 4, "十分な共通日数のペアがありません。");
      return;
    }
    clearChildren(corrFocusRankingBodyEl);
    related.forEach((r, idx) => {
      const zone = scoreBand(r.score);
      const tr = document.createElement("tr");
      appendTableCell(tr, String(idx + 1), "rank");
      appendTableCell(tr, r.other);
      appendTableCell(tr, fmtCorr(r.score), `corr score-band-${zone.band}`, zone.label);
      appendTableCell(tr, String(r.overlap));
      corrFocusRankingBodyEl.appendChild(tr);
    });
  }

  function renderCorrelationTables(periodRows) {
    const corrData = computeCorrelationData(periodRows);
    const top = [...corrData.pairs].sort((a, b) => b.score - a.score).slice(0, 20);
    const bottom = [...corrData.pairs].sort((a, b) => a.score - b.score).slice(0, 20);

    renderPairTable(corrTopPairsBodyEl, top, "表示可能な相関ペアがありません。");
    renderPairTable(corrBottomPairsBodyEl, bottom, "表示可能な相関ペアがありません。");
    renderFocusRanking(corrData);

    corrMetaLabelEl.textContent = `期間: ${periodLabel(state.periodDays)} | 最低共通日数: ${corrData.minOverlap} 日 | ペア数: ${corrData.pairs.length}`;
  }

  function weightedPrice(rows) {
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

  function sumQuantity(rows) {
    return rows.reduce((acc, r) => acc + (isPositive(r.quantity) ? r.quantity : 0), 0);
  }

  // 直近7日と前の7日を比べた、入荷量の変化率(横軸)と販売価格中値の変化率(縦軸)。
  // 中値は入荷量で加重平均する。表示期間の切り替えとは連動しない。
  function renderWeeklyMap() {
    const latestDate = getLatestDate(state.rows);
    if (!latestDate) {
      weeklyMapMetaLabelEl.textContent = "";
      weeklyMapNoticeEl.hidden = true;
      weeklyMapChart.clear();
      return;
    }
    const recentStart = shiftYmd(latestDate, -6);
    const priorStart = shiftYmd(latestDate, -13);
    const priorEnd = shiftYmd(latestDate, -7);
    const topN = Number(config.weeklyMapTopItems) || 20;

    const points = [];
    state.seriesByItem.forEach((series, itemName) => {
      const recent = series.filter((r) => r.sale_date >= recentStart && r.sale_date <= latestDate);
      const prior = series.filter((r) => r.sale_date >= priorStart && r.sale_date <= priorEnd);
      const qRecent = sumQuantity(recent);
      const qPrior = sumQuantity(prior);
      const pRecent = weightedPrice(recent);
      const pPrior = weightedPrice(prior);
      if (!(qRecent > 0) || !(qPrior > 0) || pRecent == null || pPrior == null) {
        return;
      }
      points.push({
        item_name: itemName,
        qRecent,
        qPrior,
        pRecent,
        pPrior,
        qtyChange: ((qRecent - qPrior) / qPrior) * 100,
        priceChange: ((pRecent - pPrior) / pPrior) * 100,
        total: qRecent + qPrior,
      });
    });
    points.sort((a, b) => b.total - a.total);
    // 2軸グラフ・品目別価格推移で選択中の品目は、入荷量の順位に関係なく必ず表示する
    const selectedItems = [...new Set([state.focusItem, ...state.trendItems].filter(Boolean))];
    const selectedSet = new Set(selectedItems);
    const topPoints = points.slice(0, topN);
    const topSet = new Set(topPoints.map((p) => p.item_name));
    const extraPoints = points.filter((p) => selectedSet.has(p.item_name) && !topSet.has(p.item_name));
    const shown = [...topPoints, ...extraPoints];
    const shownSet = new Set(shown.map((p) => p.item_name));
    const missing = selectedItems.filter((item) => !shownSet.has(item));

    weeklyMapMetaLabelEl.textContent = `直近7日（${formatYmd(recentStart)}〜${formatYmd(latestDate)}） vs 前の7日（${formatYmd(priorStart)}〜${formatYmd(priorEnd)}） | 入荷量上位${topPoints.length}品目${extraPoints.length ? `＋選択中の${extraPoints.length}品目` : ""}`;
    weeklyMapNoticeEl.textContent = missing.length
      ? `選択中の${missing.join("、")}は、どちらかの7日間に入荷がないため表示できません。`
      : "";
    weeklyMapNoticeEl.hidden = !missing.length;

    if (!shown.length) {
      weeklyMapChart.setOption(
        {
          title: {
            text: "比較できる品目がありません。",
            left: "center",
            top: "middle",
            textStyle: { color: "#5b6c5a", fontSize: 14, fontWeight: 500 },
          },
        },
        true
      );
      return;
    }

    const niceMax = (values) => Math.ceil((Math.max(10, ...values.map(Math.abs)) * 1.15) / 20) * 20;
    const xMax = niceMax(shown.map((p) => p.qtyChange));
    const yMax = niceMax(shown.map((p) => p.priceChange));
    const maxTotal = Math.max(...shown.map((p) => p.total));
    // 変化の大きい品目と、2軸グラフで選択中の品目だけ名前を出す
    const labelCount = Number(config.weeklyMapLabelItems) || 6;
    const labeled = new Set(
      [...shown]
        .sort(
          (a, b) =>
            Math.hypot(b.qtyChange / xMax, b.priceChange / yMax) - Math.hypot(a.qtyChange / xMax, a.priceChange / yMax)
        )
        .slice(0, labelCount)
        .map((p) => p.item_name)
    );
    selectedItems.forEach((item) => labeled.add(item));

    const fmtPct = (v) => `${v > 0 ? "+" : ""}${v.toFixed(1)}%`;
    const toMapPoint = (d) => {
      const isFocus = d.item_name === state.focusItem;
      const trendIdx = state.trendItems.indexOf(d.item_name);
      const isSelected = isFocus || trendIdx >= 0;
      return {
        name: d.item_name,
        value: [d.qtyChange, d.priceChange],
        point: d,
        symbolSize: 8 + 16 * Math.sqrt(d.total / maxTotal),
        // オレンジ: 2軸グラフの品目 / 線と同じ色: 品目別価格推移の品目 / 灰色: その他
        itemStyle: isFocus
          ? { color: "#d06f3b", borderColor: "#7a3514", borderWidth: 2, opacity: 1 }
          : isSelected
            ? { color: trendColor(trendIdx), borderColor: "#fff", borderWidth: 1.5, opacity: 0.95 }
            : { color: "#a3b0a1", opacity: 0.6 },
        label: {
          show: labeled.has(d.item_name),
          formatter: "{b}",
          position: "top",
          color: isFocus ? "#b0552a" : isSelected ? "#1f2d20" : "#5b6c5a",
          fontSize: 11,
          fontWeight: isSelected ? 700 : 400,
        },
      };
    };

    weeklyMapChart.setOption(
      {
        animationDuration: 400,
        tooltip: {
          trigger: "item",
          formatter: (p) => {
            const d = p.data && p.data.point;
            if (!d) {
              return "";
            }
            return [
              `<b>${d.item_name}</b>`,
              `入荷量: ${fmtPct(d.qtyChange)}（${Math.round(d.qPrior).toLocaleString("ja-JP")} → ${Math.round(d.qRecent).toLocaleString("ja-JP")}）`,
              `販売価格中値: ${fmtPct(d.priceChange)}（${fmtPrice(d.pPrior)} → ${fmtPrice(d.pRecent)}）`,
            ].join("<br/>");
          },
        },
        grid: { left: 52, right: 20, top: 16, bottom: 52 },
        xAxis: {
          type: "value",
          min: -xMax,
          max: xMax,
          // スマホ幅でラベルが詰まらないよう横軸は目盛りを少なめに
          interval: xMax / 2,
          name: "入荷量 前週比",
          nameLocation: "middle",
          nameGap: 30,
          nameTextStyle: { color: "#516050" },
          axisLabel: { color: "#516050", formatter: (v) => `${Math.round(v)}%` },
          splitLine: { lineStyle: { color: "#e7eee4" } },
        },
        yAxis: {
          type: "value",
          min: -yMax,
          max: yMax,
          interval: yMax / 4,
          name: "中値 前週比",
          nameLocation: "middle",
          nameGap: 40,
          nameTextStyle: { color: "#516050" },
          axisLabel: { color: "#516050", formatter: (v) => `${Math.round(v)}%` },
          splitLine: { lineStyle: { color: "#e7eee4" } },
        },
        series: [
          {
            type: "scatter",
            silent: true,
            symbolSize: 0,
            data: [],
            // 左上（品薄・値上がり）と右下（出回り増・値下がり）を薄く色付け
            markArea: {
              silent: true,
              data: [
                [{ coord: [-xMax, 0], itemStyle: { color: "rgba(206, 77, 65, 0.07)" } }, { coord: [0, yMax] }],
                [{ coord: [0, -yMax], itemStyle: { color: "rgba(26, 118, 162, 0.07)" } }, { coord: [xMax, 0] }],
              ],
            },
            markLine: {
              silent: true,
              symbol: "none",
              label: { show: false },
              lineStyle: { color: "#94a590", type: "solid", width: 1 },
              data: [{ xAxis: 0 }, { yAxis: 0 }],
            },
            z: 1,
          },
          // 選択外の品目（重なったラベルは隠す）
          {
            type: "scatter",
            cursor: "pointer",
            labelLayout: { hideOverlap: true },
            data: shown.filter((d) => !selectedSet.has(d.item_name)).map(toMapPoint),
            z: 2,
          },
          // 選択中の品目（ラベルは常に表示）
          {
            type: "scatter",
            cursor: "pointer",
            labelLayout: { moveOverlap: "shiftY" },
            data: shown.filter((d) => selectedSet.has(d.item_name)).map(toMapPoint),
            z: 3,
          },
        ],
      },
      true
    );
  }

  function setFocusItem(item) {
    if (!item || !Array.from(focusItemEl.options).some((opt) => opt.value === item)) {
      return;
    }
    state.focusItem = item;
    focusItemEl.value = item;
    saveSelectorPrefs();
    renderComboChart(state.periodRows);
    renderWeeklyMap();
  }

  function renderAll() {
    const periodRows = filterRowsByPeriod(state.rows, state.periodDays);
    state.periodRows = periodRows;
    ensureSelectors(periodRows);
    renderKpiCards();
    renderTrendChart(periodRows);
    renderComboChart(periodRows);
    renderWeeklyMap();
    if (!corrSectionEl.hidden) {
      renderCorrelationTables(periodRows);
    }
    renderUnitPriceLollipop(periodRows);
    setStatus(`表示期間: ${periodLabel(state.periodDays)} | データ件数: ${periodRows.length}`);
  }

  function attachEvents() {
    periodButtons.forEach((b) => {
      b.classList.toggle("is-active", parsePeriodDays(b.dataset.days) === state.periodDays);
    });

    periodButtons.forEach((btn) => {
      btn.addEventListener("click", () => {
        const days = parsePeriodDays(btn.dataset.days);
        if (days == null) {
          return;
        }
        state.periodDays = days;
        periodButtons.forEach((b) => b.classList.toggle("is-active", b === btn));
        saveSelectorPrefs();
        renderAll();
      });
    });

    trendChipsEl.addEventListener("click", (event) => {
      const btn = event.target.closest(".chip-remove");
      if (!btn) {
        return;
      }
      updateTrendItems(state.trendItems.filter((item) => item !== btn.dataset.item));
    });

    trendAddBtn.addEventListener("click", () => {
      trendSearchEl.value = "";
      applyTrendSearch();
      syncTrendOptions();
      trendDialogEl.showModal();
      trendOptionsEl.scrollTop = 0;
    });

    // 背景（ダイアログ外）タップで閉じる
    trendDialogEl.addEventListener("click", (event) => {
      if (event.target === trendDialogEl) {
        trendDialogEl.close();
      }
    });

    trendSearchEl.addEventListener("input", applyTrendSearch);

    trendOptionsEl.addEventListener("change", (event) => {
      const cb = event.target;
      if (!(cb instanceof HTMLInputElement) || cb.type !== "checkbox") {
        return;
      }
      const next = cb.checked
        ? [...state.trendItems.filter((item) => item !== cb.value), cb.value]
        : state.trendItems.filter((item) => item !== cb.value);
      updateTrendItems(next);
    });

    focusItemEl.addEventListener("change", () => {
      setFocusItem(focusItemEl.value);
    });

    weeklyMapChart.on("click", (params) => {
      if (params.data && params.data.point) {
        setFocusItem(params.data.point.item_name);
      }
    });

    corrFocusItemEl.addEventListener("change", () => {
      state.corrFocusItem = corrFocusItemEl.value;
      saveSelectorPrefs();
      renderAll();
    });

    reloadBtn.addEventListener("click", () => {
      window.location.reload();
    });

    window.addEventListener("resize", () => {
      trendChart.resize();
      comboChart.resize();
      weeklyMapChart.resize();
      unitPriceChart.resize();
    });
  }

  async function main() {
    if (!config.supabaseUrl || !config.supabaseAnonKey || config.supabaseUrl.includes("YOUR-PROJECT-REF")) {
      setStatus("config.js の Supabase 設定を入力してください。");
      return;
    }

    // 期間ボタンの初期表示に反映するため、イベント登録より先に復元する
    loadSelectorPrefs();
    attachEvents();
    await renderRecentUpdates();
    setStatus("Supabaseからデータを取得中...");

    try {
      const client = window.supabase.createClient(config.supabaseUrl, config.supabaseAnonKey);
      state.analyticsClient = client;
      setupErrorLogging(client);
      void logUsageEvent(client, {
        visitor_id: getVisitorId(),
        event_type: "page_view",
        page_path: window.location.pathname || "/",
        metadata: {
          referrer: document.referrer || "",
        },
      });
      state.rows = await fetchAllRows(client);
      state.rows.sort((a, b) => a.sale_date.localeCompare(b.sale_date));
      state.seriesByItem = buildSeriesByItem(state.rows);
      if (!state.rows.length) {
        setStatus("表示できるデータがありません。");
        return;
      }
      renderAll();
    } catch (error) {
      if (state.analyticsClient) {
        void logUsageEvent(state.analyticsClient, {
          visitor_id: getVisitorId(),
          event_type: "error",
          page_path: window.location.pathname || "/",
          error_code: "dashboard_load_failed",
          message_summary: sanitizeMessage(error.message || String(error)),
          metadata: {},
        });
      }
      setStatus(`読み込み失敗: ${error.message || String(error)}`);
    }
  }

  main();
})();
