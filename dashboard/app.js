(function () {
  const config = window.DASHBOARD_CONFIG || {};
  const statusEl = document.getElementById("statusMessage");
  const kpiCardsEl = document.getElementById("kpiCards");
  const kpiDateLabelEl = document.getElementById("kpiDateLabel");
  const trendItemsEl = document.getElementById("trendItems");
  const focusItemEl = document.getElementById("focusItem");
  const corrFocusItemEl = document.getElementById("corrFocusItem");
  const corrMetaLabelEl = document.getElementById("corrMetaLabel");
  const corrTopPairsBodyEl = document.getElementById("corrTopPairsBody");
  const corrBottomPairsBodyEl = document.getElementById("corrBottomPairsBody");
  const corrFocusRankingBodyEl = document.getElementById("corrFocusRankingBody");
  const reloadBtn = document.getElementById("reloadBtn");
  const periodButtons = Array.from(document.querySelectorAll(".period-btn"));

  const state = {
    rows: [],
    seriesByItem: new Map(),
    periodDays: Number(config.defaultDays) || 30,
    trendItems: [],
    focusItem: "",
    corrFocusItem: "",
    analyticsClient: null,
  };

  const trendChart = echarts.init(document.getElementById("trendChart"));
  const comboChart = echarts.init(document.getElementById("comboChart"));
  const VISITOR_ID_KEY = "agri_dashboard_visitor_id";

  function setStatus(message) {
    statusEl.textContent = message;
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

  function filterRowsByPeriod(rows, periodDays) {
    if (!rows.length) {
      return [];
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

  function ensureSelectors(periodRows) {
    const items = Array.from(new Set(periodRows.map((r) => r.item_name)));
    const ranked = getItemCandidates(periodRows).filter((item) => items.includes(item));
    const orderedItems = [...ranked, ...items.filter((i) => !ranked.includes(i))];

    trendItemsEl.innerHTML = orderedItems.map((item) => `<option value="${item}">${item}</option>`).join("");
    focusItemEl.innerHTML = orderedItems.map((item) => `<option value="${item}">${item}</option>`).join("");
    corrFocusItemEl.innerHTML = orderedItems.map((item) => `<option value="${item}">${item}</option>`).join("");

    const defaultTrendCount = Math.min(Number(config.trendDefaultItems) || 6, orderedItems.length);
    if (!state.trendItems.length) {
      state.trendItems = orderedItems.slice(0, defaultTrendCount);
    } else {
      state.trendItems = state.trendItems.filter((item) => orderedItems.includes(item));
      if (!state.trendItems.length) {
        state.trendItems = orderedItems.slice(0, defaultTrendCount);
      }
    }
    Array.from(trendItemsEl.options).forEach((opt) => {
      opt.selected = state.trendItems.includes(opt.value);
    });

    if (!state.focusItem || !orderedItems.includes(state.focusItem)) {
      state.focusItem = orderedItems[0] || "";
    }
    focusItemEl.value = state.focusItem;

    if (!state.corrFocusItem || !orderedItems.includes(state.corrFocusItem)) {
      state.corrFocusItem = orderedItems[0] || "";
    }
    corrFocusItemEl.value = state.corrFocusItem;
  }

  function renderKpiCards(periodRows) {
    const latestDate = getLatestDate(periodRows);
    if (!latestDate) {
      kpiCardsEl.innerHTML = "";
      kpiDateLabelEl.textContent = "";
      return;
    }

    const latestRows = periodRows.filter((r) => r.sale_date === latestDate);
    const changes = [];

    latestRows.forEach((row) => {
      const series = state.seriesByItem.get(row.item_name) || [];
      const idx = series.findIndex((x) => x.sale_date === latestDate);
      if (idx <= 0) {
        return;
      }
      const prev = series[idx - 1];
      if (!prev || prev.avg_price == null || prev.avg_price === 0 || row.avg_price == null) {
        return;
      }
      const changeRate = ((row.avg_price - prev.avg_price) / prev.avg_price) * 100;
      changes.push({
        item_name: row.item_name,
        current: row.avg_price,
        previous: prev.avg_price,
        changeRate,
      });
    });

    changes.sort((a, b) => b.changeRate - a.changeRate);
    const top = changes.slice(0, 3);
    const bottom = [...changes].sort((a, b) => a.changeRate - b.changeRate).slice(0, 3);

    const topCards = top.map((d, i) => {
      return `
        <article class="kpi-card up">
          <div class="kpi-rank">上昇 ${i + 1}</div>
          <div class="kpi-item">${d.item_name}</div>
          <div class="kpi-rate up">${fmtRate(d.changeRate)}</div>
          <div class="kpi-sub">${fmtPrice(d.current)} / 前日 ${fmtPrice(d.previous)}</div>
        </article>
      `;
    });

    const bottomCards = bottom.map((d, i) => {
      return `
        <article class="kpi-card down">
          <div class="kpi-rank">下落 ${i + 1}</div>
          <div class="kpi-item">${d.item_name}</div>
          <div class="kpi-rate down">${fmtRate(d.changeRate)}</div>
          <div class="kpi-sub">${fmtPrice(d.current)} / 前日 ${fmtPrice(d.previous)}</div>
        </article>
      `;
    });

    kpiCardsEl.innerHTML = [...topCards, ...bottomCards].join("");
    kpiDateLabelEl.textContent = `基準日: ${latestDate}`;
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
        legend: { top: 0, data: ["入荷量", "平均価格"] },
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
            name: "平均価格",
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
      targetEl.innerHTML = `<tr><td colspan="4">${emptyText}</td></tr>`;
      return;
    }
    targetEl.innerHTML = rows
      .map((row, idx) => {
        const zone = scoreBand(row.score);
        return `
          <tr>
            <td class="rank">${idx + 1}</td>
            <td>${row.itemA} × ${row.itemB}</td>
            <td class="corr score-band-${zone.band}" title="${zone.label}">${fmtCorr(row.score)}</td>
            <td>${row.overlap}</td>
          </tr>
        `;
      })
      .join("");
  }

  function renderFocusRanking(corrData) {
    const focus = state.corrFocusItem;
    if (!focus) {
      corrFocusRankingBodyEl.innerHTML = '<tr><td colspan="4">品目を選択してください。</td></tr>';
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
      corrFocusRankingBodyEl.innerHTML = '<tr><td colspan="4">十分な共通日数のペアがありません。</td></tr>';
      return;
    }
    corrFocusRankingBodyEl.innerHTML = related
      .map((r, idx) => {
        const zone = scoreBand(r.score);
        return `
          <tr>
            <td class="rank">${idx + 1}</td>
            <td>${r.other}</td>
            <td class="corr score-band-${zone.band}" title="${zone.label}">${fmtCorr(r.score)}</td>
            <td>${r.overlap}</td>
          </tr>
        `;
      })
      .join("");
  }

  function renderCorrelationTables(periodRows) {
    const corrData = computeCorrelationData(periodRows);
    const top = [...corrData.pairs].sort((a, b) => b.score - a.score).slice(0, 20);
    const bottom = [...corrData.pairs].sort((a, b) => a.score - b.score).slice(0, 20);

    renderPairTable(corrTopPairsBodyEl, top, "表示可能な相関ペアがありません。");
    renderPairTable(corrBottomPairsBodyEl, bottom, "表示可能な相関ペアがありません。");
    renderFocusRanking(corrData);

    corrMetaLabelEl.textContent = `期間: 直近 ${state.periodDays} 日 | 最低共通日数: ${corrData.minOverlap} 日 | ペア数: ${corrData.pairs.length}`;
  }

  function renderAll() {
    const periodRows = filterRowsByPeriod(state.rows, state.periodDays);
    ensureSelectors(periodRows);
    renderKpiCards(periodRows);
    renderTrendChart(periodRows);
    renderComboChart(periodRows);
    renderCorrelationTables(periodRows);
    setStatus(`表示期間: 直近 ${state.periodDays} 日 | データ件数: ${periodRows.length}`);
  }

  function attachEvents() {
    periodButtons.forEach((b) => {
      b.classList.toggle("is-active", Number(b.dataset.days) === state.periodDays);
    });

    periodButtons.forEach((btn) => {
      btn.addEventListener("click", () => {
        const days = Number(btn.dataset.days);
        if (!Number.isFinite(days) || days <= 0) {
          return;
        }
        state.periodDays = days;
        periodButtons.forEach((b) => b.classList.toggle("is-active", b === btn));
        renderAll();
      });
    });

    trendItemsEl.addEventListener("change", () => {
      const selected = Array.from(trendItemsEl.selectedOptions).map((opt) => opt.value);
      if (selected.length) {
        state.trendItems = selected;
        renderAll();
      }
    });

    focusItemEl.addEventListener("change", () => {
      state.focusItem = focusItemEl.value;
      renderAll();
    });

    corrFocusItemEl.addEventListener("change", () => {
      state.corrFocusItem = corrFocusItemEl.value;
      renderAll();
    });

    reloadBtn.addEventListener("click", () => {
      window.location.reload();
    });

    window.addEventListener("resize", () => {
      trendChart.resize();
      comboChart.resize();
    });
  }

  async function main() {
    if (!config.supabaseUrl || !config.supabaseAnonKey || config.supabaseUrl.includes("YOUR-PROJECT-REF")) {
      setStatus("config.js の Supabase 設定を入力してください。");
      return;
    }

    attachEvents();
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
