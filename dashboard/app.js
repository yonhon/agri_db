(function () {
  const config = window.DASHBOARD_CONFIG || {};
  const statusEl = document.getElementById("statusMessage");
  const kpiCardsEl = document.getElementById("kpiCards");
  const kpiDateLabelEl = document.getElementById("kpiDateLabel");
  const trendItemsEl = document.getElementById("trendItems");
  const focusItemEl = document.getElementById("focusItem");
  const reloadBtn = document.getElementById("reloadBtn");
  const periodButtons = Array.from(document.querySelectorAll(".period-btn"));

  const state = {
    rows: [],
    seriesByItem: new Map(),
    periodDays: Number(config.defaultDays) || 30,
    trendItems: [],
    focusItem: "",
  };

  const trendChart = echarts.init(document.getElementById("trendChart"));
  const comboChart = echarts.init(document.getElementById("comboChart"));
  const heatmapChart = echarts.init(document.getElementById("heatmapChart"));

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

    trendItemsEl.innerHTML = orderedItems
      .map((item) => `<option value="${item}">${item}</option>`)
      .join("");
    focusItemEl.innerHTML = orderedItems
      .map((item) => `<option value="${item}">${item}</option>`)
      .join("");

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

    const cards = [...topCards, ...bottomCards];
    kpiCardsEl.innerHTML = cards.join("");
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
      const key = `${r.item_name}__${r.sale_date}`;
      valueMap.set(key, r.avg_price);
    });

    const series = state.trendItems.map((item) => {
      const data = dates.map((d) => valueMap.get(`${item}__${d}`) ?? null);
      return {
        name: item,
        type: "line",
        smooth: true,
        showSymbol: false,
        data,
      };
    });

    trendChart.setOption(
      {
        animationDuration: 400,
        tooltip: { trigger: "axis" },
        legend: { top: 0, type: "scroll" },
        grid: { left: 48, right: 22, top: 36, bottom: 44 },
        xAxis: {
          type: "category",
          data: dates.map(formatYmd),
          axisLabel: { color: "#516050" },
        },
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
    const quantity = itemRows.map((r) => r.quantity);
    const price = itemRows.map((r) => r.avg_price);

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
            data: quantity,
          },
          {
            name: "平均価格",
            type: "line",
            yAxisIndex: 1,
            smooth: true,
            symbolSize: 6,
            itemStyle: { color: "#d06f3b" },
            data: price,
          },
        ],
      },
      true
    );
  }

  function renderHeatmap(periodRows) {
    const dates = Array.from(new Set(periodRows.map((r) => r.sale_date))).sort();
    const ranking = new Map();
    periodRows.forEach((r) => {
      if (!ranking.has(r.item_name)) {
        ranking.set(r.item_name, 0);
      }
      ranking.set(r.item_name, ranking.get(r.item_name) + (r.quantity || 0));
    });
    const maxItems = Number(config.heatmapItems) || 12;
    const items = Array.from(ranking.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, maxItems)
      .map((entry) => entry[0]);

    const xIndex = new Map(dates.map((d, i) => [d, i]));
    const yIndex = new Map(items.map((item, i) => [item, i]));
    const values = [];
    const matrix = [];

    periodRows.forEach((r) => {
      if (!xIndex.has(r.sale_date) || !yIndex.has(r.item_name) || r.avg_price == null) {
        return;
      }
      const value = r.avg_price;
      values.push(value);
      matrix.push([xIndex.get(r.sale_date), yIndex.get(r.item_name), value]);
    });

    const min = values.length ? Math.min(...values) : 0;
    const max = values.length ? Math.max(...values) : 1;

    heatmapChart.setOption(
      {
        animationDuration: 350,
        tooltip: {
          formatter: (params) => {
            const [dx, iy, v] = params.value;
            return `${items[iy]}<br/>${dates[dx]}: ${fmtPrice(v)}`;
          },
        },
        grid: { left: 98, right: 22, top: 20, bottom: 70 },
        xAxis: {
          type: "category",
          data: dates.map(formatYmd),
          splitArea: { show: true },
          axisLabel: { color: "#516050", rotate: 35 },
        },
        yAxis: {
          type: "category",
          data: items,
          splitArea: { show: true },
          axisLabel: { color: "#516050" },
        },
        visualMap: {
          min,
          max,
          calculable: true,
          orient: "horizontal",
          left: "center",
          bottom: 8,
          inRange: {
            color: ["#e6f3ed", "#99d0b6", "#3f9b70", "#2c5f48"],
          },
        },
        series: [
          {
            type: "heatmap",
            data: matrix,
            label: { show: false },
            emphasis: {
              itemStyle: {
                shadowBlur: 6,
                shadowColor: "rgba(0, 0, 0, 0.25)",
              },
            },
          },
        ],
      },
      true
    );
  }

  function renderAll() {
    const periodRows = filterRowsByPeriod(state.rows, state.periodDays);
    ensureSelectors(periodRows);
    renderKpiCards(periodRows);
    renderTrendChart(periodRows);
    renderComboChart(periodRows);
    renderHeatmap(periodRows);
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

    reloadBtn.addEventListener("click", () => {
      window.location.reload();
    });

    window.addEventListener("resize", () => {
      trendChart.resize();
      comboChart.resize();
      heatmapChart.resize();
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
      state.rows = await fetchAllRows(client);
      state.rows.sort((a, b) => a.sale_date.localeCompare(b.sale_date));
      state.seriesByItem = buildSeriesByItem(state.rows);
      if (!state.rows.length) {
        setStatus("表示できるデータがありません。");
        return;
      }
      renderAll();
    } catch (error) {
      setStatus(`読み込み失敗: ${error.message || String(error)}`);
    }
  }

  main();
})();
