(function () {
  const config = window.DASHBOARD_CONFIG || {};
  const statusEl = document.getElementById("statusMessage");
  const dailyMetaEl = document.getElementById("dailyMeta");
  const monthlyMetaEl = document.getElementById("monthlyMeta");
  const errorMetaEl = document.getElementById("errorMeta");
  const errorLatestBodyEl = document.getElementById("errorLatestBody");
  const periodButtons = Array.from(document.querySelectorAll(".period-btn"));
  const reloadBtn = document.getElementById("reloadBtn");

  const dailyTrendChart = echarts.init(document.getElementById("dailyTrendChart"));
  const monthlyTrendChart = echarts.init(document.getElementById("monthlyTrendChart"));
  const errorDailyChart = echarts.init(document.getElementById("errorDailyChart"));

  const VISITOR_ID_KEY = "agri_dashboard_visitor_id";

  const state = {
    periodDays: 30,
    dailyMetrics: [],
    monthlyMetrics: [],
    dailyUserPv: [],
    monthlyUserPv: [],
    errorLatest: [],
    client: null,
  };

  function setStatus(message) {
    statusEl.textContent = message;
  }

  function clearChildren(el) {
    while (el.firstChild) {
      el.removeChild(el.firstChild);
    }
  }

  function appendTableCell(rowEl, text, className) {
    const td = document.createElement("td");
    td.textContent = text;
    if (className) {
      td.className = className;
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

  function fmtYmd(dateStr) {
    return new Date(`${dateStr}T00:00:00+09:00`).toLocaleDateString("ja-JP", {
      month: "2-digit",
      day: "2-digit",
    });
  }

  function fmtYm(dateStr) {
    return new Date(`${dateStr}T00:00:00+09:00`).toLocaleDateString("ja-JP", {
      year: "numeric",
      month: "2-digit",
    });
  }

  function fmtDateTime(timestamp) {
    if (!timestamp) {
      return "-";
    }
    return new Date(timestamp).toLocaleString("ja-JP", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  function toDate(dateStr) {
    return new Date(`${dateStr}T00:00:00+09:00`);
  }

  function getLatestDate(rows, key) {
    if (!rows.length) {
      return null;
    }
    return rows[rows.length - 1][key];
  }

  function filterRecentDays(rows, key, periodDays) {
    if (!rows.length) {
      return [];
    }
    const latest = toDate(getLatestDate(rows, key));
    const cutoff = new Date(latest);
    cutoff.setDate(cutoff.getDate() - Math.max(0, periodDays - 1));
    return rows.filter((row) => toDate(row[key]) >= cutoff);
  }

  function filterRecentMonths(rows, key, monthCount) {
    if (!rows.length) {
      return [];
    }
    const latest = toDate(getLatestDate(rows, key));
    const cutoff = new Date(latest.getFullYear(), latest.getMonth() - Math.max(0, monthCount - 1), 1);
    return rows.filter((row) => toDate(row[key]) >= cutoff);
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

  async function logUsageEvent(payload) {
    if (!state.client) {
      return;
    }
    try {
      await state.client.from("usage_events").insert(payload);
    } catch (_) {
      // No-op: never block page on logging errors.
    }
  }

  function setupErrorLogging() {
    window.addEventListener("error", (event) => {
      void logUsageEvent({
        visitor_id: getVisitorId(),
        event_type: "error",
        page_path: window.location.pathname || "/",
        error_code: "usage_admin_window_error",
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
      void logUsageEvent({
        visitor_id: getVisitorId(),
        event_type: "error",
        page_path: window.location.pathname || "/",
        error_code: "usage_admin_unhandled_rejection",
        message_summary: sanitizeMessage(reason),
        metadata: {},
      });
    });
  }

  async function fetchAllFromView(viewName, columns, orderByColumn) {
    const pageSize = 1000;
    let from = 0;
    const rows = [];
    while (true) {
      const { data, error } = await state.client
        .from(viewName)
        .select(columns)
        .order(orderByColumn, { ascending: true })
        .range(from, from + pageSize - 1);
      if (error) {
        throw error;
      }
      if (!data || data.length === 0) {
        break;
      }
      rows.push(...data);
      if (data.length < pageSize) {
        break;
      }
      from += pageSize;
    }
    return rows;
  }

  function pickTopVisitors(rows, dateKey) {
    const totals = new Map();
    rows.forEach((row) => {
      const key = row.visitor_id;
      totals.set(key, (totals.get(key) || 0) + Number(row.pv || 0));
    });
    return Array.from(totals.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([visitorId], idx) => ({
        visitorId,
        label: `user_${idx + 1}`,
        points: rows
          .filter((row) => row.visitor_id === visitorId)
          .sort((a, b) => String(a[dateKey]).localeCompare(String(b[dateKey]))),
      }));
  }

  function renderDailyTrend() {
    const metrics = filterRecentDays(state.dailyMetrics, "day_jst", state.periodDays);
    const userRows = filterRecentDays(state.dailyUserPv, "day_jst", state.periodDays);
    const topVisitors = pickTopVisitors(userRows, "day_jst");
    const dates = metrics.map((r) => r.day_jst);

    const userSeries = topVisitors.map((u) => {
      const map = new Map(u.points.map((p) => [p.day_jst, Number(p.pv || 0)]));
      return {
        name: `${u.label} PV`,
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 1.5, type: "dashed" },
        data: dates.map((d) => map.get(d) || 0),
      };
    });

    dailyTrendChart.setOption(
      {
        animationDuration: 300,
        tooltip: { trigger: "axis" },
        legend: { top: 0, type: "scroll" },
        grid: { left: 48, right: 24, top: 40, bottom: 44 },
        xAxis: { type: "category", data: dates.map(fmtYmd) },
        yAxis: { type: "value", splitLine: { lineStyle: { color: "#e7eee4" } } },
        series: [
          {
            name: "日次UU",
            type: "line",
            smooth: true,
            showSymbol: false,
            lineStyle: { width: 2.5 },
            data: metrics.map((r) => Number(r.uu || 0)),
          },
          {
            name: "日次PV",
            type: "line",
            smooth: true,
            showSymbol: false,
            lineStyle: { width: 2.5 },
            data: metrics.map((r) => Number(r.pv || 0)),
          },
          ...userSeries,
        ],
      },
      true
    );

    const topText = topVisitors.map((u) => u.label).join(", ") || "-";
    dailyMetaEl.textContent = `表示期間: 直近${state.periodDays}日 / 上位ユーザー: ${topText}`;
  }

  function renderMonthlyTrend() {
    const metrics = filterRecentMonths(state.monthlyMetrics, "month_jst", 12);
    const monthSet = new Set(metrics.map((r) => r.month_jst));
    const userRows = state.monthlyUserPv.filter((r) => monthSet.has(r.month_jst));
    const topVisitors = pickTopVisitors(userRows, "month_jst");
    const months = metrics.map((r) => r.month_jst);

    const userSeries = topVisitors.map((u) => {
      const map = new Map(u.points.map((p) => [p.month_jst, Number(p.pv || 0)]));
      return {
        name: `${u.label} 月次PV`,
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 1.5, type: "dashed" },
        data: months.map((m) => map.get(m) || 0),
      };
    });

    monthlyTrendChart.setOption(
      {
        animationDuration: 300,
        tooltip: { trigger: "axis" },
        legend: { top: 0, type: "scroll" },
        grid: { left: 48, right: 24, top: 40, bottom: 44 },
        xAxis: { type: "category", data: months.map(fmtYm) },
        yAxis: { type: "value", splitLine: { lineStyle: { color: "#e7eee4" } } },
        series: [
          {
            name: "月次UU",
            type: "bar",
            data: metrics.map((r) => Number(r.uu || 0)),
            itemStyle: { color: "#6fa8dc" },
          },
          {
            name: "月次PV",
            type: "bar",
            data: metrics.map((r) => Number(r.pv || 0)),
            itemStyle: { color: "#7ec58d" },
          },
          ...userSeries,
        ],
      },
      true
    );

    const topText = topVisitors.map((u) => u.label).join(", ") || "-";
    monthlyMetaEl.textContent = `表示期間: 直近12か月 / 上位ユーザー: ${topText}`;
  }

  function renderErrorSection() {
    const metrics = filterRecentDays(state.dailyMetrics, "day_jst", state.periodDays);
    const dates = metrics.map((r) => r.day_jst);
    const values = metrics.map((r) => Number(r.error_count || 0));

    errorDailyChart.setOption(
      {
        animationDuration: 300,
        tooltip: { trigger: "axis" },
        grid: { left: 48, right: 24, top: 20, bottom: 44 },
        xAxis: { type: "category", data: dates.map(fmtYmd) },
        yAxis: { type: "value", splitLine: { lineStyle: { color: "#e7eee4" } } },
        series: [
          {
            name: "error件数",
            type: "line",
            smooth: true,
            showSymbol: false,
            lineStyle: { width: 2.5, color: "#ce4d41" },
            itemStyle: { color: "#ce4d41" },
            data: values,
          },
        ],
      },
      true
    );

    errorMetaEl.textContent = `表示期間: 直近${state.periodDays}日`;

    if (!state.errorLatest.length) {
      renderEmptyTableRow(errorLatestBodyEl, 5, "直近7日でエラーはありません。");
      return;
    }
    clearChildren(errorLatestBodyEl);
    state.errorLatest.slice(0, 20).forEach((row, idx) => {
      const tr = document.createElement("tr");
      appendTableCell(tr, String(idx + 1), "rank");
      appendTableCell(tr, String(row.error_code || ""));
      appendTableCell(tr, String(row.message_summary || ""));
      appendTableCell(tr, String(Number(row.count_7d || 0)));
      appendTableCell(tr, fmtDateTime(row.last_seen_at));
      errorLatestBodyEl.appendChild(tr);
    });
  }

  function renderAll() {
    renderDailyTrend();
    renderMonthlyTrend();
    renderErrorSection();
    setStatus("利用ログを表示中");
  }

  function attachEvents() {
    periodButtons.forEach((btn) => {
      btn.classList.toggle("is-active", Number(btn.dataset.days) === state.periodDays);
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

    reloadBtn.addEventListener("click", () => {
      window.location.reload();
    });

    window.addEventListener("resize", () => {
      dailyTrendChart.resize();
      monthlyTrendChart.resize();
      errorDailyChart.resize();
    });
  }

  async function loadData() {
    const [dailyMetrics, monthlyMetrics, dailyUserPv, monthlyUserPv, errorLatest] = await Promise.all([
      fetchAllFromView("usage_daily_metrics_jst", "day_jst,pv,uu,error_count", "day_jst"),
      fetchAllFromView("usage_monthly_metrics_jst", "month_jst,pv,uu,error_count", "month_jst"),
      fetchAllFromView("usage_daily_user_pv_jst", "day_jst,visitor_id,pv", "day_jst"),
      fetchAllFromView("usage_monthly_user_pv_jst", "month_jst,visitor_id,pv", "month_jst"),
      fetchAllFromView("usage_error_latest_7d_jst", "error_code,message_summary,count_7d,last_seen_at", "last_seen_at"),
    ]);
    state.dailyMetrics = dailyMetrics.sort((a, b) => String(a.day_jst).localeCompare(String(b.day_jst)));
    state.monthlyMetrics = monthlyMetrics.sort((a, b) => String(a.month_jst).localeCompare(String(b.month_jst)));
    state.dailyUserPv = dailyUserPv.sort((a, b) => String(a.day_jst).localeCompare(String(b.day_jst)));
    state.monthlyUserPv = monthlyUserPv.sort((a, b) => String(a.month_jst).localeCompare(String(b.month_jst)));
    state.errorLatest = errorLatest.sort((a, b) => Number(b.count_7d || 0) - Number(a.count_7d || 0));
  }

  async function main() {
    if (!config.supabaseUrl || !config.supabaseAnonKey || config.supabaseUrl.includes("YOUR-PROJECT-REF")) {
      setStatus("config.js の Supabase 設定を入力してください。");
      return;
    }

    attachEvents();
    setStatus("利用ログを読み込み中...");

    try {
      state.client = window.supabase.createClient(config.supabaseUrl, config.supabaseAnonKey);
      setupErrorLogging();
      void logUsageEvent({
        visitor_id: getVisitorId(),
        event_type: "page_view",
        page_path: window.location.pathname || "/",
        metadata: {
          referrer: document.referrer || "",
        },
      });
      await loadData();
      renderAll();
    } catch (error) {
      void logUsageEvent({
        visitor_id: getVisitorId(),
        event_type: "error",
        page_path: window.location.pathname || "/",
        error_code: "usage_admin_load_failed",
        message_summary: sanitizeMessage(error.message || String(error)),
        metadata: {},
      });
      setStatus(`読み込み失敗: ${error.message || String(error)}`);
    }
  }

  main();
})();
