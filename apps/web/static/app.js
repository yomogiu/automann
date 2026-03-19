const state = {
  reports: [],
  filters: [],
  tagLabels: {},
  selectedId: null,
  activeFilter: "all",
  activeQuery: "",
  loading: false,
};

const elements = {};

document.addEventListener("DOMContentLoaded", () => {
  bindElements();
  bindEvents();
  setTodayDate();
  void loadArchive(getQueryParam("report_id"));
});

function bindElements() {
  elements.todayDate = document.getElementById("todayDate");
  elements.totalCount = document.getElementById("totalCount");
  elements.searchInput = document.getElementById("searchInput");
  elements.searchButton = document.getElementById("searchButton");
  elements.clearButton = document.getElementById("clearButton");
  elements.filterBar = document.getElementById("filterBar");
  elements.statusText = document.getElementById("statusText");
  elements.resultCount = document.getElementById("resultCount");
  elements.resultArea = document.getElementById("resultArea");
  elements.flashMessage = document.getElementById("flashMessage");
}

function bindEvents() {
  elements.searchButton.addEventListener("click", runSearch);
  elements.clearButton.addEventListener("click", clearSearch);
  elements.searchInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      runSearch();
    }
  });

  elements.filterBar.addEventListener("click", (event) => {
    const pill = event.target.closest(".pill");
    if (!pill) {
      return;
    }
    state.activeFilter = pill.dataset.filter || "all";
    syncPills();
    renderReports();
  });

  elements.resultArea.addEventListener("click", (event) => {
    const row = event.target.closest("[data-report-id]");
    if (!row) {
      return;
    }
    const reportId = row.dataset.reportId;
    const report = findReport(reportId);
    if (report && openReportArtifact(report)) {
      return;
    }
    state.selectedId = state.selectedId === reportId ? null : reportId;
    renderReports();
  });
}

function setTodayDate() {
  elements.todayDate.textContent = new Date().toLocaleDateString("en-CA", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

async function loadArchive(targetReportId = null) {
  state.loading = true;
  renderLoadingState();
  try {
    const [reportsResponse, taxonomyResponse] = await Promise.all([
      fetchJSON("/reports?limit=100"),
      fetchJSON("/reports/taxonomy"),
    ]);

    state.reports = reportsResponse.reports || [];
    state.filters = taxonomyResponse.filters || [];
    state.tagLabels = Object.fromEntries((taxonomyResponse.tags || []).map((tag) => [tag.key, tag.label]));
    state.activeFilter = availableFilterKeys().includes(state.activeFilter) ? state.activeFilter : "all";
    state.selectedId = state.reports.some((report) => report.id === targetReportId) ? targetReportId : null;

    if (targetReportId && state.selectedId !== targetReportId) {
      const report = await loadArchivedReport(targetReportId);
      if (report) {
        state.selectedId = report.id;
      }
    }

    renderFilterBar();
    renderReports();
  } catch (error) {
    elements.totalCount.textContent = "— docs";
    elements.statusText.textContent = "Could not load archive";
    elements.resultCount.textContent = "";
    elements.resultArea.innerHTML = `<div class="error-msg">Could not load taxonomy-backed reports right now. ${escapeHTML(error.message || String(error))}</div>`;
  } finally {
    state.loading = false;
  }
}

async function loadArchivedReport(reportId) {
  try {
    const payload = await fetchJSON(`/reports/${encodeURIComponent(reportId)}`);
    const report = payload && typeof payload === "object" ? payload : null;
    if (!report) {
      return null;
    }
    state.reports = [report, ...state.reports.filter((item) => item.id !== report.id)];
    return report;
  } catch (error) {
    showFlash(`Could not load report ${reportId}: ${error.message || String(error)}`, true);
    return null;
  }
}

function renderFilterBar() {
  const pills = [
    '<button class="pill" type="button" data-filter="all">All</button>',
    ...state.filters.map(
      (filter) => `<button class="pill" type="button" data-filter="${escapeHTML(filter.key)}">${escapeHTML(filter.label)}</button>`,
    ),
  ].join("");
  elements.filterBar.innerHTML = `<span class="filter-label">Filter:</span>${pills}`;
  syncPills();
}

function availableFilterKeys() {
  return ["all", ...state.filters.map((filter) => filter.key)];
}

function syncPills() {
  for (const pill of elements.filterBar.querySelectorAll(".pill")) {
    pill.classList.toggle("active", pill.dataset.filter === state.activeFilter);
  }
}

function runSearch() {
  state.activeQuery = elements.searchInput.value.trim().toLowerCase();
  renderReports();
}

function clearSearch() {
  elements.searchInput.value = "";
  state.activeQuery = "";
  state.activeFilter = "all";
  state.selectedId = null;
  hideFlash();
  syncPills();
  renderReports();
}

function filteredReports() {
  return state.reports.filter((report) => {
    const matchesFilter = state.activeFilter === "all" || (report.filter_keys || []).includes(state.activeFilter);
    if (!matchesFilter) {
      return false;
    }
    if (!state.activeQuery) {
      return true;
    }
    const searchable = [
      report.title,
      report.summary,
      report.content_markdown,
      report.report_type,
      ...(report.filter_keys || []),
      ...(report.tag_keys || []),
    ].join(" ").toLowerCase();
    return searchable.includes(state.activeQuery);
  });
}

function renderReports() {
  const reports = filteredReports();
  elements.totalCount.textContent = `${state.reports.length} docs`;
  elements.statusText.textContent = buildStatusText(reports.length, Boolean(state.activeQuery));
  elements.resultCount.textContent = reports.length ? `${reports.length} ${reports.length === 1 ? "document" : "documents"}` : "";

  if (state.selectedId && !reports.some((report) => report.id === state.selectedId)) {
    state.selectedId = null;
  }

  if (!reports.length) {
    elements.resultArea.innerHTML = emptyStateMarkup(
      "Nothing on the shelf",
      state.activeQuery
        ? "No reports match this search. Try another phrase or switch taxonomy filters."
        : "No reports have been written to the archive yet.",
    );
    return;
  }

  elements.resultArea.innerHTML = `<div class="doc-list">${reports.map(renderReportRow).join("")}</div>`;
}

function buildStatusText(count, hasQuery) {
  if (hasQuery) {
    return `Results for “${elements.searchInput.value.trim()}”`;
  }
  if (!count || state.activeFilter === "all") {
    return "Showing all documents";
  }
  const filter = state.filters.find((item) => item.key === state.activeFilter);
  return `Filtered by ${filter ? filter.label : humanizeToken(state.activeFilter)}`;
}

function renderReportRow(report) {
  const selected = report.id === state.selectedId ? " is-selected" : "";
  const detail = report.id === state.selectedId ? renderReportDetail(report) : "";
  return `
    <div class="doc-row-wrap">
      <button class="doc-row${selected}" type="button" data-report-id="${escapeHTML(report.id)}">
        <div class="doc-main">
          <div class="doc-source">${escapeHTML(primaryFilterLabel(report))}</div>
          <div class="doc-title">${escapeHTML(report.title || "Untitled report")}</div>
          <div class="doc-desc">${escapeHTML(report.summary || snippet(report.content_markdown) || "No summary stored.")}</div>
          <div class="doc-tags">${renderTagBadges(report)}</div>
        </div>
        <div class="doc-right">
          <div class="doc-date">${escapeHTML(formatDate(report.updated_at || report.created_at))}</div>
        </div>
      </button>
      ${detail}
    </div>
  `;
}

function primaryFilterLabel(report) {
  const firstFilter = (report.filter_keys || [])[0];
  const definition = state.filters.find((item) => item.key === firstFilter);
  return definition ? definition.label : "Report";
}

function renderTagBadges(report) {
  const labels = [];
  for (const filterKey of report.filter_keys || []) {
    const filter = state.filters.find((item) => item.key === filterKey);
    if (filter) {
      labels.push(filter.label);
    }
  }
  for (const tagKey of report.tag_keys || []) {
    labels.push(state.tagLabels[tagKey] || humanizeToken(tagKey));
  }
  return labels.map((label) => `<span class="tag">${escapeHTML(label)}</span>`).join("");
}

function renderReportDetail(report) {
  const artifactAction = report.source_artifact_id
    ? `<a class="inline-button inline-button-primary" href="${escapeHTML(reportDownloadUrl(report))}" target="_blank" rel="noreferrer">Open report in new tab</a>`
    : "";
  const metadata = report.metadata && typeof report.metadata === "object" ? report.metadata : {};
  const presentationMode = String(metadata.presentation_mode || "raw_review");
  const annotatedHtmlArtifactId = metadata.annotated_html_artifact_id || metadata.html_artifact_id || null;

  const analysisBlock = presentationMode === "annotated_paper"
    ? renderAnnotatedPaperBlock(annotatedHtmlArtifactId)
    : `
        <div class="preview-block">
          <div class="preview-title">Markdown preview</div>
          <pre class="copy-block">${escapeHTML(report.content_markdown || "No markdown body stored.")}</pre>
        </div>
      `;

  return `
    <div class="doc-detail">
      <div class="doc-detail-inner">
        <div class="detail-kicker">Taxonomy</div>
        <div class="detail-title">${escapeHTML(report.title || "Untitled report")}</div>
        <div class="detail-text">${escapeHTML(report.summary || "No summary stored for this report.")}</div>
        <div class="detail-meta">
          ${metaItem("Report type", humanizeToken(report.report_type))}
          ${metaItem("Filters", (report.filter_keys || []).map((key) => {
            const definition = state.filters.find((item) => item.key === key);
            return definition ? definition.label : humanizeToken(key);
          }).join(", ") || "—")}
          ${metaItem("Tags", (report.tag_keys || []).map((key) => state.tagLabels[key] || humanizeToken(key)).join(", ") || "—")}
          ${metaItem("Updated", formatDateTime(report.updated_at || report.created_at))}
        </div>
        ${artifactAction ? `<div class="detail-actions">${artifactAction}</div>` : ""}
        ${analysisBlock}
      </div>
    </div>
  `;
}

function findReport(reportId) {
  return state.reports.find((report) => report.id === reportId) || null;
}

function reportDownloadUrl(report) {
  return `/artifacts/${encodeURIComponent(report.source_artifact_id)}/download`;
}

function openReportArtifact(report) {
  if (!report || !report.source_artifact_id) {
    return false;
  }
  window.open(reportDownloadUrl(report), "_blank", "noopener,noreferrer");
  return true;
}

function renderAnnotatedPaperBlock(annotatedHtmlArtifactId) {
  if (!annotatedHtmlArtifactId) {
    return `
      <div class="analysis-label">AI Analysis</div>
      <div class="preview-block">
        <div class="detail-note">Annotated paper mode is set, but no annotated HTML artifact is available yet.</div>
      </div>
    `;
  }
  return `
    <div class="analysis-label">AI Analysis</div>
    <div class="preview-block">
      <div class="preview-title">Annotated paper presentation</div>
      <iframe
        class="analysis-frame"
        src="/artifacts/${escapeHTML(annotatedHtmlArtifactId)}/download"
        title="Annotated paper analysis"
        loading="lazy"
      ></iframe>
    </div>
  `;
}

function renderLoadingState() {
  elements.totalCount.textContent = "— docs";
  elements.statusText.textContent = "Loading documents";
  elements.resultCount.textContent = "";
  elements.resultArea.innerHTML = `<div class="loading"><div class="spinner"></div>Brewing taxonomy-backed report results&hellip;</div>`;
}

function metaItem(label, value) {
  return `
    <div class="meta-item">
      <span class="meta-label">${escapeHTML(label)}</span>
      <span class="meta-value">${escapeHTML(String(value ?? "—"))}</span>
    </div>
  `;
}

function emptyStateMarkup(title, body) {
  return `
    <div class="empty-state">
      <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1" aria-hidden="true"><path d="M12 6.253v13"></path><path d="M12 6.253C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253"></path><path d="M12 6.253C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253"></path></svg>
      <div class="empty-title">${escapeHTML(title)}</div>
      <div class="empty-sub">${escapeHTML(body)}</div>
    </div>
  `;
}

function hideFlash() {
  elements.flashMessage.hidden = true;
  elements.flashMessage.textContent = "";
  elements.flashMessage.classList.remove("is-error");
}

function showFlash(message, isError = false) {
  elements.flashMessage.hidden = false;
  elements.flashMessage.textContent = message;
  elements.flashMessage.classList.toggle("is-error", Boolean(isError));
}

function getQueryParam(name) {
  const value = new URL(window.location.href).searchParams.get(name);
  return value ? value.trim() : null;
}

async function fetchJSON(path) {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    throw new Error(await readError(response));
  }
  return response.json();
}

async function readError(response) {
  try {
    const payload = await response.json();
    return payload.detail || JSON.stringify(payload);
  } catch (_error) {
    return `${response.status} ${response.statusText}`;
  }
}

function formatDate(value) {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleDateString("en-CA", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function formatDateTime(value) {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("en-CA", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function snippet(value, limit = 180) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!text) {
    return "";
  }
  return text.length > limit ? `${text.slice(0, limit - 1)}…` : text;
}

function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function humanizeToken(value) {
  const text = String(value || "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!text) {
    return "Unknown";
  }
  return text.replace(/\b\w/g, (char) => char.toUpperCase());
}
