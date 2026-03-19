const POLL_INTERVAL_MS = 2500;
const POLLING_STATUSES = new Set(["pending", "running", "waiting_input"]);
const PRIMARY_FLOW_NAMES = new Set([
  "codex_search_report_flow",
  "research_report_flow",
  "artifact_ingest_flow",
]);

const state = {
  activeTab: "research",
  researchMode: "search_report",
  ingestMode: "url",
  runs: [],
  selectedRunId: null,
  selectedRun: null,
  selectedRunSyncedId: null,
  activePollId: null,
  pollTimer: null,
  resumeRunId: null,
  loadingRun: false,
};

const elements = {};

document.addEventListener("DOMContentLoaded", () => {
  bindElements();
  if (!elements.researchFlash) {
    return;
  }
  setTodayDate();
  bindEvents();
  applyInitialQueryState();
  void initializePage();
});

function bindElements() {
  elements.researchFlash = document.getElementById("researchFlash");
  elements.researchTodayDate = document.getElementById("researchTodayDate");
  elements.researchRunCount = document.getElementById("researchRunCount");
  elements.researchTabButton = document.getElementById("researchTabButton");
  elements.ingestTabButton = document.getElementById("ingestTabButton");
  elements.researchTabPanel = document.getElementById("researchTabPanel");
  elements.ingestTabPanel = document.getElementById("ingestTabPanel");
  elements.researchModeSelect = document.getElementById("researchModeSelect");
  elements.searchFields = document.getElementById("searchFields");
  elements.reportFields = document.getElementById("reportFields");
  elements.searchPrompt = document.getElementById("searchPrompt");
  elements.searchLocalKnowledge = document.getElementById("searchLocalKnowledge");
  elements.searchBrowserWeb = document.getElementById("searchBrowserWeb");
  elements.submitResearchButton = document.getElementById("submitResearchButton");
  elements.reportTheme = document.getElementById("reportTheme");
  elements.reportBoundaries = document.getElementById("reportBoundaries");
  elements.reportAreas = document.getElementById("reportAreas");
  elements.ingestModeSelect = document.getElementById("ingestModeSelect");
  elements.ingestUrlFields = document.getElementById("ingestUrlFields");
  elements.ingestPasteFields = document.getElementById("ingestPasteFields");
  elements.ingestUrl = document.getElementById("ingestUrl");
  elements.ingestContent = document.getElementById("ingestContent");
  elements.ingestContentFormat = document.getElementById("ingestContentFormat");
  elements.ingestTitle = document.getElementById("ingestTitle");
  elements.ingestTags = document.getElementById("ingestTags");
  elements.submitIngestButton = document.getElementById("submitIngestButton");
  elements.runStatusValue = document.getElementById("runStatusValue");
  elements.runStatusNote = document.getElementById("runStatusNote");
  elements.runStdout = document.getElementById("runStdout");
  elements.runStderr = document.getElementById("runStderr");
  elements.resultActions = document.getElementById("resultActions");
  elements.recentRunsList = document.getElementById("recentRunsList");
}

function bindEvents() {
  elements.researchTabButton.addEventListener("click", () => setActiveTab("research"));
  elements.ingestTabButton.addEventListener("click", () => setActiveTab("ingest"));

  elements.researchModeSelect.addEventListener("change", () => {
    setResearchMode(elements.researchModeSelect.value);
  });
  elements.ingestModeSelect.addEventListener("change", () => {
    setIngestMode(elements.ingestModeSelect.value);
  });

  elements.submitResearchButton.addEventListener("click", () => {
    void handleResearchSubmit();
  });
  elements.submitIngestButton.addEventListener("click", () => {
    void handleIngestSubmit();
  });

  elements.recentRunsList.addEventListener("click", (event) => {
    const row = event.target.closest("[data-run-id]");
    if (!row) {
      return;
    }
    void loadRunById(row.dataset.runId, { focus: true });
  });

  elements.resultActions.addEventListener("click", (event) => {
    const button = event.target.closest("[data-action]");
    if (!button) {
      return;
    }
    const action = button.dataset.action;
    if (action === "continue-search") {
      void continueSearchFromSelectedRun();
    }
  });
}

function applyInitialQueryState() {
  const runId = getQueryParam("run_id");
  if (runId) {
    state.selectedRunId = runId;
    state.activePollId = runId;
  }
}

async function initializePage() {
  renderUI();
  await Promise.all([loadRecentRuns(), maybeHydrateSelectedRun()]);
}

function setActiveTab(tab) {
  state.activeTab = tab === "ingest" ? "ingest" : "research";
  renderUI();
}

function setResearchMode(mode) {
  state.researchMode = mode === "research_report" ? "research_report" : "search_report";
  renderUI();
}

function setIngestMode(mode) {
  state.ingestMode = mode === "paste" ? "paste" : "url";
  renderUI();
}

function renderUI() {
  if (!elements.researchFlash) {
    return;
  }

  const researchActive = state.activeTab === "research";
  elements.researchTabButton.classList.toggle("is-active", researchActive);
  elements.ingestTabButton.classList.toggle("is-active", !researchActive);
  elements.researchTabButton.setAttribute("aria-selected", String(researchActive));
  elements.ingestTabButton.setAttribute("aria-selected", String(!researchActive));
  elements.researchTabPanel.hidden = !researchActive;
  elements.ingestTabPanel.hidden = researchActive;

  const isSearchMode = state.researchMode === "search_report";
  elements.researchModeSelect.value = state.researchMode;
  elements.searchFields.hidden = !isSearchMode;
  elements.reportFields.hidden = isSearchMode;

  const isUrlMode = state.ingestMode === "url";
  elements.ingestModeSelect.value = state.ingestMode;
  elements.ingestUrlFields.hidden = !isUrlMode;
  elements.ingestPasteFields.hidden = isUrlMode;

  elements.submitResearchButton.textContent = state.researchMode === "search_report" ? "Run search report" : "Run research report";
  elements.submitIngestButton.textContent = state.ingestMode === "url" ? "Ingest URL" : "Ingest paste";

  if (state.selectedRun) {
    renderRunState(state.selectedRun);
  } else {
    renderEmptyRunState();
  }

  renderResultActions();
  renderRecentRuns();
}

function renderEmptyRunState() {
  elements.runStatusValue.textContent = "Idle";
  elements.runStatusNote.textContent = "Choose a research task or ingest source.";
  elements.runStdout.textContent = "";
  elements.runStderr.textContent = "";
  elements.resultActions.innerHTML = "";
}

function renderRunState(run) {
  const status = normalizeStatus(run.status);
  elements.runStatusValue.textContent = humanizeToken(status || "unknown");
  elements.runStatusNote.textContent = buildRunNote(run);
  elements.runStdout.textContent = String(run.stdout || "").trim();
  elements.runStderr.textContent = String(run.stderr || "").trim();
}

function renderResultActions() {
  const run = state.selectedRun;
  if (!run) {
    elements.resultActions.innerHTML = "";
    return;
  }

  const status = normalizeStatus(run.status);
  const flowName = String(run.flow_name || "");
  const outputs = asObject(run.structured_outputs);
  const actions = [];

  if (status === "completed" && flowName === "codex_search_report_flow") {
    const reportId = String(outputs?.report?.current_report_id || "").trim();
    if (reportId) {
      actions.push(linkButton(`/`, `Open report`, `report-link`, `?report_id=${encodeURIComponent(reportId)}`));
    }
    actions.push(buttonMarkup("Continue Search", "continue-search"));
  } else if (status === "completed" && flowName === "research_report_flow") {
    const reportId = String(outputs.current_report_id || outputs?.report?.current_report_id || "").trim();
    if (reportId) {
      actions.push(linkButton(`/`, `Open report`, `report-link`, `?report_id=${encodeURIComponent(reportId)}`));
    }
  } else if (status === "completed" && flowName === "artifact_ingest_flow") {
    const sourceId = firstCompletedSourceDocumentId(outputs);
    if (sourceId) {
      actions.push(linkButton(`/sources`, `Open source`, `source-link`, `?source_id=${encodeURIComponent(sourceId)}`));
    }
  }

  if (flowName === "research_report_flow" && (status === "waiting_input" || outputs?.checkpoint || outputs?.pending_report_id)) {
    const pendingId = String(outputs.pending_report_id || "").trim();
    const currentId = String(outputs.current_report_id || "").trim();
    const targetId = pendingId || currentId;
    if (targetId) {
      actions.push(linkButton(`/`, `Open pending report`, `pending-report-link`, `?report_id=${encodeURIComponent(targetId)}`));
    }
    actions.push(`<span class="result-note">Needs review before promotion.</span>`);
  }

  if (status !== "completed" && status !== "waiting_input" && flowName === "codex_search_report_flow") {
    const reportId = String(outputs?.report?.current_report_id || "").trim();
    if (reportId) {
      actions.push(linkButton(`/`, `Open report`, `report-link`, `?report_id=${encodeURIComponent(reportId)}`));
    }
  }

  if (!actions.length) {
    elements.resultActions.innerHTML = "";
    return;
  }

  elements.resultActions.innerHTML = actions.join("");
}

function renderRecentRuns() {
  if (!elements.recentRunsList) {
    return;
  }
  if (elements.researchRunCount) {
    elements.researchRunCount.textContent = `${state.runs.length} runs`;
  }

  if (!state.runs.length) {
    elements.recentRunsList.innerHTML = `<div class="recent-runs-empty">No recent runs yet.</div>`;
    return;
  }

  elements.recentRunsList.innerHTML = state.runs.map((run) => renderRecentRunRow(run)).join("");
}

function renderRecentRunRow(run) {
  const selected = run.id === state.selectedRunId ? " is-selected" : "";
  const status = humanizeToken(normalizeStatus(run.status) || "unknown");
  const flowLabel = humanizeFlowName(run.flow_name || "run");
  const summary = summarizeRun(run);
  return `
    <button class="recent-run-row${selected}" type="button" data-run-id="${escapeHTML(run.id)}">
      <div class="recent-run-main">
        <div class="recent-run-flow">${escapeHTML(flowLabel)}</div>
        <div class="recent-run-summary">${escapeHTML(summary)}</div>
      </div>
      <div class="recent-run-side">
        <span class="recent-run-status">${escapeHTML(status)}</span>
        <span class="recent-run-time">${escapeHTML(formatDateTime(run.updated_at || run.created_at))}</span>
      </div>
    </button>
  `;
}

async function handleResearchSubmit() {
  const payload = buildResearchPayload();
  if (!payload) {
    return;
  }

  try {
    showFlash("Submitting research run...");
    const response = await postJSON("/commands/" + (state.researchMode === "search_report" ? "search-report" : "research-report"), payload);
    await handleSubmittedRun(response);
  } catch (error) {
    showFlash(`Could not submit research: ${error.message || String(error)}`, true);
  }
}

async function handleIngestSubmit() {
  const payload = buildIngestPayload();
  if (!payload) {
    return;
  }

  try {
    showFlash("Submitting ingest run...");
    const response = await postJSON("/commands/artifact-ingest", payload);
    await handleSubmittedRun(response);
  } catch (error) {
    showFlash(`Could not submit ingest: ${error.message || String(error)}`, true);
  }
}

function buildResearchPayload() {
  if (state.researchMode === "search_report") {
    const prompt = String(elements.searchPrompt.value || "").trim();
    const enabledSources = [];
    if (elements.searchLocalKnowledge.checked) {
      enabledSources.push("local_knowledge");
    }
    if (elements.searchBrowserWeb.checked) {
      enabledSources.push("browser_web");
    }

    if (!prompt) {
      showFlash("Search prompt is required.", true);
      return null;
    }

    return {
      prompt,
      enabled_sources: enabledSources.length ? enabledSources : ["local_knowledge"],
      planner_enabled: true,
      max_results_per_query: 8,
      ...(state.resumeRunId ? { resume_from_run_id: state.resumeRunId } : {}),
    };
  }

  const theme = String(elements.reportTheme.value || "").trim();
  if (!theme) {
    showFlash("Theme is required.", true);
    return null;
  }

  return {
    theme,
    boundaries: parseLines(elements.reportBoundaries.value),
    areas_of_interest: parseLines(elements.reportAreas.value),
    edit_mode: "merge",
    human_policy: { mode: "auto" },
  };
}

function buildIngestPayload() {
  const tags = parseCommaList(elements.ingestTags.value);
  const mode = state.ingestMode;

  if (mode === "url") {
    const url = String(elements.ingestUrl.value || "").trim();
    if (!url) {
      showFlash("URL is required.", true);
      return null;
    }
    return {
      items: [
        {
          input_kind: "url",
          url,
          title: String(elements.ingestTitle.value || "").trim() || undefined,
          tags,
        },
      ],
    };
  }

  const content = String(elements.ingestContent.value || "").trim();
  if (!content) {
    showFlash("Pasted content is required.", true);
    return null;
  }

  return {
    items: [
      {
        input_kind: "inline",
        content,
        content_format: elements.ingestContentFormat.value || "markdown",
        title: String(elements.ingestTitle.value || "").trim() || undefined,
        tags,
      },
    ],
  };
}

async function handleSubmittedRun(response) {
  const runId = String(response.run_id || "").trim();
  if (!runId) {
    throw new Error("run_id missing from command response");
  }

  state.selectedRunId = runId;
  state.activePollId = runId;
  state.resumeRunId = null;
  updateUrlRunId(runId);
  await loadRunById(runId, { focus: true, syncForm: true });
  showFlash(`Run queued: ${runId}`);
  await loadRecentRuns();
}

async function maybeHydrateSelectedRun() {
  const runId = state.selectedRunId;
  if (!runId) {
    return;
  }
  await loadRunById(runId, { focus: false, syncForm: true });
}

async function loadRunById(runId, { focus = false, syncForm = false } = {}) {
  if (!runId) {
    return;
  }

  state.loadingRun = true;
  try {
    const payload = await fetchJSON(`/runs/${encodeURIComponent(runId)}`);
    const run = normalizeRun(payload);
    state.selectedRunId = run.id;
    state.selectedRun = run;
    if (syncForm || state.selectedRunSyncedId !== run.id) {
      syncFormFromRun(run);
      state.selectedRunSyncedId = run.id;
    }
    updateUrlRunId(run.id);
    renderUI();

    const status = normalizeStatus(run.status);
    if (POLLING_STATUSES.has(status)) {
      schedulePoll(run.id);
    } else {
      stopPolling();
      state.activePollId = run.id;
      if (status === "waiting_input" || shouldShowNeedsReview(run)) {
        showFlash("This research run needs review before it can be promoted.");
      }
    }

    if (focus) {
      elements.runStatusValue.focus?.();
    }
  } catch (error) {
    showFlash(`Could not load run ${runId}: ${error.message || String(error)}`, true);
  } finally {
    state.loadingRun = false;
  }
}

function syncFormFromRun(run) {
  const flowName = String(run.flow_name || "");
  const input = asObject(run.input_payload);
  const outputs = asObject(run.structured_outputs);

  if (flowName === "codex_search_report_flow") {
    setActiveTab("research");
    setResearchMode("search_report");
    const prompt = String(outputs?.handoff?.suggested_followup_prompt || input.prompt || "").trim();
    if (prompt) {
      elements.searchPrompt.value = prompt;
    }
    const enabledSources = Array.isArray(input.enabled_sources) ? input.enabled_sources.map(String) : [];
    elements.searchLocalKnowledge.checked = enabledSources.length ? enabledSources.includes("local_knowledge") : true;
    elements.searchBrowserWeb.checked = enabledSources.includes("browser_web");
    state.resumeRunId = normalizeStatus(run.status) === "completed" ? run.id : null;
    return;
  }

  if (flowName === "research_report_flow") {
    setActiveTab("research");
    setResearchMode("research_report");
    const theme = String(input.theme || "").trim();
    if (theme) {
      elements.reportTheme.value = theme;
    }
    elements.reportBoundaries.value = joinLines(input.boundaries);
    elements.reportAreas.value = joinLines(input.areas_of_interest);
    state.resumeRunId = null;
    return;
  }

  if (flowName === "artifact_ingest_flow") {
    setActiveTab("ingest");
    const items = Array.isArray(input.items) ? input.items : [];
    const firstItem = asObject(items[0]);
    if (firstItem.input_kind === "inline") {
      setIngestMode("paste");
      elements.ingestContent.value = String(firstItem.content || "");
      elements.ingestContentFormat.value = String(firstItem.content_format || "markdown");
    } else {
      setIngestMode("url");
      elements.ingestUrl.value = String(firstItem.url || "");
    }
    elements.ingestTitle.value = String(firstItem.title || "");
    elements.ingestTags.value = Array.isArray(firstItem.tags) ? firstItem.tags.join(", ") : "";
    state.resumeRunId = null;
  }
}

function schedulePoll(runId) {
  stopPolling();
  state.activePollId = runId;
  state.pollTimer = window.setTimeout(() => {
    void pollRun(runId);
  }, POLL_INTERVAL_MS);
}

function stopPolling() {
  if (state.pollTimer !== null) {
    window.clearTimeout(state.pollTimer);
    state.pollTimer = null;
  }
}

async function pollRun(runId) {
  if (state.activePollId !== runId) {
    return;
  }

  try {
    const payload = await fetchJSON(`/runs/${encodeURIComponent(runId)}`);
    const run = normalizeRun(payload);
    state.selectedRun = run;
    state.selectedRunId = run.id;
    if (state.selectedRunSyncedId !== run.id && normalizeStatus(run.status) !== "running") {
      syncFormFromRun(run);
      state.selectedRunSyncedId = run.id;
    }
    renderUI();

    const status = normalizeStatus(run.status);
    if (POLLING_STATUSES.has(status)) {
      schedulePoll(run.id);
    } else {
      stopPolling();
      if (status === "waiting_input" || shouldShowNeedsReview(run)) {
        showFlash("This research run needs review before it can be promoted.");
      }
      await loadRecentRuns();
    }
  } catch (error) {
    stopPolling();
    showFlash(`Polling failed for run ${runId}: ${error.message || String(error)}`, true);
  }
}

async function loadRecentRuns() {
  try {
    const payload = await fetchJSON("/runs?limit=25");
    const runs = Array.isArray(payload.runs) ? payload.runs : [];
    const filtered = runs
      .filter((run) => PRIMARY_FLOW_NAMES.has(String(run.flow_name || "")))
      .map(normalizeRun);

    if (state.selectedRun && !filtered.some((run) => run.id === state.selectedRun.id)) {
      filtered.unshift(state.selectedRun);
    }

    state.runs = dedupeRuns(filtered);
    renderRecentRuns();
  } catch (error) {
    showFlash(`Could not load recent runs: ${error.message || String(error)}`, true);
  }
}

async function continueSearchFromSelectedRun() {
  const run = state.selectedRun;
  if (!run || String(run.flow_name || "") !== "codex_search_report_flow" || normalizeStatus(run.status) !== "completed") {
    return;
  }

  const outputs = asObject(run.structured_outputs);
  const handoff = asObject(outputs.handoff);
  const suggested = String(handoff.suggested_followup_prompt || "").trim();
  const prompt = suggested || String(asObject(run.input_payload).prompt || "").trim();

  setActiveTab("research");
  setResearchMode("search_report");
  if (prompt) {
    elements.searchPrompt.value = prompt;
  }
  const enabledSources = Array.isArray(asObject(run.input_payload).enabled_sources)
    ? asObject(run.input_payload).enabled_sources.map(String)
    : [];
  elements.searchLocalKnowledge.checked = enabledSources.length ? enabledSources.includes("local_knowledge") : true;
  elements.searchBrowserWeb.checked = enabledSources.includes("browser_web");
  state.resumeRunId = run.id;
  renderUI();
  showFlash("Prepared follow-up search from the selected run.");
}

function normalizeRun(payload) {
  const outputSummary = asObject(payload.output_summary);
  return {
    id: String(payload.id || "").trim(),
    flow_name: String(payload.flow_name || "").trim(),
    status: String(payload.status || "").trim(),
    input_payload: asObject(payload.input_payload),
    structured_outputs: asObject(payload.structured_outputs || outputSummary),
    stdout: String(payload.stdout || ""),
    stderr: String(payload.stderr || ""),
    created_at: payload.created_at || "",
    updated_at: payload.updated_at || "",
  };
}

function normalizeStatus(value) {
  return String(value || "").trim().toLowerCase();
}

function shouldShowNeedsReview(run) {
  const outputs = asObject(run.structured_outputs);
  return String(run.flow_name || "") === "research_report_flow" && (Boolean(outputs.checkpoint) || Boolean(outputs.pending_report_id));
}

function buildRunNote(run) {
  const status = normalizeStatus(run.status);
  const flowName = String(run.flow_name || "");
  const outputs = asObject(run.structured_outputs);

  if (flowName === "codex_search_report_flow" && status === "completed") {
    return "Search report completed. You can continue from the latest prompt.";
  }
  if (flowName === "research_report_flow" && shouldShowNeedsReview(run)) {
    return "Needs review before the report becomes current.";
  }
  if (flowName === "artifact_ingest_flow" && status === "completed") {
    return `Ingest completed with ${countSuccessfulSources(outputs)} source document(s).`;
  }
  if (status === "waiting_input") {
    return "Waiting for review.";
  }
  if (status === "running") {
    return "Run is in progress.";
  }
  if (status === "pending") {
    return "Run has been queued.";
  }
  if (status === "failed") {
    return "Run failed. Inspect stderr for details.";
  }
  return "Choose a task or load a run.";
}

function countSuccessfulSources(outputs) {
  const items = Array.isArray(outputs.items) ? outputs.items : [];
  return items.reduce((count, item) => count + (String(asObject(item).status || "") === "completed" ? 1 : 0), 0);
}

function firstCompletedSourceDocumentId(outputs) {
  const items = Array.isArray(outputs.items) ? outputs.items : [];
  for (const item of items) {
    const output = asObject(item);
    if (String(output.status || "") === "completed" && output.source_document_id) {
      return String(output.source_document_id).trim();
    }
  }
  const nestedIds = asObject(outputs.ingest_handoff).source_document_ids;
  if (Array.isArray(nestedIds)) {
    for (const item of nestedIds) {
      const text = String(item || "").trim();
      if (text) {
        return text;
      }
    }
  }
  return "";
}

function updateUrlRunId(runId) {
  if (!runId) {
    return;
  }
  const url = new URL(window.location.href);
  url.searchParams.set("run_id", runId);
  window.history.replaceState({}, "", url);
}

function parseLines(value) {
  return String(value || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
}

function joinLines(values) {
  if (!Array.isArray(values)) {
    return "";
  }
  return values.map((value) => String(value || "").trim()).filter(Boolean).join("\n");
}

function parseCommaList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function dedupeRuns(runs) {
  const seen = new Set();
  const result = [];
  for (const run of runs) {
    if (!run.id || seen.has(run.id)) {
      continue;
    }
    seen.add(run.id);
    result.push(run);
  }
  return result;
}

function humanizeFlowName(value) {
  const text = String(value || "")
    .replace(/_flow$/, "")
    .replace(/_/g, " ")
    .trim();
  return text ? text.replace(/\b\w/g, (char) => char.toUpperCase()) : "Run";
}

function summarizeRun(run) {
  const outputs = asObject(run.structured_outputs);
  const status = normalizeStatus(run.status);
  if (String(run.flow_name || "") === "codex_search_report_flow") {
    return String(outputs?.report?.title || outputs?.handoff?.resume_summary || outputs?.report?.summary || status || "Search report");
  }
  if (String(run.flow_name || "") === "research_report_flow") {
    return String(outputs.current_report_id ? `Report ${outputs.current_report_id}` : outputs?.research_run?.stdout || status || "Research report");
  }
  if (String(run.flow_name || "") === "artifact_ingest_flow") {
    return String(outputs.success_count !== undefined ? `${outputs.success_count} of ${outputs.input_count || 0} ingested` : status || "Ingest");
  }
  return status || "Run";
}

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function showFlash(message, isError = false) {
  elements.researchFlash.hidden = false;
  elements.researchFlash.textContent = message;
  elements.researchFlash.dataset.state = isError ? "error" : "info";
}

async function fetchJSON(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
    },
    ...options,
  });
  if (!response.ok) {
    throw new Error(await readError(response));
  }
  return response.json();
}

async function postJSON(path, body) {
  return fetchJSON(path, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

function getQueryParam(name) {
  const value = new URL(window.location.href).searchParams.get(name);
  return value ? value.trim() : null;
}

function setTodayDate() {
  if (!elements.researchTodayDate) {
    return;
  }
  elements.researchTodayDate.textContent = new Date().toLocaleDateString("en-CA", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

async function readError(response) {
  try {
    const payload = await response.json();
    return payload.detail || JSON.stringify(payload);
  } catch (_error) {
    return `${response.status} ${response.statusText}`;
  }
}

function linkButton(href, label, className, queryString = "") {
  return `<a class="inline-button ${className}" href="${escapeHTML(href + queryString)}" target="_self">${escapeHTML(label)}</a>`;
}

function buttonMarkup(label, action) {
  return `<button class="inline-button inline-button-primary" type="button" data-action="${escapeHTML(action)}">${escapeHTML(label)}</button>`;
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

function formatDateTime(value) {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleString("en-CA", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
