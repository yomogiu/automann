const DEFAULT_SEARCH_PROMPT = [
  "# Search report instructions",
  "",
  "Use the structured query config and decide whether to search local knowledge, browser-backed web sources, or both.",
  "",
  "Requirements:",
  "- Prioritize concrete evidence over generic synthesis.",
  "- Group findings by theme.",
  "- Preserve useful citations and source links.",
  "- Call out uncertainty or conflicting signals explicitly.",
].join("\n");

const templateCatalog = [
  {
    id: "daily_brief",
    label: "Daily Brief",
    headline: "Daily research opening bell",
    description: "Schedule a predictable morning brief with defined ingestion lanes and a lightweight publication path.",
    pills: ["daily", "brief", "scheduled"],
    title: "Morning brief",
    scheduleText: "0 7 * * 1-5",
    workPool: "mini-process",
  },
  {
    id: "paper_batch",
    label: "Paper Batch",
    headline: "Queued paper review set",
    description: "Review a bundle of papers together, then keep the comparison memo separate from the archive shelf.",
    pills: ["papers", "batch", "analysis"],
    title: "Weekly paper queue",
    scheduleText: "30 13 * * 3",
    workPool: "mini-process",
  },
  {
    id: "search_report",
    label: "Search Report",
    headline: "Prompt-shaped search sweep",
    description: "Refine a repo-backed instruction file and use it to steer local knowledge recall and browser-backed search.",
    pills: ["search", "prompt", "prefect"],
    title: "Search sweep",
    scheduleText: "15 9 * * 1-5",
    workPool: "mini-process",
  },
];

const state = {
  selectedTemplateId: templateCatalog[0].id,
  automations: [],
  editor: null,
  editorMode: "draft",
  recentRuns: [],
  prefectLink: "http://127.0.0.1:4200",
};

const elements = {};

document.addEventListener("DOMContentLoaded", () => {
  bindElements();
  bindEvents();
  setTodayDate();
  renderTemplates();
  renderAutomations();
  renderDetail();
  renderRecentRuns();
  void initializePage();
});

function bindElements() {
  elements.templateGrid = document.getElementById("templateGrid");
  elements.automationList = document.getElementById("automationList");
  elements.automationCount = document.getElementById("automationCount");
  elements.automationTodayDate = document.getElementById("automationTodayDate");
  elements.prefectState = document.getElementById("prefectState");
  elements.prefectNote = document.getElementById("prefectNote");
  elements.prefectLink = document.getElementById("prefectLink");
  elements.recentRunCount = document.getElementById("recentRunCount");
  elements.recentRunNote = document.getElementById("recentRunNote");
  elements.listMeta = document.getElementById("listMeta");
  elements.detailTitle = document.getElementById("detailTitle");
  elements.detailSubtitle = document.getElementById("detailSubtitle");
  elements.scheduleTitle = document.getElementById("scheduleTitle");
  elements.scheduleSummary = document.getElementById("scheduleSummary");
  elements.detailMeta = document.getElementById("detailMeta");
  elements.configFields = document.getElementById("configFields");
  elements.promptPath = document.getElementById("promptPath");
  elements.promptPreview = document.getElementById("promptPreview");
  elements.jsonPreview = document.getElementById("jsonPreview");
  elements.recentRuns = document.getElementById("recentRuns");
  elements.pageFlash = document.getElementById("pageFlash");
  elements.newAutomationButton = document.getElementById("newAutomationButton");
  elements.saveAutomationButton = document.getElementById("saveAutomationButton");
  elements.runNowButton = document.getElementById("runNowButton");
  elements.pauseButton = document.getElementById("pauseButton");
  elements.archiveButton = document.getElementById("archiveButton");
  elements.applyJsonButton = document.getElementById("applyJsonButton");
  elements.openDeploymentButton = document.getElementById("openDeploymentButton");
  elements.openRunButton = document.getElementById("openRunButton");
  elements.showBoundaryButton = document.getElementById("showBoundaryButton");
}

function bindEvents() {
  elements.templateGrid.addEventListener("click", (event) => {
    const card = event.target.closest("[data-template-id]");
    if (!card) {
      return;
    }
    state.selectedTemplateId = card.dataset.templateId;
    renderTemplates();
  });

  elements.newAutomationButton.addEventListener("click", () => {
    createDraftFromTemplate(state.selectedTemplateId);
    showFlash(`Started a new ${typeLabel(state.selectedTemplateId)} draft.`);
  });

  elements.automationList.addEventListener("click", (event) => {
    const row = event.target.closest("[data-automation-id], [data-draft]");
    if (!row) {
      return;
    }
    if (row.dataset.draft === "true") {
      renderAutomations();
      renderDetail();
      renderRecentRuns();
      return;
    }
    void loadAutomationDetail(row.dataset.automationId);
  });

  elements.configFields.addEventListener("input", handleFormEdit);
  elements.configFields.addEventListener("change", handleFormEdit);

  elements.promptPreview.addEventListener("input", () => {
    if (!state.editor || state.editor.automation_type !== "search_report") {
      return;
    }
    state.editor.prompt_body = elements.promptPreview.value;
  });

  elements.saveAutomationButton.addEventListener("click", () => {
    void saveCurrentAutomation();
  });

  elements.runNowButton.addEventListener("click", () => {
    void handleRunNow();
  });

  elements.pauseButton.addEventListener("click", () => {
    void handlePauseResume();
  });

  elements.archiveButton.addEventListener("click", () => {
    void handleArchive();
  });

  elements.applyJsonButton.addEventListener("click", () => {
    applyJsonEditor();
  });

  elements.showBoundaryButton.addEventListener("click", () => {
    window.alert("This page owns saved recipes, prompt files, and simple lifecycle controls. Deep runtime logs, retries, and worker internals stay in Prefect.");
  });

  [elements.openDeploymentButton, elements.openRunButton].forEach((link) => {
    link.addEventListener("click", (event) => {
      if (link.classList.contains("is-disabled")) {
        event.preventDefault();
      }
    });
  });
}

async function initializePage() {
  await loadSystemStatus();
  await refreshAutomationShelf();
}

async function loadSystemStatus() {
  try {
    const health = await fetchJSON("/health");
    const prefectApiUrl = String(health.prefect_api_url || "");
    if (prefectApiUrl) {
      state.prefectLink = prefectApiUrl.replace(/\/api\/?$/, "");
      elements.prefectLink.href = state.prefectLink;
      elements.prefectState.textContent = "Connected";
      elements.prefectNote.textContent = state.prefectLink;
      return;
    }
    elements.prefectState.textContent = "Unavailable";
    elements.prefectNote.textContent = "No Prefect API URL was returned by the health endpoint.";
  } catch (error) {
    elements.prefectState.textContent = "Offline";
    elements.prefectNote.textContent = `Could not read stack health: ${error.message || String(error)}`;
  }
}

async function refreshAutomationShelf(preferredId = null) {
  try {
    const payload = await fetchJSON("/automations?limit=100");
    state.automations = Array.isArray(payload.automations) ? payload.automations : [];
    renderAutomations();

    const selectedId = preferredId || (state.editorMode === "saved" ? state.editor?.id : null);
    if (selectedId && state.automations.some((item) => item.id === selectedId)) {
      await loadAutomationDetail(selectedId);
      return;
    }

    if (state.automations.length) {
      await loadAutomationDetail(state.automations[0].id);
      return;
    }

    createDraftFromTemplate(state.selectedTemplateId);
  } catch (error) {
    showFlash(`Could not load automations: ${error.message || String(error)}`, true);
    state.automations = [];
    if (!state.editor) {
      createDraftFromTemplate(state.selectedTemplateId);
    } else {
      renderAutomations();
      renderDetail();
      renderRecentRuns();
    }
  }
}

async function loadAutomationDetail(automationId) {
  if (!automationId) {
    return;
  }

  try {
    const [detailPayload, runsPayload] = await Promise.all([
      fetchJSON(`/automations/${encodeURIComponent(automationId)}`),
      fetchJSON(`/automations/${encodeURIComponent(automationId)}/runs?limit=8`),
    ]);
    state.editor = normalizeAutomation(detailPayload.automation || {});
    state.editorMode = "saved";
    state.selectedTemplateId = state.editor.automation_type;
    state.recentRuns = Array.isArray(runsPayload.runs) ? runsPayload.runs : [];
    renderTemplates();
    renderAutomations();
    renderDetail();
    renderRecentRuns();
  } catch (error) {
    showFlash(`Could not load automation detail: ${error.message || String(error)}`, true);
  }
}

function createDraftFromTemplate(templateId) {
  const template = templateForType(templateId);
  state.selectedTemplateId = template.id;
  state.editorMode = "draft";
  state.editor = normalizeAutomation({
    automation_type: template.id,
    title: template.title,
    description: template.description,
    schedule_text: template.scheduleText,
    timezone: userTimeZone(),
    work_pool: template.workPool,
    status: "active",
    prompt_path: "",
    prompt_body: template.id === "search_report" ? DEFAULT_SEARCH_PROMPT : "",
    payload: defaultPayloadFor(template.id),
    prefect: { status: "draft" },
    latest_run: null,
  });
  state.recentRuns = [];
  renderTemplates();
  renderAutomations();
  renderDetail();
  renderRecentRuns();
}

function renderTemplates() {
  elements.templateGrid.innerHTML = templateCatalog
    .map((template) => {
      const active = template.id === state.selectedTemplateId ? " is-selected" : "";
      return `
        <button class="template-card${active}" type="button" data-template-id="${escapeHTML(template.id)}">
          <div class="detail-kicker">${escapeHTML(template.label)}</div>
          <div class="template-card-title">${escapeHTML(template.headline)}</div>
          <div class="template-card-body">${escapeHTML(template.description)}</div>
          <div class="template-pill-row">
            ${template.pills.map((pill) => `<span class="tag">${escapeHTML(pill)}</span>`).join("")}
          </div>
        </button>
      `;
    })
    .join("");
}

function renderAutomations() {
  const savedCount = state.automations.length;
  const draftActive = state.editorMode === "draft" && state.editor;
  elements.automationCount.textContent = `${savedCount} recipes`;
  elements.listMeta.textContent = draftActive ? `${savedCount} saved recipes + draft` : `${savedCount} saved recipes`;

  if (!savedCount && !draftActive) {
    elements.automationList.innerHTML = emptyStateMarkup(
      "No automations yet",
      "Choose a starting pattern above and create the first saved run."
    );
    return;
  }

  const rows = [];
  if (draftActive) {
    rows.push(`
      <button class="automation-row is-selected" type="button" data-draft="true">
        <div class="recipe-type">${escapeHTML(typeLabel(state.editor.automation_type))}</div>
        <div class="recipe-title">${escapeHTML(state.editor.title || `${typeLabel(state.editor.automation_type)} draft`)}</div>
        <div class="recipe-summary">Unsaved draft. Save it to create the Prefect deployment and enable runtime actions.</div>
        <div class="recipe-meta">
          <span class="tag">Draft</span>
          <span class="tag">${escapeHTML(scheduleLabel(state.editor.schedule_text))}</span>
        </div>
      </button>
    `);
  }

  rows.push(
    ...state.automations.map((item) => {
      const active = state.editorMode === "saved" && state.editor?.id === item.id ? " is-selected" : "";
      return `
        <button class="automation-row${active}" type="button" data-automation-id="${escapeHTML(item.id)}">
          <div class="recipe-type">${escapeHTML(typeLabel(item.automation_type))}</div>
          <div class="recipe-title">${escapeHTML(item.title)}</div>
          <div class="recipe-summary">${escapeHTML(item.description || templateForType(item.automation_type).description)}</div>
          <div class="recipe-meta">
            <span class="tag">${escapeHTML(item.status)}</span>
            <span class="tag">${escapeHTML(scheduleLabel(item.schedule_text))}</span>
          </div>
        </button>
      `;
    })
  );

  elements.automationList.innerHTML = rows.join("");
}

function renderDetail() {
  const record = state.editor;
  if (!record) {
    elements.detailTitle.textContent = "Select a recipe";
    elements.detailSubtitle.textContent = "Inspect cadence, scope, and lightweight runtime status.";
    elements.scheduleTitle.textContent = "Cadence pending";
    elements.scheduleSummary.textContent = "Select a recipe to inspect its cadence and worker lane.";
    elements.detailMeta.innerHTML = "";
    elements.configFields.innerHTML = emptyStateMarkup("No recipe selected", "Choose a saved automation or start a draft from a template.");
    elements.promptPath.textContent = "No prompt file for this recipe.";
    elements.promptPreview.value = "Select a search automation to inspect its prompt instructions.";
    elements.promptPreview.disabled = true;
    elements.jsonPreview.value = "{}";
    elements.jsonPreview.disabled = true;
    elements.saveAutomationButton.disabled = true;
    elements.runNowButton.disabled = true;
    elements.pauseButton.disabled = true;
    elements.archiveButton.disabled = true;
    elements.applyJsonButton.disabled = true;
    setLinkState(elements.openDeploymentButton, state.prefectLink, false);
    setLinkState(elements.openRunButton, state.prefectLink, false);
    return;
  }

  const isDraft = state.editorMode === "draft";
  const isArchived = record.status === "archived";
  const isSearch = record.automation_type === "search_report";
  const latestRun = record.latest_run;

  elements.detailTitle.textContent = record.title || `${typeLabel(record.automation_type)} draft`;
  elements.detailSubtitle.textContent = isDraft
    ? `${typeLabel(record.automation_type)} draft. Save it here, then inspect deeper runtime detail in Prefect.`
    : `${typeLabel(record.automation_type)} automation with thin runtime status and Prefect deep-linking.`;
  elements.scheduleTitle.textContent = scheduleLabel(record.schedule_text);
  elements.scheduleSummary.textContent = record.description || templateForType(record.automation_type).description;

  elements.detailMeta.innerHTML = [
    metaItem("Status", isDraft ? "draft" : record.status),
    metaItem("Prefect", record.prefect?.status || "unknown"),
    metaItem("Next run", formatDateTime(record.prefect?.next_run_at) || "Not scheduled"),
    metaItem("Deployment", record.prefect?.deployment_path || "Not created yet"),
  ].join("");

  elements.configFields.innerHTML = renderStructuredEditor(record);
  elements.promptPath.textContent = isSearch
    ? record.prompt_path || "Prompt file will be created under the configured automation prompt root on save."
    : "No prompt file for this recipe.";
  elements.promptPreview.disabled = !isSearch || isArchived;
  elements.promptPreview.value = isSearch
    ? record.prompt_body || DEFAULT_SEARCH_PROMPT
    : "This automation type is configured through structured fields and payload JSON.";
  elements.jsonPreview.disabled = isArchived;
  elements.jsonPreview.value = JSON.stringify(record.payload || {}, null, 2);

  elements.saveAutomationButton.textContent = isDraft ? "Create" : "Save";
  elements.saveAutomationButton.disabled = isArchived;
  elements.runNowButton.disabled = isDraft || isArchived;
  elements.pauseButton.textContent = record.status === "paused" ? "Resume" : "Pause";
  elements.pauseButton.disabled = isDraft || isArchived;
  elements.archiveButton.disabled = isDraft || isArchived;
  elements.applyJsonButton.disabled = isArchived;

  setLinkState(
    elements.openDeploymentButton,
    record.prefect?.deployment_url || state.prefectLink,
    Boolean(record.prefect?.deployment_url)
  );
  setLinkState(
    elements.openRunButton,
    latestRunUrl(latestRun),
    Boolean(latestRunUrl(latestRun))
  );
}

function renderRecentRuns() {
  const record = state.editor;
  if (!record || !state.recentRuns.length) {
    elements.recentRunCount.textContent = "0";
    elements.recentRunNote.textContent = record && state.editorMode === "draft"
      ? "Runs appear after the first saved execution"
      : "No top-level runs yet";
    elements.recentRuns.innerHTML = emptyStateMarkup(
      "No recent runs loaded",
      "This panel shows top-level runs for the selected automation."
    );
    return;
  }

  elements.recentRunCount.textContent = String(state.recentRuns.length);
  elements.recentRunNote.textContent = `${record.title} latest run: ${state.recentRuns[0].status || "unknown"}`;
  elements.recentRuns.innerHTML = state.recentRuns
    .map((run) => {
      const statusClass = `is-${String(run.status || "").toLowerCase()}`;
      return `
        <div class="recent-run">
          <span class="recent-run-status ${escapeHTML(statusClass)}">${escapeHTML(run.status || "unknown")}</span>
          <div class="recent-run-main">
            <div class="recent-run-title">${escapeHTML(humanizeFlowName(run.flow_name || "run"))}</div>
            <div class="recent-run-subtitle">${escapeHTML(run.id || "n/a")}</div>
          </div>
          <div class="recent-run-date">${escapeHTML(formatDateTime(run.updated_at || run.created_at) || "—")}</div>
        </div>
      `;
    })
    .join("");
}

function handleFormEdit(event) {
  if (!state.editor) {
    return;
  }
  const target = event.target;
  const scope = target.dataset.scope;
  const field = target.dataset.field;
  if (!scope || !field) {
    return;
  }

  if (scope === "common") {
    state.editor[field] = target.value;
  } else if (scope === "payload") {
    updatePayloadField(field, target);
  }

  if (field === "prompt_path") {
    state.editor.prompt_path = target.value;
    elements.promptPath.textContent = target.value || "Prompt file will be created under the configured automation prompt root on save.";
  }

  if (state.editorMode === "draft") {
    renderAutomations();
  }
  elements.jsonPreview.value = JSON.stringify(state.editor.payload || {}, null, 2);
}

function updatePayloadField(field, target) {
  const payload = state.editor.payload || {};
  switch (field) {
    case "include_news":
    case "include_arxiv":
    case "include_browser_jobs":
    case "publish":
    case "planner_enabled":
      payload[field] = Boolean(target.checked);
      break;
    case "max_results_per_query":
      payload[field] = clampInteger(target.value, 1, 50, 8);
      break;
    case "compare_prompt":
    case "theme":
      payload[field] = target.value;
      break;
    case "queries":
      payload.queries = splitLines(target.value);
      break;
    case "papers":
      payload.papers = parsePaperLines(target.value);
      break;
    case "enabled_sources": {
      const checked = Array.from(
        elements.configFields.querySelectorAll('[data-field="enabled_sources"]:checked')
      ).map((item) => item.value);
      payload.enabled_sources = checked;
      break;
    }
    default:
      payload[field] = target.value;
      break;
  }
  state.editor.payload = payload;
}

function applyJsonEditor() {
  if (!state.editor) {
    return;
  }

  try {
    const parsed = JSON.parse(elements.jsonPreview.value || "{}");
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("Payload JSON must be an object");
    }
    state.editor.payload = parsed;
    renderDetail();
    showFlash("Applied payload JSON to the editor.");
  } catch (error) {
    showFlash(`Could not apply JSON: ${error.message || String(error)}`, true);
  }
}

async function saveCurrentAutomation({ quiet = false } = {}) {
  if (!state.editor) {
    return null;
  }

  const requestBody = buildAutomationRequest(state.editor);
  if (!requestBody.title) {
    showFlash("Title is required before saving.", true);
    return null;
  }

  try {
    const endpoint = state.editorMode === "draft"
      ? "/automations"
      : `/automations/${encodeURIComponent(state.editor.id)}`;
    const method = state.editorMode === "draft" ? "POST" : "PATCH";
    const payload = await fetchJSON(endpoint, {
      method,
      body: JSON.stringify(requestBody),
    });
    if (!quiet) {
      showFlash(state.editorMode === "draft" ? "Automation created." : "Automation updated.");
    }
    await refreshAutomationShelf(payload.automation?.id || null);
    return payload.automation || null;
  } catch (error) {
    showFlash(`Could not save automation: ${error.message || String(error)}`, true);
    return null;
  }
}

async function handleRunNow() {
  if (!state.editor || state.editorMode === "draft" || state.editor.status === "archived") {
    return;
  }

  const saved = await saveCurrentAutomation({ quiet: true });
  if (saved === null) {
    return;
  }

  try {
    const payload = await fetchJSON(`/automations/${encodeURIComponent(saved.id)}/run`, {
      method: "POST",
      body: JSON.stringify({
        payload_overrides: cloneJSON(state.editor.payload || {}),
      }),
    });
    showFlash(`Run queued: ${payload.run_id || "pending"}.`);
    await loadAutomationDetail(saved.id);
  } catch (error) {
    showFlash(`Could not start run: ${error.message || String(error)}`, true);
  }
}

async function handlePauseResume() {
  if (!state.editor || state.editorMode === "draft" || state.editor.status === "archived") {
    return;
  }

  const action = state.editor.status === "paused" ? "resume" : "pause";
  try {
    const payload = await fetchJSON(`/automations/${encodeURIComponent(state.editor.id)}/${action}`, {
      method: "POST",
    });
    showFlash(action === "pause" ? "Automation paused." : "Automation resumed.");
    await refreshAutomationShelf(payload.automation?.id || state.editor.id);
  } catch (error) {
    showFlash(`Could not ${action} automation: ${error.message || String(error)}`, true);
  }
}

async function handleArchive() {
  if (!state.editor || state.editorMode === "draft" || state.editor.status === "archived") {
    return;
  }
  if (!window.confirm(`Archive "${state.editor.title}"? The saved run history stays available, but the Prefect deployment will be removed.`)) {
    return;
  }

  try {
    await fetchJSON(`/automations/${encodeURIComponent(state.editor.id)}/archive`, {
      method: "POST",
    });
    showFlash("Automation archived.");
    await refreshAutomationShelf();
  } catch (error) {
    showFlash(`Could not archive automation: ${error.message || String(error)}`, true);
  }
}

function buildAutomationRequest(record) {
  const payload = sanitizePayload(record.automation_type, cloneJSON(record.payload || {}));
  const title = String(record.title || "").trim();
  const request = {
    title,
    description: normalizeNullable(record.description),
    schedule_text: normalizeNullable(record.schedule_text),
    timezone: normalizeNullable(record.timezone),
    work_pool: normalizeNullable(record.work_pool),
    payload,
  };

  if (record.automation_type === "search_report") {
    request.prompt_path = normalizeNullable(record.prompt_path);
    request.prompt_body = String(record.prompt_body || "").trim() || DEFAULT_SEARCH_PROMPT;
  }

  if (state.editorMode === "draft") {
    request.automation_type = record.automation_type;
  }

  return request;
}

function sanitizePayload(automationType, payload) {
  const normalized = payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {};
  switch (automationType) {
    case "daily_brief":
      normalized.include_news = Boolean(normalized.include_news);
      normalized.include_arxiv = Boolean(normalized.include_arxiv);
      normalized.include_browser_jobs = Boolean(normalized.include_browser_jobs);
      normalized.publish = Boolean(normalized.publish);
      normalized.metadata = normalizeObject(normalized.metadata);
      normalized.human_policy = normalizeObject(normalized.human_policy);
      return normalized;
    case "paper_batch":
      normalized.compare_prompt = String(
        normalized.compare_prompt
          || "Compare the papers, highlight agreements and disagreements, and summarize the practical implications."
      );
      normalized.papers = Array.isArray(normalized.papers) ? normalized.papers.filter((item) => String(item?.paper_id || "").trim()) : [];
      normalized.metadata = normalizeObject(normalized.metadata);
      normalized.human_policy = normalizeObject(normalized.human_policy);
      return normalized;
    case "search_report":
      normalized.theme = String(normalized.theme || "").trim();
      normalized.queries = Array.isArray(normalized.queries) ? normalized.queries.map((item) => String(item || "").trim()).filter(Boolean) : [];
      normalized.enabled_sources = Array.isArray(normalized.enabled_sources)
        ? normalized.enabled_sources.map((item) => String(item)).filter(Boolean)
        : ["local_knowledge"];
      normalized.planner_enabled = normalized.planner_enabled !== false;
      normalized.max_results_per_query = clampInteger(normalized.max_results_per_query, 1, 50, 8);
      normalized.metadata = normalizeObject(normalized.metadata);
      normalized.human_policy = normalizeObject(normalized.human_policy);
      return normalized;
    default:
      return normalized;
  }
}

function renderStructuredEditor(record) {
  const payload = sanitizePayload(record.automation_type, cloneJSON(record.payload || {}));
  return `
    <div class="structured-grid interaction-form">
      <div class="automation-form-grid">
        ${textField("Title", "common", "title", record.title, "Morning brief")}
        ${textField("Timezone", "common", "timezone", record.timezone || userTimeZone(), "America/Toronto")}
        ${textAreaField("Description", "common", "description", record.description, "A short note about what this automation should produce.", true)}
        ${textField("Schedule (cron)", "common", "schedule_text", record.schedule_text, "0 7 * * 1-5")}
        ${textField("Work pool", "common", "work_pool", record.work_pool || templateForType(record.automation_type).workPool, "mini-process")}
        ${renderTypeSpecificFields(record.automation_type, payload)}
      </div>
    </div>
  `;
}

function renderTypeSpecificFields(automationType, payload) {
  switch (automationType) {
    case "daily_brief":
      return `
        <div class="full-width">
          <div class="structured-label">Included lanes</div>
          <div class="toggle-list">
            ${toggleField("Include news", "Daily news ingestion and synthesis", "payload", "include_news", Boolean(payload.include_news))}
            ${toggleField("Include arXiv", "Paper ingest and summary lane", "payload", "include_arxiv", Boolean(payload.include_arxiv))}
            ${toggleField("Include browser jobs", "Allow browser-backed collection when needed", "payload", "include_browser_jobs", Boolean(payload.include_browser_jobs))}
            ${toggleField("Publish report", "Mark the brief for publication output", "payload", "publish", Boolean(payload.publish))}
          </div>
        </div>
      `;
    case "paper_batch":
      return `
        ${textAreaField(
          "Compare prompt",
          "payload",
          "compare_prompt",
          payload.compare_prompt,
          "Compare the papers, highlight agreements and disagreements, and summarize the practical implications.",
          true
        )}
        ${textAreaField(
          "Papers",
          "payload",
          "papers",
          formatPaperLines(payload.papers),
          "One paper per line: paper_id | title | source_url",
          true
        )}
        <div class="full-width form-help">Use one paper per line. Only the paper id is required.</div>
      `;
    case "search_report":
      return `
        ${textField("Theme", "payload", "theme", payload.theme, "Semiconductor supply chain")}
        ${numberField("Results per query", "payload", "max_results_per_query", payload.max_results_per_query || 8, 1, 50)}
        ${textAreaField("Queries", "payload", "queries", formatStringList(payload.queries), "One query per line", true)}
        ${textField("Prompt file path", "common", "prompt_path", state.editor.prompt_path, "searches/chip-search.md", true)}
        <div class="full-width form-help">Relative prompt paths are stored under the configured automation prompt root.</div>
        <div class="full-width">
          <div class="structured-label">Enabled sources</div>
          <div class="toggle-list">
            ${checkboxField("Local knowledge", "Use the local retrieval corpus", "payload", "enabled_sources", "local_knowledge", hasSource(payload, "local_knowledge"))}
            ${checkboxField("Browser web", "Allow browser-backed web search inside Prefect", "payload", "enabled_sources", "browser_web", hasSource(payload, "browser_web"))}
            ${toggleField("Planner enabled", "Let Codex plan and expand the search within the enabled source set", "payload", "planner_enabled", payload.planner_enabled !== false)}
          </div>
        </div>
      `;
    default:
      return "";
  }
}

function textField(label, scope, field, value, placeholder, fullWidth = false) {
  return `
    <label class="${fullWidth ? "full-width" : ""}">
      <span>${escapeHTML(label)}</span>
      <input
        type="text"
        data-scope="${escapeHTML(scope)}"
        data-field="${escapeHTML(field)}"
        value="${escapeHTML(String(value || ""))}"
        placeholder="${escapeHTML(String(placeholder || ""))}"
      >
    </label>
  `;
}

function textAreaField(label, scope, field, value, placeholder, fullWidth = false) {
  return `
    <label class="${fullWidth ? "full-width" : ""}">
      <span>${escapeHTML(label)}</span>
      <textarea
        rows="5"
        data-scope="${escapeHTML(scope)}"
        data-field="${escapeHTML(field)}"
        placeholder="${escapeHTML(String(placeholder || ""))}"
      >${escapeHTML(String(value || ""))}</textarea>
    </label>
  `;
}

function numberField(label, scope, field, value, min, max) {
  return `
    <label>
      <span>${escapeHTML(label)}</span>
      <input
        type="number"
        min="${escapeHTML(String(min))}"
        max="${escapeHTML(String(max))}"
        data-scope="${escapeHTML(scope)}"
        data-field="${escapeHTML(field)}"
        value="${escapeHTML(String(value || ""))}"
      >
    </label>
  `;
}

function toggleField(label, note, scope, field, checked) {
  return `
    <label class="toggle-field">
      <input type="checkbox" data-scope="${escapeHTML(scope)}" data-field="${escapeHTML(field)}" ${checked ? "checked" : ""}>
      <span class="toggle-copy">
        <span class="toggle-title">${escapeHTML(label)}</span>
        <span class="toggle-note">${escapeHTML(note)}</span>
      </span>
    </label>
  `;
}

function checkboxField(label, note, scope, field, value, checked) {
  return `
    <label class="toggle-field">
      <input
        type="checkbox"
        data-scope="${escapeHTML(scope)}"
        data-field="${escapeHTML(field)}"
        value="${escapeHTML(value)}"
        ${checked ? "checked" : ""}
      >
      <span class="toggle-copy">
        <span class="toggle-title">${escapeHTML(label)}</span>
        <span class="toggle-note">${escapeHTML(note)}</span>
      </span>
    </label>
  `;
}

function normalizeAutomation(record) {
  const automationType = String(record.automation_type || state.selectedTemplateId || "daily_brief");
  return {
    id: record.id || null,
    task_key: record.task_key || null,
    automation_type: automationType,
    flow_name: record.flow_name || flowNameForType(automationType),
    title: record.title || "",
    description: record.description || "",
    schedule_text: record.schedule_text || "",
    timezone: record.timezone || userTimeZone(),
    work_pool: record.work_pool || templateForType(automationType).workPool,
    status: record.status || "active",
    prompt_path: record.prompt_path || "",
    prompt_body: record.prompt_body || (automationType === "search_report" ? DEFAULT_SEARCH_PROMPT : ""),
    payload: sanitizePayload(automationType, cloneJSON(record.payload || defaultPayloadFor(automationType))),
    prefect: cloneJSON(record.prefect || { status: "unknown" }),
    latest_run: cloneJSON(record.latest_run || null),
  };
}

function defaultPayloadFor(automationType) {
  switch (automationType) {
    case "daily_brief":
      return {
        include_news: true,
        include_arxiv: true,
        include_browser_jobs: true,
        publish: true,
      };
    case "paper_batch":
      return {
        papers: [],
        compare_prompt: "Compare the papers, highlight agreements and disagreements, and summarize the practical implications.",
      };
    case "search_report":
      return {
        theme: "",
        queries: [],
        enabled_sources: ["local_knowledge"],
        planner_enabled: true,
        max_results_per_query: 8,
      };
    default:
      return {};
  }
}

function templateForType(automationType) {
  return templateCatalog.find((item) => item.id === automationType) || templateCatalog[0];
}

function typeLabel(value) {
  return templateForType(value).label;
}

function flowNameForType(value) {
  if (value === "paper_batch") {
    return "paper_batch_flow";
  }
  if (value === "search_report") {
    return "codex_search_report_flow";
  }
  return "daily_brief_flow";
}

function latestRunUrl(run) {
  if (!run || !run.prefect_flow_run_id) {
    return "";
  }
  return `${state.prefectLink}/runs/flow-run/${encodeURIComponent(run.prefect_flow_run_id)}`;
}

function setTodayDate() {
  elements.automationTodayDate.textContent = new Date().toLocaleDateString("en-CA", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function setLinkState(link, href, enabled) {
  link.href = href || state.prefectLink;
  link.classList.toggle("is-disabled", !enabled);
  link.setAttribute("aria-disabled", enabled ? "false" : "true");
  link.tabIndex = enabled ? 0 : -1;
}

function showFlash(message, isError = false) {
  elements.pageFlash.hidden = false;
  elements.pageFlash.textContent = message;
  elements.pageFlash.style.borderColor = isError ? "rgba(201, 100, 93, 0.5)" : "";
  elements.pageFlash.style.color = isError ? "#ffd6d3" : "";
}

function splitLines(value) {
  return String(value || "")
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function parsePaperLines(value) {
  return splitLines(value).map((line) => {
    const [paperId = "", title = "", sourceUrl = ""] = line.split("|").map((item) => item.trim());
    const item = { paper_id: paperId };
    if (title) {
      item.title = title;
    }
    if (sourceUrl) {
      item.source_url = sourceUrl;
    }
    return item;
  }).filter((item) => item.paper_id);
}

function formatPaperLines(items) {
  if (!Array.isArray(items)) {
    return "";
  }
  return items
    .map((item) => [item.paper_id || "", item.title || "", item.source_url || ""].filter(Boolean).join(" | "))
    .join("\n");
}

function formatStringList(items) {
  return Array.isArray(items) ? items.join("\n") : "";
}

function hasSource(payload, source) {
  return Array.isArray(payload.enabled_sources) && payload.enabled_sources.includes(source);
}

function normalizeNullable(value) {
  const text = String(value || "").trim();
  return text || null;
}

function normalizeObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function clampInteger(value, min, max, fallback) {
  const parsed = Number.parseInt(String(value), 10);
  if (Number.isNaN(parsed)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, parsed));
}

function cloneJSON(value) {
  if (value === null || value === undefined) {
    return value;
  }
  return JSON.parse(JSON.stringify(value));
}

function emptyStateMarkup(title, subtitle) {
  return `
    <div class="empty-state">
      <div class="empty-title">${escapeHTML(title)}</div>
      <div class="empty-sub">${escapeHTML(subtitle)}</div>
    </div>
  `;
}

function scheduleLabel(value) {
  return String(value || "").trim() || "No schedule";
}

function metaItem(label, value) {
  return `
    <div class="meta-item">
      <span class="meta-label">${escapeHTML(label)}</span>
      <span class="meta-value">${escapeHTML(String(value ?? "—"))}</span>
    </div>
  `;
}

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const payload = await response.json();
      detail = payload.detail || JSON.stringify(payload);
    } catch {}
    throw new Error(detail);
  }
  return response.json();
}

function humanizeFlowName(value) {
  return String(value)
    .replace(/_flow$/, "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatDateTime(value) {
  if (!value) {
    return "";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  return parsed.toLocaleString("en-CA", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function userTimeZone() {
  return Intl.DateTimeFormat().resolvedOptions().timeZone || "America/Toronto";
}

function escapeHTML(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
