const state = {
  sources: [],
  selectedId: null,
  activeQuery: "",
  loading: false,
  detailsById: {},
  previewsById: {},
};

const elements = {};

document.addEventListener("DOMContentLoaded", () => {
  bindElements();
  bindEvents();
  setTodayDate();
  void loadSources();
});

function bindElements() {
  elements.todayDate = document.getElementById("sourcesTodayDate");
  elements.totalCount = document.getElementById("sourcesCount");
  elements.searchInput = document.getElementById("searchInput");
  elements.searchButton = document.getElementById("searchButton");
  elements.clearButton = document.getElementById("clearButton");
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

  elements.resultArea.addEventListener("click", (event) => {
    const row = event.target.closest("[data-source-id]");
    if (!row) {
      return;
    }
    const sourceId = row.dataset.sourceId;
    if (state.selectedId === sourceId) {
      state.selectedId = null;
      renderSources();
      return;
    }
    state.selectedId = sourceId;
    renderSources();
    void loadSourceDetail(sourceId);
  });
}

function setTodayDate() {
  elements.todayDate.textContent = new Date().toLocaleDateString("en-CA", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

async function loadSources() {
  state.loading = true;
  renderLoadingState();
  try {
    const payload = await fetchJSON("/sources?limit=100");
    state.sources = Array.isArray(payload.sources) ? payload.sources : [];
    state.selectedId = state.sources.some((source) => source.id === state.selectedId) ? state.selectedId : null;
    renderSources();
  } catch (error) {
    state.sources = [];
    elements.totalCount.textContent = "— sources";
    elements.statusText.textContent = "Could not load sources";
    elements.resultCount.textContent = "";
    elements.resultArea.innerHTML = emptyStateMarkup(
      "Could not load sources",
      error.message || String(error),
    );
  } finally {
    state.loading = false;
  }
}

async function loadSourceDetail(sourceId) {
  try {
    const payload = await fetchJSON(`/sources/${sourceId}`);
    const source = payload.source || null;
    if (!source) {
      throw new Error("Source detail payload was empty.");
    }
    state.detailsById[sourceId] = source;
    const previewUrl = source.current_text_artifact?.preview_url;
    if (previewUrl) {
      try {
        state.previewsById[sourceId] = await fetchJSON(previewUrl);
      } catch (previewError) {
        state.previewsById[sourceId] = {
          status: "error",
          error: previewError.message || String(previewError),
        };
      }
    } else {
      state.previewsById[sourceId] = null;
    }
    if (state.selectedId === sourceId) {
      renderSources();
    }
  } catch (error) {
    showFlash(`Could not load source detail: ${error.message || String(error)}`, true);
  }
}

function runSearch() {
  state.activeQuery = elements.searchInput.value.trim().toLowerCase();
  renderSources();
}

function clearSearch() {
  elements.searchInput.value = "";
  state.activeQuery = "";
  state.selectedId = null;
  hideFlash();
  renderSources();
}

function filteredSources() {
  if (!state.activeQuery) {
    return state.sources;
  }
  return state.sources.filter((source) => searchableSourceText(source).includes(state.activeQuery));
}

function searchableSourceText(source) {
  return [
    source.title,
    source.canonical_uri,
    source.author,
    source.source_type,
    JSON.stringify(source.metadata || {}),
  ]
    .concat(flattenValues(source.metadata || {}))
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function flattenValues(value) {
  if (value === null || value === undefined) {
    return [];
  }
  if (Array.isArray(value)) {
    return value.flatMap((item) => flattenValues(item));
  }
  if (typeof value === "object") {
    return Object.entries(value).flatMap(([key, item]) => [key, ...flattenValues(item)]);
  }
  return [String(value)];
}

function renderLoadingState() {
  elements.totalCount.textContent = "— sources";
  elements.statusText.textContent = "Loading sources";
  elements.resultCount.textContent = "";
  elements.resultArea.innerHTML = emptyStateMarkup(
    "Loading sources",
    "Pulling source documents and their current text artifacts.",
  );
}

function renderSources() {
  const sources = filteredSources();
  elements.totalCount.textContent = `${state.sources.length} sources`;
  elements.statusText.textContent = buildStatusText(sources.length, Boolean(state.activeQuery));
  elements.resultCount.textContent = sources.length
    ? `${sources.length} ${sources.length === 1 ? "source" : "sources"}`
    : "";

  if (state.selectedId && !sources.some((source) => source.id === state.selectedId)) {
    state.selectedId = null;
  }

  if (!sources.length) {
    elements.resultArea.innerHTML = emptyStateMarkup(
      "No sources found",
      state.activeQuery
        ? "No source documents match this search."
        : "No source documents have been ingested yet.",
    );
    return;
  }

  elements.resultArea.innerHTML = `<div class="doc-list">${sources.map(renderSourceRow).join("")}</div>`;
}

function buildStatusText(count, hasQuery) {
  if (hasQuery) {
    return `Results for “${elements.searchInput.value.trim()}”`;
  }
  return count ? "Showing all sources" : "No sources available";
}

function renderSourceRow(source) {
  const selected = source.id === state.selectedId ? " is-selected" : "";
  const detail = source.id === state.selectedId ? renderSourceDetail(source) : "";
  return `
    <div class="doc-row-wrap">
      <button class="doc-row${selected}" type="button" data-source-id="${escapeHTML(source.id)}">
        <div class="doc-main">
          <div class="doc-source">${escapeHTML(source.source_type || "Source")}</div>
          <div class="doc-title">${escapeHTML(source.title || source.canonical_uri || "Untitled source")}</div>
          <div class="doc-desc">${escapeHTML(source.canonical_uri || "No canonical URI stored.")}</div>
          <div class="doc-tags">
            ${renderSourceTags(source)}
          </div>
        </div>
        <div class="doc-right">
          <div class="doc-date">${escapeHTML(formatDate(source.updated_at || source.created_at))}</div>
        </div>
      </button>
      ${detail}
    </div>
  `;
}

function renderSourceTags(source) {
  const tags = [];
  if (Array.isArray(source.metadata?.tags)) {
    for (const tag of source.metadata.tags) {
      tags.push(`<span class="tag">${escapeHTML(String(tag))}</span>`);
    }
  }
  if (Array.isArray(source.metadata?.entities)) {
    for (const entity of source.metadata.entities) {
      tags.push(`<span class="tag">${escapeHTML(String(entity))}</span>`);
    }
  }
  tags.push(`<span class="tag">${escapeHTML(String(source.artifact_count || 0))} artifacts</span>`);
  return tags.join("");
}

function renderSourceDetail(source) {
  const detail = state.detailsById[source.id] || source;
  const currentArtifact = detail.current_text_artifact;
  const preview = state.previewsById[source.id];
  return `
    <div class="doc-detail">
      <div class="doc-detail-inner source-detail">
        <div class="detail-kicker">Source details</div>
        <div class="detail-title">${escapeHTML(detail.title || source.title || "Untitled source")}</div>
        <div class="detail-summary">${escapeHTML(detail.canonical_uri || source.canonical_uri || "")}</div>
        <div class="detail-meta source-meta-grid">
          ${metaItem("Source type", detail.source_type || "Unknown")}
          ${metaItem("Author", detail.author || "Unknown")}
          ${metaItem("Published", formatDate(detail.published_at))}
          ${metaItem("Artifacts", String(detail.artifact_count ?? source.artifact_count ?? 0))}
        </div>

        ${renderMetadataBlock(detail.metadata)}

        <div class="source-section">
          <div class="source-section-title">Current text artifact</div>
          ${currentArtifact ? renderCurrentArtifact(currentArtifact, preview) : '<div class="source-empty-detail">No current text artifact is linked to this source.</div>'}
        </div>

        <div class="source-section">
          <div class="source-section-title">Linked artifacts</div>
          ${renderArtifactList(detail.artifacts || [])}
        </div>

        <div class="source-section">
          <div class="source-section-title">Current chunks</div>
          ${renderChunkList(detail.current_chunks || [])}
        </div>
      </div>
    </div>
  `;
}

function renderMetadataBlock(metadata) {
  if (!metadata || typeof metadata !== "object" || !Object.keys(metadata).length) {
    return "";
  }
  const badges = [];
  if (Array.isArray(metadata.tags)) {
    badges.push(...metadata.tags.map((tag) => `<span class="source-chip">${escapeHTML(String(tag))}</span>`));
  }
  if (Array.isArray(metadata.entities)) {
    badges.push(...metadata.entities.map((entity) => `<span class="source-chip">${escapeHTML(String(entity))}</span>`));
  }
  return `
    <div class="source-section">
      <div class="source-section-title">Metadata</div>
      <div class="source-chip-row">${badges.join("") || '<span class="source-chip">No tags or entities stored</span>'}</div>
      <div class="preview-block source-preview-block">
        <div class="preview-title">Raw metadata</div>
        <pre class="copy-block source-preview-content">${escapeHTML(JSON.stringify(metadata, null, 2))}</pre>
      </div>
    </div>
  `;
}

function renderCurrentArtifact(artifact, preview) {
  const previewBody = renderPreview(preview, artifact);
  return `
    <div class="source-artifact-item">
      <div class="source-artifact-top">
        <div class="source-artifact-title">${escapeHTML(artifact.kind || "Artifact")}</div>
        <div class="source-artifact-meta">${escapeHTML(formatDate(artifact.created_at))}</div>
      </div>
      <div class="source-artifact-body">${escapeHTML(artifact.storage_uri || artifact.path || "No storage URI")}</div>
      <div class="source-link-row">
        <a class="source-link" href="${escapeHTML(artifact.preview_url)}">Preview</a>
        <a class="source-link" href="${escapeHTML(artifact.download_url)}">Download</a>
      </div>
      <div class="source-preview-block">
        <div class="preview-title">Preview</div>
        ${previewBody}
      </div>
    </div>
  `;
}

function renderPreview(preview, artifact) {
  if (!preview) {
    return '<div class="source-preview-status">Loading preview via the artifact preview endpoint...</div>';
  }
  if (preview.status === "missing") {
    return `<div class="source-preview-status">Preview missing for ${escapeHTML(artifact.id)}.</div>`;
  }
  if (preview.status === "binary") {
    return `<div class="source-preview-status">Binary artifact. Use the download link to inspect the file.</div>`;
  }
  if (preview.status === "error") {
    return `<div class="source-preview-status">Could not load preview: ${escapeHTML(preview.error || "Unknown error")}</div>`;
  }
  const content = preview.content || "";
  return `
    <pre class="copy-block source-preview-content">${escapeHTML(content)}${preview.truncated ? "\n\n[truncated]" : ""}</pre>
  `;
}

function renderArtifactList(artifacts) {
  if (!artifacts.length) {
    return '<div class="source-empty-detail">No linked artifacts stored for this source.</div>';
  }
  return `
    <div class="source-artifact-list">
      ${artifacts.map((artifact) => `
        <div class="source-artifact-item">
          <div class="source-artifact-top">
            <div class="source-artifact-title">${escapeHTML(artifact.kind || "Artifact")}</div>
            <div class="source-artifact-meta">${escapeHTML(formatDate(artifact.created_at))}</div>
          </div>
          <div class="source-artifact-body">${escapeHTML(artifact.storage_uri || artifact.path || "No storage URI")}</div>
          <div class="source-link-row">
            <a class="source-link" href="${escapeHTML(artifact.preview_url)}">Preview</a>
            <a class="source-link" href="${escapeHTML(artifact.download_url)}">Download</a>
          </div>
        </div>
      `).join("")}
    </div>
  `;
}

function renderChunkList(chunks) {
  if (!chunks.length) {
    return '<div class="source-empty-detail">No chunks were found for the current text artifact.</div>';
  }
  return `
    <div class="source-chunk-list">
      ${chunks.map((chunk) => `
        <div class="source-chunk-item">
          <div class="source-chunk-top">
            <div class="source-chunk-title">Chunk ${escapeHTML(String(chunk.ordinal))}</div>
            <div class="source-chunk-meta">${escapeHTML(String(chunk.token_count || 0))} tokens</div>
          </div>
          <div class="source-chunk-body">${escapeHTML(chunk.text || "")}</div>
        </div>
      `).join("")}
    </div>
  `;
}

function metaItem(label, value) {
  return `
    <div class="meta-item">
      <span class="meta-label">${escapeHTML(label)}</span>
      <span class="meta-value">${escapeHTML(String(value || "—"))}</span>
    </div>
  `;
}

function emptyStateMarkup(title, subtitle) {
  return `
    <div class="empty-state">
      <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1" aria-hidden="true"><path d="M12 6.253v13"></path><path d="M12 6.253C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253"></path><path d="M12 6.253C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253"></path></svg>
      <div class="empty-title" style="margin-top:.6rem">${escapeHTML(title)}</div>
      <div class="empty-sub">${escapeHTML(subtitle)}</div>
    </div>
  `;
}

function showFlash(message, isError = false) {
  elements.flashMessage.hidden = false;
  elements.flashMessage.textContent = message;
  elements.flashMessage.classList.toggle("is-error", isError);
}

function hideFlash() {
  elements.flashMessage.hidden = true;
  elements.flashMessage.textContent = "";
  elements.flashMessage.classList.remove("is-error");
}

async function fetchJSON(url) {
  const response = await fetch(url, {
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed with status ${response.status}`);
  }
  return response.json();
}

function formatDate(value) {
  if (!value) {
    return "Unknown";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleDateString("en-CA", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function escapeHTML(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
