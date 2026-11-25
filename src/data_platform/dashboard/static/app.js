"use strict";

const $ = (id) => document.getElementById(id);
const REFRESH_INTERVAL = 60000;
const PAGE_SIZE = 100;
const SCHEMA_LAYERS = ["bronze", "silver", "gold"];
const SIDEBAR_DEFAULT_WIDTH = 280;
const SIDEBAR_MIN_WIDTH = 220;
const SIDEBAR_MAX_WIDTH = 480;
let nextRefreshAt = Date.now() + REFRESH_INTERVAL;
let refreshing = false;
const state = {
  relations: [],
  schemas: [],
  selected: null,
  columns: [],
  page: 1,
  sort: null,
  direction: "desc",
  headerFilters: {},
  paused: false,
  busy: false,
  pending: false,
  revision: 0,
  hasMore: false,
  columnKey: "",
};

const sidebarResizer = $("sidebar-resizer");
const sidebar = document.querySelector(".sidebar");
let sidebarDrag = null;

function toggleMobileTables(open) {
  sidebar.classList.toggle("mobile-open", open);
  $("mobile-tables").setAttribute("aria-expanded", String(open));
  $("mobile-tables").textContent = open ? "Close" : "Tables";
  document.querySelector("main").inert = open && mobileLayout.matches;
}
const mobileLayout = window.matchMedia("(max-width: 760px)");
mobileLayout.addEventListener("change", () => {
  $("column-menu").open = false;
  toggleMobileTables(false);
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && sidebar.classList.contains("mobile-open")) {
    toggleMobileTables(false);
    $("mobile-tables").focus();
  }
});
$("mobile-tables").addEventListener("click", () => {
  toggleMobileTables(!sidebar.classList.contains("mobile-open"));
});

function sidebarMaxWidth() {
  return Math.min(SIDEBAR_MAX_WIDTH, Math.floor(innerWidth * 0.4));
}

function setSidebarWidth(width, remember = true) {
  width = Math.round(
    Math.max(SIDEBAR_MIN_WIDTH, Math.min(sidebarMaxWidth(), width)),
  );
  document.documentElement.style.setProperty("--sidebar-width", `${width}px`);
  if (remember) {
    try {
      localStorage.setItem("sidebar-width", String(width));
    } catch {
      /* Storage may be disabled. */
    }
  }
  updateSidebarHandle();
}

function updateSidebarHandle() {
  if (innerWidth <= 760) return;
  sidebarResizer.setAttribute(
    "aria-valuenow",
    String(Math.round(sidebar.getBoundingClientRect().width)),
  );
  sidebarResizer.setAttribute("aria-valuemax", String(sidebarMaxWidth()));
}

try {
  const saved = Number(localStorage.getItem("sidebar-width"));
  if (
    Number.isFinite(saved) &&
    saved >= SIDEBAR_MIN_WIDTH &&
    saved <= SIDEBAR_MAX_WIDTH
  ) {
    document.documentElement.style.setProperty("--sidebar-width", `${saved}px`);
  }
} catch {
  /* Use the default width when storage is unavailable. */
}
updateSidebarHandle();
window.addEventListener("resize", updateSidebarHandle);

sidebarResizer.addEventListener("pointerdown", (event) => {
  if (event.button !== 0) return;
  event.preventDefault();
  sidebarDrag = {
    x: event.clientX,
    width: sidebar.getBoundingClientRect().width,
  };
  sidebarResizer.setPointerCapture(event.pointerId);
  document.body.classList.add("resizing-sidebar");
  closeFilter();
});
sidebarResizer.addEventListener("pointermove", (event) => {
  if (sidebarDrag)
    setSidebarWidth(sidebarDrag.width + event.clientX - sidebarDrag.x, false);
});
sidebarResizer.addEventListener("lostpointercapture", () => {
  if (sidebarDrag) setSidebarWidth(sidebar.getBoundingClientRect().width);
  sidebarDrag = null;
  document.body.classList.remove("resizing-sidebar");
});
sidebarResizer.addEventListener("dblclick", () =>
  setSidebarWidth(SIDEBAR_DEFAULT_WIDTH),
);
sidebarResizer.addEventListener("keydown", (event) => {
  const width = sidebar.getBoundingClientRect().width;
  const changes = {
    ArrowLeft: width - 10,
    ArrowRight: width + 10,
    Home: SIDEBAR_MIN_WIDTH,
    End: sidebarMaxWidth(),
  };
  if (event.key in changes) {
    event.preventDefault();
    setSidebarWidth(changes[event.key]);
    positionFilter();
  }
});

function notice(message, error = false) {
  $("notice").textContent = message;
  $("notice").classList.toggle("error", error);
  $("notice").hidden = !message;
}

async function getJSON(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(12000) });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(
      typeof body.detail === "string"
        ? body.detail
        : "Check your filters and try again.",
    );
  }
  return response.json();
}

function relationPath({ schema, name }) {
  return `/api/relations/${encodeURIComponent(schema)}/${encodeURIComponent(name)}`;
}

function schemaMarkerClass(schema) {
  const layer = SCHEMA_LAYERS[state.schemas.indexOf(schema)] || "";
  return `schema-dot ${layer}`;
}

function cellFormatter(cell) {
  const value = cell.getValue();
  const element = document.createElement("span");
  if (value === null || value === undefined) {
    element.className = "null-value";
    element.textContent = "NULL";
  } else if (value === "") {
    element.className = "empty-value";
    element.textContent = "(empty)";
  } else {
    element.textContent =
      typeof value === "object" ? JSON.stringify(value) : String(value);
  }
  return element;
}

const grid = new Tabulator("#grid", {
  data: [],
  columns: [],
  height: "100%",
  layout: "fitDataFill",
  placeholder: "No records to display",
  nestedFieldSeparator: false,
  columnDefaults: {
    minWidth: 135,
    maxWidth: 420,
    resizable: true,
    headerSort: false,
    formatter: cellFormatter,
    headerClick: (event, column) => {
      if (event.target.closest(".tabulator-header-filter")) return;
      const field = column.getField();
      if (state.sort !== field) {
        state.sort = field;
        state.direction = "asc";
      } else if (state.direction === "asc") {
        state.direction = "desc";
      } else {
        state.sort = null;
        state.direction = "desc";
      }
      state.page = 1;
      state.revision++;
      updateTitles();
      loadPage();
    },
  },
});
const gridReady = new Promise((resolve) => grid.on("tableBuilt", resolve));
grid.on("columnVisibilityChanged", updateColumnCount);

function updateColumnCount() {
  const columns = grid.getColumns();
  const visible = columns.filter((column) => column.isVisible()).length;
  $("column-count").textContent =
    visible === columns.length
      ? `${visible} columns`
      : `${visible} of ${columns.length} columns`;
}
let activeColumn = null;
let activeButton = null;
let lookupTimer;
let lookupRevision = 0;
let availableValues = [];
let moreValues = false;

function valueLabel(value) {
  return value === null ? "(NULL)" : value === "" ? "(empty)" : value;
}

function headerFilter(cell) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "header-value-filter";
  button.dataset.column = cell.getField();
  button.setAttribute("aria-haspopup", "dialog");
  button.setAttribute("aria-controls", "filter-popup");
  setFilterButton(button);
  button.addEventListener("click", (event) => {
    event.stopPropagation();
    openFilter(cell.getField(), button);
  });
  return button;
}

function setFilterButton(button) {
  const count = (state.headerFilters[button.dataset.column] || []).length;
  button.textContent = count ? `${count} selected` : "All values";
  button.classList.toggle("filtered", count > 0);
  button.setAttribute(
    "aria-label",
    `Filter ${button.dataset.column}: ${button.textContent}`,
  );
}

function applyHeaderFilters() {
  for (const button of document.querySelectorAll(".header-value-filter"))
    setFilterButton(button);
  $("clear-filters").disabled = Object.keys(state.headerFilters).length === 0;
  state.page = 1;
  state.revision++;
  loadPage();
}

function closeFilter() {
  $("filter-popup").hidePopover();
  clearFilterLookup();
}

function clearFilterLookup() {
  activeColumn = null;
  activeButton = null;
  clearTimeout(lookupTimer);
  lookupRevision++;
}

function openFilter(column, button) {
  if (activeColumn === column && $("filter-popup").matches(":popover-open")) {
    closeFilter();
    return;
  }
  activeColumn = column;
  activeButton = button;
  $("filter-title").textContent = `Filter ${column}`;
  $("value-search").value = "";
  availableValues = [];
  moreValues = false;
  renderValues();
  $("filter-popup").showPopover({ source: button });
  positionFilter();
  $("value-search").focus();
  lookupValues();
}

function positionFilter() {
  if (!activeButton || !$("filter-popup").matches(":popover-open")) return;
  const anchor = activeButton.getBoundingClientRect();
  const popup = $("filter-popup").getBoundingClientRect();
  $("filter-popup").style.left =
    `${Math.max(8, Math.min(anchor.left, innerWidth - popup.width - 8))}px`;
  $("filter-popup").style.top =
    `${Math.max(8, Math.min(anchor.bottom + 6, innerHeight - popup.height - 8))}px`;
}
window.addEventListener("resize", positionFilter);

async function lookupValues() {
  if (!activeColumn || !state.selected) return;
  const revision = ++lookupRevision;
  const params = new URLSearchParams({
    column: activeColumn,
    search: $("value-search").value,
  });
  $("value-summary").textContent = "Loading available values…";
  try {
    const data = await getJSON(
      `${relationPath(state.selected)}/values?${params}`,
    );
    if (revision !== lookupRevision || !activeColumn) return;
    availableValues = data.values;
    moreValues = data.has_more;
    renderValues();
  } catch (error) {
    if (revision === lookupRevision)
      $("value-summary").textContent = error.message;
  }
}

function renderValues() {
  $("value-options").replaceChildren();
  for (const value of availableValues) {
    const token = JSON.stringify(value);
    const label = document.createElement("label");
    label.className = "value-option";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.dataset.value = token;
    checkbox.addEventListener("change", () => {
      setFilterValue(token, checkbox.checked);
    });
    label.append(checkbox, document.createTextNode(valueLabel(value)));
    $("value-options").append(label);
  }
  updateValueSelection();
}

function setFilterValue(token, checked) {
  const selected = new Set(state.headerFilters[activeColumn] || []);
  if (checked) selected.add(token);
  else selected.delete(token);
  if (selected.size) state.headerFilters[activeColumn] = [...selected];
  else delete state.headerFilters[activeColumn]; // An empty subset means all values.
  applyHeaderFilters();
  updateValueSelection();
}

function updateValueSelection() {
  const selected = new Set(state.headerFilters[activeColumn] || []);
  $("all-values").checked = selected.size === 0;
  for (const checkbox of $("value-options").querySelectorAll("input")) {
    checkbox.checked = selected.has(checkbox.dataset.value);
  }
  const summary = selected.size
    ? `${selected.size} values selected`
    : "All values included";
  $("value-summary").textContent =
    `${summary} · ${moreValues ? "First 100 matches; search to find more" : `${availableValues.length} matches`}`;
  positionFilter();
}

$("value-search").addEventListener("input", () => {
  clearTimeout(lookupTimer);
  lookupRevision++;
  lookupTimer = setTimeout(lookupValues, 250);
});
$("all-values").addEventListener("change", () => {
  delete state.headerFilters[activeColumn];
  applyHeaderFilters();
  updateValueSelection();
});
$("close-filter").addEventListener("click", closeFilter);
$("filter-popup").addEventListener("toggle", (event) => {
  if (event.newState === "closed") clearFilterLookup();
});

function columnTitle(column) {
  // Build DOM nodes: database identifiers and values must never become HTML.
  const element = document.createElement("button");
  element.type = "button";
  element.className = "column-sort";
  element.setAttribute("aria-label", `Sort by ${column.name}`);
  element.title = "Click to cycle: ascending, descending, default order";
  element.textContent = column.name;
  if (state.sort === column.name) {
    const marker = document.createElement("span");
    marker.className = "sort-indicator";
    marker.textContent = state.direction === "asc" ? "↑" : "↓";
    element.append(marker);
  }
  const type = document.createElement("span");
  type.className = "column-type";
  type.textContent = column.type;
  element.append(type);
  return element;
}

function updateTitles() {
  for (const column of state.columns) {
    grid
      .getColumn(column.name)
      .getElement()
      .querySelector(".tabulator-col-title")
      .replaceChildren(columnTitle(column));
  }
}

function setColumns(columns) {
  state.columns = columns;
  grid.setColumns(
    columns.map((column) => ({
      field: column.name,
      ...(mobileLayout.matches && (column.name === "period" || column.name.endsWith("_at"))
        ? { minWidth: 220 }
        : {}),
      title: column.name,
      titleFormatter: () => columnTitle(column),
      tooltip: false,
      headerFilter,
    })),
  );
  updateColumnCount();
  $("column-options").replaceChildren();
  for (const column of columns) {
    const label = document.createElement("label");
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = true;
    checkbox.addEventListener("change", () => grid.toggleColumn(column.name));
    label.append(checkbox, document.createTextNode(column.name));
    $("column-options").append(label);
  }
}

function renderRelations() {
  const query = $("relation-search").value.toLowerCase().trim();
  $("relations").replaceChildren();
  for (const schema of state.schemas) {
    const matches = state.relations.filter(
      (item) =>
        item.schema === schema &&
        `${item.schema}.${item.name}`.toLowerCase().includes(query),
    );
    const group = document.createElement("section");
    group.className = "schema-group";
    const heading = document.createElement("div");
    heading.className = "schema-heading";
    const dot = document.createElement("span");
    dot.className = schemaMarkerClass(schema);
    const count = document.createElement("small");
    count.textContent = matches.length;
    heading.append(dot, document.createTextNode(schema), count);
    group.append(heading);
    for (const relation of matches) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "relation-link";
      const active =
        state.selected?.schema === relation.schema &&
        state.selected?.name === relation.name;
      button.classList.toggle("active", active);
      if (active) button.setAttribute("aria-current", "page");
      const symbol = document.createElement("span");
      symbol.className = "relation-symbol";
      symbol.textContent = relation.kind === "view" ? "◫" : "▦";
      const name = document.createElement("span");
      name.className = "relation-name";
      name.textContent = relation.name;
      button.append(symbol, name);
      button.addEventListener("click", () => selectRelation(relation));
      group.append(button);
    }
    $("relations").append(group);
  }
}

async function discover() {
  const data = await getJSON("/api/relations");
  state.schemas = data.schemas;
  state.relations = data.relations;
  const tables = data.relations.filter(
    (relation) => relation.kind === "table",
  ).length;
  const views = data.relations.length - tables;
  $("relation-summary").textContent =
    `${tables} ${tables === 1 ? "table" : "tables"} · ${views} ${views === 1 ? "view" : "views"}`;
  renderRelations();
  if (
    state.selected &&
    !data.relations.some(
      (item) =>
        item.schema === state.selected.schema &&
        item.name === state.selected.name,
    )
  ) {
    state.revision++;
    state.selected = null;
    state.columnKey = "";
    state.columns = [];
    grid.clearData();
    notice(
      "This relation is no longer available. Choose another relation.",
      true,
    );
  }
  if (!state.selected && data.relations.length) {
    // Prefer a current analytics view for the first visit.
    const initial =
      data.relations.find(
        (item) =>
          item.schema === data.schemas[2] && item.name.endsWith("_latest"),
      ) || data.relations[0];
    selectRelation(initial);
  } else if (!data.relations.length) {
    notice(
      "No relations yet. Run the ingestion pipeline to populate the warehouse.",
    );
  }
}

function selectRelation(relation) {
  if (window.matchMedia("(max-width: 760px)").matches) {
    const browsing = sidebar.contains(document.activeElement);
    toggleMobileTables(false);
    if (browsing) $("mobile-tables").focus();
  }
  state.selected = relation;
  state.page = 1;
  state.sort = null;
  state.direction = "desc";
  state.headerFilters = {};
  state.hasMore = false;
  state.columnKey = "";
  state.revision++;
  closeFilter();
  $("clear-filters").disabled = true;
  $("sort-summary").textContent = "Default sort";
  $("clear-sort").disabled = true;
  $("schema-label").textContent = relation.schema;
  $("schema-marker").className = schemaMarkerClass(relation.schema);
  $("schema-marker").hidden = false;
  $("kind-label").textContent = relation.kind.toUpperCase();
  $("relation-title").textContent = relation.name;
  $("column-menu").open = false;
  grid.clearData();
  renderRelations();
  loadPage();
}

async function loadPage() {
  if (!state.selected) return;
  if (state.busy) {
    state.pending = true;
    return;
  }
  state.busy = true;
  const revision = state.revision;
  const current = { ...state.selected };
  const base = relationPath(current);
  $("refresh").querySelector(".button-label").textContent = "Refreshing…";
  $("refresh").setAttribute("aria-label", "Refreshing");
  $("previous").disabled = $("next").disabled = true;
  try {
    await gridReady;
    const key = `${current.schema}.${current.name}`;
    if (state.columnKey !== key) {
      notice("Loading columns and records…");
    }
    const metadata = await getJSON(`${base}/columns`);
    if (revision !== state.revision) return;
    if (
      state.columnKey !== key ||
      JSON.stringify(metadata.columns) !== JSON.stringify(state.columns)
    ) {
      setColumns(metadata.columns);
      state.columnKey = key;
    }
    const params = new URLSearchParams({
      page: state.page,
      page_size: PAGE_SIZE,
      direction: state.direction,
    });
    for (const [column, selected] of Object.entries(state.headerFilters)) {
      for (const value of selected) {
        params.append("match_column", column);
        params.append("match_value", value);
      }
    }
    if (state.sort) params.set("sort", state.sort);
    const data = await getJSON(`${base}/rows?${params}`);
    if (revision !== state.revision) return;
    await grid.replaceData(data.rows);
    state.hasMore = data.has_more;
    $("sort-summary").textContent = data.sort.column
      ? `${data.sort.default ? "Default" : "Sort"}: ${data.sort.column} · ${data.sort.direction === "asc" ? "ascending" : "descending"}`
      : "Default row order";
    $("clear-sort").disabled = data.sort.default;
    $("page-label").textContent = `Page ${state.page}`;
    const firstRow = (state.page - 1) * PAGE_SIZE + 1;
    const lastRow = firstRow + data.rows.length - 1;
    $("result-summary").textContent = data.rows.length
      ? `Showing ${firstRow}–${lastRow} rows`
      : "No matching records";
    $("last-refreshed").textContent =
      `Last refreshed ${new Date(data.refreshed_at).toISOString().replace("T", " ").slice(0, 19)} UTC`;
    notice(
      data.rows.length
        ? ""
        : "No records match this page and your filters. Try the previous page or reset filters.",
    );
  } catch (error) {
    if (revision === state.revision) {
      notice(`${error.message} Displayed records may be stale.`, true);
    }
  } finally {
    state.busy = false;
    $("refresh").querySelector(".button-label").textContent = "Refresh";
    $("refresh").setAttribute("aria-label", "Refresh");
    $("previous").disabled = state.page === 1;
    $("next").disabled = !state.hasMore;
    if (state.pending) {
      state.pending = false;
      loadPage();
    }
  }
}

$("relation-search").addEventListener("input", renderRelations);
$("clear-filters").addEventListener("click", () => {
  state.headerFilters = {};
  applyHeaderFilters();
});
$("clear-sort").addEventListener("click", () => {
  state.sort = null;
  state.direction = "desc";
  state.page = 1;
  state.revision++;
  updateTitles();
  loadPage();
});
$("previous").addEventListener("click", () => {
  if (state.page > 1) {
    state.page--;
    state.revision++;
    loadPage();
  }
});
$("next").addEventListener("click", () => {
  state.page++;
  state.revision++;
  loadPage();
});
function updateUtcClock() {
  const now = new Date().toISOString();
  const clock = $("utc-time");
  clock.dateTime = now;
  clock.textContent = now.replace("T", " ").slice(0, 19);
}
function updateRefreshStatus() {
  let status;
  if (state.paused) status = "Updates paused";
  else if (document.hidden) status = "Paused while hidden";
  else if (refreshing) status = "Refreshing…";
  else {
    const remaining = Math.max(
      0,
      Math.ceil((nextRefreshAt - Date.now()) / 1000),
    );
    status = `Refresh in ${remaining}s`;
  }
  const label = $("refresh-status");
  if (label.textContent !== status) label.textContent = status;
}
$("pause").addEventListener("click", () => {
  state.paused = !state.paused;
  const label = state.paused ? "Resume updates" : "Pause updates";
  $("pause").querySelector(".button-label").textContent = label;
  $("pause").querySelector(".button-icon").textContent = state.paused ? "▶" : "⏸";
  $("pause").setAttribute("aria-label", label);
  $("pause").title = label;
  $("pause").setAttribute("aria-pressed", String(state.paused));
  if (!state.paused) nextRefreshAt = Date.now() + REFRESH_INTERVAL;
  updateRefreshStatus();
});
async function refresh() {
  if (state.busy || refreshing) return;
  refreshing = true;
  updateRefreshStatus();
  try {
    await discover();
    await loadPage();
  } catch (error) {
    notice(`${error.message} Displayed records may be stale.`, true);
  } finally {
    refreshing = false;
    nextRefreshAt = Date.now() + REFRESH_INTERVAL;
    updateRefreshStatus();
  }
}
$("refresh").addEventListener("click", refresh);
setInterval(() => {
  updateUtcClock();
  updateRefreshStatus();
  if (!state.paused && !document.hidden && Date.now() >= nextRefreshAt)
    refresh();
}, 1000);
document.addEventListener("visibilitychange", () => {
  updateUtcClock();
  updateRefreshStatus();
  if (!document.hidden && !state.paused) refresh();
});
updateUtcClock();
updateRefreshStatus();
gridReady.then(discover).catch((error) => notice(error.message, true));
