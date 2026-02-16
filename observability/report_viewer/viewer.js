const $ = (sel) => document.querySelector(sel);

async function fetchJson(url) {
  const res = await fetch(url, { credentials: "same-origin" });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`HTTP ${res.status}: ${txt || res.statusText}`);
  }
  return await res.json();
}

function fmtMs(v) {
  if (v === null || v === undefined) return "";
  const n = Number(v);
  if (!Number.isFinite(n)) return "";
  if (n >= 1000) return `${(n / 1000).toFixed(2)}s`;
  return `${n.toFixed(1)}ms`;
}

function esc(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function jsonPretty(obj) {
  return JSON.stringify(obj, null, 2);
}

function pick(obj, keys) {
  for (const k of keys) {
    if (obj && Object.prototype.hasOwnProperty.call(obj, k) && obj[k] !== undefined) return obj[k];
  }
  return undefined;
}

function shortToolCallId(tcid) {
  const s = String(tcid || "");
  if (!s) return "";
  // Our tool_call_id can carry a suffix after '+...' (e.g. base64-ish). For graph readability we
  // prefer the stable prefix: call_xxx__thought__YYY
  const i = s.indexOf("+");
  return i >= 0 ? s.slice(0, i) : s;
}

function nodeLabel(n) {
  if (!n) return "";
  if (n.type === "agent") return n.agent_id || n.id;
  if (n.type === "tool_call") {
    const tcid = shortToolCallId(n.tool_call_id || "");
    return tcid || n.tool_call_id || n.id;
  }
  if (n.type === "batch") return n.batch_id || n.id;
  return n.id;
}

function nodeSearchText(n) {
  if (!n) return "";
  if (n.type === "agent") {
    const roster = n.roster || {};
    const meta = roster.metadata || {};
    return `${n.agent_id || ""} ${roster.display_name || ""} ${roster.worker_target || ""} ${JSON.stringify(roster.tags || [])} ${JSON.stringify(meta || {})}`.toLowerCase();
  }
  if (n.type === "tool_call") {
    return `${shortToolCallId(n.tool_call_id || "")} ${n.tool_name || ""} ${n.caller_agent_id || ""} ${n.caller_step_id || ""} ${n.trace_id || ""}`.toLowerCase();
  }
  if (n.type === "batch") {
    return `${n.batch_id || ""} ${n.parent_agent_id || ""} ${n.parent_tool_call_id || ""} ${n.status || ""} ${JSON.stringify(n.metadata || {})}`.toLowerCase();
  }
  return `${n.id || ""} ${JSON.stringify(n)}`.toLowerCase();
}

class GraphView {
  constructor(canvas) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d");
    this.dpr = window.devicePixelRatio || 1;
    this.resize();
    window.addEventListener("resize", () => this.resize());

    this.nodes = [];
    this.edges = [];
    this.nodeById = new Map();

    this.edgeFilter = "all";
    this.search = "";

    this.camera = { x: 0, y: 0, z: 1 };
    this.drag = { active: false, mode: "pan", node: null, px: 0, py: 0 };

    this.onSelect = null;

    canvas.addEventListener("mousedown", (e) => this.onDown(e));
    window.addEventListener("mousemove", (e) => this.onMove(e));
    window.addEventListener("mouseup", () => this.onUp());
    canvas.addEventListener("wheel", (e) => this.onWheel(e), { passive: false });
    canvas.addEventListener("dblclick", () => this.fit());

    this._tick = this._tick.bind(this);
    requestAnimationFrame(this._tick);
  }

  resize() {
    const rect = this.canvas.getBoundingClientRect();
    const w = Math.max(320, Math.floor(rect.width));
    const h = Math.max(320, Math.floor(rect.height));
    this.canvas.width = Math.floor(w * this.dpr);
    this.canvas.height = Math.floor(h * this.dpr);
    this.canvas.style.width = `${w}px`;
    this.canvas.style.height = `${h}px`;
  }

  setData(nodes, edges) {
    this.nodes = (nodes || []).map((n) => ({ ...n }));
    this.edges = (edges || []).map((e) => ({ ...e }));
    this.nodeById = new Map(this.nodes.map((n) => [n.id, n]));

    for (const n of this.nodes) {
      n.x = 0;
      n.y = 0;
      n.pinned = false;
    }
    this.layoutStatic();
    this.fit();
  }

  fit() {
    this._fitNow();
  }

  _fitNow() {
    const W = this.canvas.width / this.dpr;
    const H = this.canvas.height / this.dpr;
    if (!this.nodes.length) {
      this.camera = { x: W * 0.5, y: H * 0.5, z: 1 };
      return;
    }
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    for (const n of this.nodes) {
      minX = Math.min(minX, n.x);
      minY = Math.min(minY, n.y);
      maxX = Math.max(maxX, n.x);
      maxY = Math.max(maxY, n.y);
    }
    const dx = Math.max(1, maxX - minX);
    const dy = Math.max(1, maxY - minY);
    const pad = 90; // padding in screen px for "always see everything"
    const availW = Math.max(40, W - pad * 2);
    const availH = Math.max(40, H - pad * 2);
    const scale = Math.min(availW / dx, availH / dy) * 0.98;
    this.camera.z = Math.max(0.12, Math.min(3.0, scale));
    this.camera.x = (minX + maxX) * 0.5;
    this.camera.y = (minY + maxY) * 0.5;
  }

  zoomBy(factor, px, py) {
    const W = this.canvas.width / this.dpr;
    const H = this.canvas.height / this.dpr;
    const cx = px === undefined ? W * 0.5 : px;
    const cy = py === undefined ? H * 0.5 : py;
    const before = this.screenToWorld(cx, cy);
    this.camera.z = Math.max(0.12, Math.min(3.0, this.camera.z * factor));
    const after = this.screenToWorld(cx, cy);
    this.camera.x += before.x - after.x;
    this.camera.y += before.y - after.y;
  }

  setEdgeFilter(v) { this.edgeFilter = v || "all"; }
  setSearch(v) { this.search = (v || "").trim().toLowerCase(); }

  layoutStatic() {
    const W = this.canvas.width / this.dpr;
    const H = this.canvas.height / this.dpr;
    if (!this.nodes.length) return;

    // Classify agents into "parents" vs "children" to reflect:
    // parent_agent -> tool_call -> batch -> child_agents
    const childAgents = new Set();
    for (const e of this.edges || []) {
      if (e && e.type === "batch_contains_agent" && typeof e.to === "string") childAgents.add(e.to);
    }

    const groups = {
      agent_parent: [],
      agent_child: [],
      batch: [],
      tool_call: [],
      other: [],
    };
    for (const n of this.nodes) {
      if (n.type === "agent") {
        if (childAgents.has(n.id)) groups.agent_child.push(n);
        else groups.agent_parent.push(n);
      }
      else if (n.type === "batch") groups.batch.push(n);
      else if (n.type === "tool_call") groups.tool_call.push(n);
      else groups.other.push(n);
    }

    const padY = 70;
    const rowH = 32;
    const maxRows = Math.max(1, Math.floor((H - padY * 2) / rowH));
    const colSpan = 92;

    const placeGroup = (nodes, baseX) => {
      nodes.sort((a, b) => nodeLabel(a).localeCompare(nodeLabel(b)));
      const cols = Math.max(1, Math.ceil(nodes.length / maxRows));
      for (let i = 0; i < nodes.length; i++) {
        const col = Math.floor(i / maxRows);
        const row = i % maxRows;
        const x = baseX + (col - (cols - 1) / 2) * colSpan;
        const y = padY + row * rowH;
        nodes[i].x = x;
        nodes[i].y = y;
      }
    };

    // Four-lane layout: parent agents -> tool calls -> batches -> child agents
    placeGroup(groups.agent_parent, W * 0.16);
    placeGroup(groups.tool_call, W * 0.42);
    placeGroup(groups.batch, W * 0.66);
    placeGroup(groups.agent_child, W * 0.88);

    if (groups.other.length) {
      placeGroup(groups.other, W * 0.50);
      // Nudge "other" below batches to avoid overlap.
      const bump = Math.min(240, Math.max(80, groups.batch.length * rowH * 0.35));
      for (const n of groups.other) n.y += bump;
    }
  }

  screenToWorld(px, py) {
    const W = this.canvas.width / this.dpr;
    const H = this.canvas.height / this.dpr;
    const x = (px - W * 0.5) / this.camera.z + this.camera.x;
    const y = (py - H * 0.5) / this.camera.z + this.camera.y;
    return { x, y };
  }

  worldToScreen(x, y) {
    const W = this.canvas.width / this.dpr;
    const H = this.canvas.height / this.dpr;
    return {
      x: (x - this.camera.x) * this.camera.z + W * 0.5,
      y: (y - this.camera.y) * this.camera.z + H * 0.5,
    };
  }

  hitNode(px, py) {
    const w = this.screenToWorld(px, py);
    const r = 14 / this.camera.z;
    let best = null;
    let bestD = Infinity;
    for (const n of this.nodes) {
      const dx = n.x - w.x;
      const dy = n.y - w.y;
      const d2 = dx * dx + dy * dy;
      if (d2 < r * r && d2 < bestD) { best = n; bestD = d2; }
    }
    return best;
  }

  onDown(e) {
    const rect = this.canvas.getBoundingClientRect();
    const px = e.clientX - rect.left;
    const py = e.clientY - rect.top;
    const hit = this.hitNode(px, py);
    this.drag.active = true;
    this.drag.px = px;
    this.drag.py = py;
    if (hit) {
      this.drag.mode = "node";
      this.drag.node = hit;
      hit.pinned = true;
      const w = this.screenToWorld(px, py);
      hit.x = w.x;
      hit.y = w.y;
    } else {
      this.drag.mode = "pan";
      this.drag.node = null;
    }
  }

  onMove(e) {
    if (!this.drag.active) return;
    const rect = this.canvas.getBoundingClientRect();
    const px = e.clientX - rect.left;
    const py = e.clientY - rect.top;
    if (this.drag.mode === "node" && this.drag.node) {
      const w = this.screenToWorld(px, py);
      this.drag.node.x = w.x;
      this.drag.node.y = w.y;
    } else if (this.drag.mode === "pan") {
      const dx = (px - this.drag.px) / this.camera.z;
      const dy = (py - this.drag.py) / this.camera.z;
      this.camera.x -= dx;
      this.camera.y -= dy;
      this.drag.px = px;
      this.drag.py = py;
    }
  }

  onUp() {
    if (!this.drag.active) return;
    const wasNode = this.drag.mode === "node" && this.drag.node;
    this.drag.active = false;
    this.drag.mode = "pan";
    if (wasNode) return;
    // click selection if mouse didn't move much
    // (we approximate; selection is handled on mouseup without movement threshold here)
  }

  onWheel(e) {
    e.preventDefault();
    const rect = this.canvas.getBoundingClientRect();
    const px = e.clientX - rect.left;
    const py = e.clientY - rect.top;
    const before = this.screenToWorld(px, py);
    const s = Math.exp(-e.deltaY * 0.0012);
    this.camera.z = Math.max(0.12, Math.min(3.0, this.camera.z * s));
    const after = this.screenToWorld(px, py);
    this.camera.x += before.x - after.x;
    this.camera.y += before.y - after.y;
  }

  _filteredEdges() {
    const f = this.edgeFilter;
    if (!f || f === "all") return this.edges;
    return this.edges.filter((e) => e.type === f);
  }

  _tick() {
    this.draw();
    requestAnimationFrame(this._tick);
  }

  draw() {
    const ctx = this.ctx;
    const W = this.canvas.width / this.dpr;
    const H = this.canvas.height / this.dpr;
    ctx.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    ctx.clearRect(0, 0, W, H);

    const edges = this._filteredEdges();
    const byId = this.nodeById;

    // edge pass
    ctx.lineWidth = 1;
    for (const e of edges) {
      const s = byId.get(e.from);
      const t = byId.get(e.to);
      if (!s || !t) continue;
      const a = this.worldToScreen(s.x, s.y);
      const b = this.worldToScreen(t.x, t.y);
      ctx.strokeStyle = edgeColor(e.type);
      ctx.beginPath();
      ctx.moveTo(a.x, a.y);
      ctx.lineTo(b.x, b.y);
      ctx.stroke();
    }

    // nodes pass
    const q = this.search;
    for (const n of this.nodes) {
      const p = this.worldToScreen(n.x, n.y);
      const r = nodeRadius(n.type);
      const label = nodeLabel(n);
      const hits = !q || nodeSearchText(n).includes(q);
      ctx.globalAlpha = hits ? 1.0 : 0.20;
      ctx.fillStyle = nodeFill(n.type);
      ctx.strokeStyle = "rgba(0,0,0,.24)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
      ctx.fill();
      ctx.stroke();

      if (this.camera.z > 0.55) {
        ctx.globalAlpha = hits ? 0.92 : 0.12;
        ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
        ctx.fillStyle = "rgba(0,0,0,.78)";
        ctx.fillText(trunc(label, 28), p.x + r + 6, p.y + 4);
      }
      ctx.globalAlpha = 1.0;
    }
  }
}

function trunc(s, n) {
  const t = String(s || "");
  if (t.length <= n) return t;
  return t.slice(0, n - 1) + "…";
}

function nodeRadius(type) {
  if (type === "agent") return 12;
  if (type === "batch") return 11;
  if (type === "tool_call") return 10;
  return 10;
}

function nodeFill(type) {
  if (type === "agent") return "rgba(14,165,164,.32)";
  if (type === "batch") return "rgba(255,107,53,.22)";
  if (type === "tool_call") return "rgba(27,27,31,.16)";
  return "rgba(27,27,31,.16)";
}

function edgeColor(type) {
  if (type === "agent_creates_agent") return "rgba(14,165,164,.55)";
  if (type === "agent_calls_tool") return "rgba(27,27,31,.45)";
  if (type === "tool_call_creates_batch") return "rgba(255,107,53,.46)";
  if (type === "agent_creates_batch") return "rgba(255,107,53,.55)";
  if (type === "batch_contains_agent") return "rgba(255,107,53,.38)";
  return "rgba(0,0,0,.28)";
}

function renderSummary(report) {
  const s = report.summary || {};
  const range = report.time_range || {};
  $("#summarySub").textContent =
    `project=${report.project_id || "?"}  since=${range.since || "-∞"}  until=${range.until || "+∞"}`;

  const items = [
    ["Agents", s.agent_count, "teal"],
    ["Identity Edges", s.identity_edge_count, "teal"],
    ["Batches", s.batch_count, "orange"],
    ["Batch Tasks", s.batch_task_count, "orange"],
    ["Steps", s.step_count, "gray"],
    ["Tool Calls", s.tool_call_count, "gray"],
    ["Trace IDs", s.trace_id_count, "gray"],
  ];
  $("#summary").innerHTML = items
    .map(([k, v, c]) => {
      const cls = c === "orange" ? "orange" : c === "gray" ? "gray" : "";
      return `<span class="pill"><span class="dot ${cls}"></span>${esc(k)}: <b>${esc(v ?? 0)}</b></span>`;
    })
    .join("");
}

function renderLlmTimeline(report) {
  const otel = report.otel || {};
  const idx = otel.index_by_trace_id || {};
  const rows = [];
  for (const [tid, v] of Object.entries(idx)) {
    const llm = (v && v.llm_calls) || [];
    for (const c of llm) rows.push({ trace_id: tid, ...c });
  }
  rows.sort((a, b) => String(a.start).localeCompare(String(b.start)));

  const html = `
    <table class="tbl">
      <thead>
        <tr>
          <th>start</th>
          <th>duration</th>
          <th>agent</th>
          <th>turn</th>
          <th>step</th>
          <th>model</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .slice(0, 800)
          .map((r, i) => `
            <tr class="clickable" data-llm-idx="${esc(i)}">
              <td class="mono">${esc(r.start || "")}</td>
              <td>${esc(fmtMs(r.duration_ms))}</td>
              <td class="mono">${esc(r.agent_id || "")}</td>
              <td class="mono">${esc(r.agent_turn_id || "")}</td>
              <td class="mono">${esc(r.step_id || "")}</td>
              <td class="mono">${esc(r.model || "")}</td>
            </tr>
          `)
          .join("")}
      </tbody>
    </table>
    <div class="muted" style="margin-top:8px;">Showing ${Math.min(rows.length, 800)} / ${rows.length} rows</div>
  `;
  $("#llmTimeline").innerHTML = html;

  // Expose for click binding in main() without re-parsing DOM cells.
  $("#llmTimeline").dataset.llmRows = JSON.stringify(rows.slice(0, 800));
}

function renderToolTable(report, onRowClick) {
  const tools = (report.tools && report.tools.tool_calls) || [];
  const html = `
    <table class="tbl">
      <thead>
        <tr>
          <th>tool</th>
          <th>tool_call_id</th>
          <th>caller</th>
          <th>agent_exec</th>
          <th>tool_svc</th>
          <th>http_total</th>
          <th>cards</th>
        </tr>
      </thead>
      <tbody>
        ${tools
          .slice(0, 1200)
          .map((t) => {
            const ot = t.otel || {};
            const cards = (t.card_ids && t.card_ids.all) || [];
            return `
              <tr class="clickable" data-tcid="${esc(t.tool_call_id)}">
                <td class="mono">${esc(t.tool_name || "")}</td>
                <td class="mono">${esc(t.tool_call_id || "")}</td>
                <td class="mono">${esc(t.caller_agent_id || "")}</td>
                <td>${esc(fmtMs(ot.agent_tool_execute_ms))}</td>
                <td>${esc(fmtMs(ot.tool_service_handle_ms))}</td>
                <td>${esc(fmtMs(ot.tool_http_total_ms))}</td>
                <td class="muted">${esc(cards.length)}</td>
              </tr>
            `;
          })
          .join("")}
      </tbody>
    </table>
    <div class="muted" style="margin-top:8px;">Showing ${Math.min(tools.length, 1200)} / ${tools.length} rows</div>
  `;
  $("#toolTable").innerHTML = html;
  $("#toolTable").querySelectorAll("tbody tr").forEach((tr) => {
    tr.addEventListener("click", () => onRowClick && onRowClick(tr.dataset.tcid));
  });
}

function renderCards(report, query) {
  const cardsById = (report.cards && report.cards.cards_by_id) || {};
  const ids = Object.keys(cardsById);
  const q = (query || "").trim().toLowerCase();
  const keep = (id, c) => {
    if (!q) return true;
    const meta = (c && c.metadata && typeof c.metadata === "object") ? c.metadata : {};
    const stepId = meta.step_id || "";
    const turnId = meta.agent_turn_id || "";
    const traceId = meta.trace_id || "";
    const parentStepId = meta.parent_step_id || "";
    const t = `${id} ${c.type || ""} ${c.tool_call_id || ""} ${(c.author_id || "")}`.toLowerCase();
    const t2 = `${t} ${stepId} ${turnId} ${traceId} ${parentStepId}`.toLowerCase();
    return t2.includes(q);
  };

  const list = [];
  for (const id of ids) {
    const c = cardsById[id];
    if (!c || !keep(id, c)) continue;
    list.push([id, c]);
  }
  list.sort((a, b) => String(a[0]).localeCompare(String(b[0])));

  const html = list.slice(0, 120).map(([id, c]) => {
    const typ = c.type || "unknown";
    const tcid = c.tool_call_id || "";
    const meta = c.metadata || {};
    const stepId = (meta && meta.step_id) || "";
    const turnId = (meta && meta.agent_turn_id) || "";
    const body = c.content ? jsonPretty(c.content) : "";
    return `
      <div class="card">
        <div class="head">
          <div class="id mono">${esc(id)}</div>
          <span class="badge">${esc(typ)}</span>
        </div>
        <div class="meta">
          ${tcid ? `<span class="mono">tool_call_id=${esc(tcid)}</span>` : ""}
          ${turnId ? `<span class="mono">turn=${esc(turnId)}</span>` : ""}
          ${stepId ? `<span class="mono">step=${esc(stepId)}</span>` : ""}
          ${c.author_id ? `<span class="mono">author=${esc(c.author_id)}</span>` : ""}
        </div>
        <details>
          <summary>content</summary>
          <pre class="code" style="max-height:220px;">${esc(body)}</pre>
        </details>
      </div>
    `;
  }).join("");

  $("#cards").innerHTML = html || `<div class="muted">No cards match.</div>`;
}

function wireDrop(loadText) {
  const zone = $("#dropZone");
  const onDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    zone.style.outline = "3px solid rgba(14,165,164,.35)";
    zone.style.outlineOffset = "-6px";
  };
  const onLeave = (e) => {
    e.preventDefault();
    e.stopPropagation();
    zone.style.outline = "";
    zone.style.outlineOffset = "";
  };
  document.addEventListener("dragover", onDrag);
  document.addEventListener("drop", async (e) => {
    onLeave(e);
    const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
    if (!f) return;
    const txt = await f.text();
    loadText(txt, f.name);
  });
  document.addEventListener("dragleave", onLeave);
}

function exampleReport() {
  return {
    schema_version: "cg.project_report.v1",
    generated_at: new Date().toISOString(),
    project_id: "proj_example",
    time_range: { since: null, until: null },
    summary: { agent_count: 3, identity_edge_count: 2, batch_count: 1, batch_task_count: 2, step_count: 4, tool_call_count: 2, trace_id_count: 3 },
    tools: {
      tool_calls: [
        { tool_call_id: "tc_1", tool_name: "fork_join", caller_agent_id: "agent_a", otel: { agent_tool_execute_ms: 12.1, tool_service_handle_ms: 45.2, tool_http_total_ms: null }, card_ids: { all: [] } },
        { tool_call_id: "tc_2", tool_name: "jina_search", caller_agent_id: "agent_b", otel: { agent_tool_execute_ms: 99.1, tool_service_handle_ms: 110.5, tool_http_total_ms: 80.0 }, card_ids: { all: [] } },
      ],
    },
    cards: { cards_by_id: {} },
    graph: {
      nodes: [
        { id: "agent:agent_a", type: "agent", agent_id: "agent_a" },
        { id: "agent:agent_b", type: "agent", agent_id: "agent_b" },
        { id: "agent:agent_c", type: "agent", agent_id: "agent_c" },
        { id: "batch:batch_1", type: "batch", batch_id: "batch_1", parent_tool_call_id: "tc_1" },
        { id: "tool_call:tc_1", type: "tool_call", tool_call_id: "tc_1", tool_name: "fork_join" },
        { id: "tool_call:tc_2", type: "tool_call", tool_call_id: "tc_2", tool_name: "jina_search" },
      ],
      edges: [
        { id: "e1", type: "agent_creates_agent", from: "agent:agent_a", to: "agent:agent_b" },
        { id: "e2", type: "agent_creates_agent", from: "agent:agent_a", to: "agent:agent_c" },
        { id: "e3b", type: "tool_call_creates_batch", from: "tool_call:tc_1", to: "batch:batch_1" },
        { id: "e4", type: "batch_contains_agent", from: "batch:batch_1", to: "agent:agent_b" },
        { id: "e5", type: "batch_contains_agent", from: "batch:batch_1", to: "agent:agent_c" },
        { id: "e6", type: "agent_calls_tool", from: "agent:agent_a", to: "tool_call:tc_1" },
        { id: "e7", type: "agent_calls_tool", from: "agent:agent_b", to: "tool_call:tc_2" },
      ],
    },
    otel: {
      index_by_trace_id: {
        "trace_1": { span_count: 42, llm_calls: [{ start: new Date().toISOString(), duration_ms: 1234, agent_id: "agent_b", agent_turn_id: "turn_1", step_id: "step_1", model: "gpt-x", stream: true }] },
      },
    },
  };
}

function main() {
  const g = new GraphView($("#graph"));

  let report = null;

  const setSelection = (kind, obj) => {
    const meta = $("#selectionMeta");
    const pre = $("#selectionJson");
    const kv = [];
    kv.push(["type", kind]);
    if (obj && obj.id) kv.push(["id", obj.id]);
    if (obj && obj.type) kv.push(["edge_type", obj.type]);
    if (obj && obj.agent_id) kv.push(["agent_id", obj.agent_id]);
    if (obj && obj.tool_call_id) kv.push(["tool_call_id", obj.tool_call_id]);
    if (obj && obj.batch_id) kv.push(["batch_id", obj.batch_id]);
    meta.innerHTML = kv.map(([k, v]) => `<div class="row"><div class="k">${esc(k)}</div><div class="v mono">${esc(v)}</div></div>`).join("");
    pre.textContent = obj ? jsonPretty(obj) : "";
  };

  const loadReport = (obj) => {
    report = obj;
    renderSummary(report);
    renderLlmTimeline(report);
    renderToolTable(report, (tcid) => {
      if (!tcid) return;

      // Selection = tool call + call/result cards
      const node = (report.graph.nodes || []).find((x) => x.type === "tool_call" && x.tool_call_id === tcid);
      const tools = (report.tools && report.tools.tool_calls) || [];
      const toolRow = tools.find((t) => t.tool_call_id === tcid) || null;
      const cardsById = (report.cards && report.cards.cards_by_id) || {};
      const cards = Object.values(cardsById).filter((c) => c && c.tool_call_id === tcid);
      const callCard = cards.find((c) => c.type === "tool.call") || null;
      const resultCards = cards.filter((c) => c.type === "tool.result");
      const otherCards = cards.filter((c) => c.type !== "tool.call" && c.type !== "tool.result");

      $("#cardSearch").value = tcid;
      renderCards(report, tcid);
      setSelection("tool_call", {
        id: `tool_call:${tcid}`,
        type: "tool_call",
        tool_call_id: tcid,
        node,
        tool: toolRow,
        request: callCard ? callCard.content : (node ? { tool_name: node.tool_name, arguments: node.call_args } : null),
        results: resultCards.map((c) => c.content),
        cards: {
          call_card_id: callCard ? callCard.card_id : null,
          result_card_ids: resultCards.map((c) => c.card_id).filter(Boolean),
          other_card_ids: otherCards.map((c) => c.card_id).filter(Boolean),
        },
        otel: (node && node.otel) || (toolRow && toolRow.otel) || null,
      });
    });
    renderCards(report, $("#cardSearch").value);

    const nodes = (report.graph && report.graph.nodes) || [];
    const edges = (report.graph && report.graph.edges) || [];
    g.setData(nodes, edges);
    setSelection("none", null);

    // Bind LLM row clicks (Timeline renders its own DOM).
    $("#llmTimeline").querySelectorAll("tbody tr").forEach((tr) => {
      tr.addEventListener("click", () => {
        if (!report) return;
        const packed = $("#llmTimeline").dataset.llmRows || "[]";
        let rows = [];
        try { rows = JSON.parse(packed); } catch { rows = []; }
        const idx = Number(tr.dataset.llmIdx);
        const r = Number.isFinite(idx) ? rows[idx] : null;
        if (!r) return;

        // Filter cards to the same step/turn; this requires the report to include those in card.metadata.
        const q = r.step_id || r.agent_turn_id || r.agent_id || "";
        $("#cardSearch").value = q;
        renderCards(report, q);
        setSelection("llm_call", {
          id: `llm_call:${r.trace_id || ""}:${r.start || ""}`,
          type: "llm_call",
          ...r,
        });
      });
    });
  };

  const loadText = (txt, name) => {
    let obj = null;
    try {
      obj = JSON.parse(txt);
    } catch (e) {
      alert("Invalid JSON: " + e);
      return;
    }
    loadReport(obj);
    if (name) $("#summarySub").textContent += `  file=${name}`;
  };

  const loadProjectList = async () => {
    const sel = $("#projectSelect");
    if (!sel) return;
    try {
      const rows = await fetchJson("/projects?limit=500");
      const items = Array.isArray(rows) ? rows : [];
      const opts = [`<option value="">Select Project</option>`];
      for (const p of items) {
        if (!p || !p.project_id) continue;
        const label = p.title ? `${p.project_id} (${p.title})` : p.project_id;
        opts.push(`<option value="${esc(p.project_id)}">${esc(label)}</option>`);
      }
      sel.innerHTML = opts.join("");
    } catch (e) {
      console.error(e);
    }
  };

  const loadFromApi = async () => {
    const sel = $("#projectSelect");
    const btn = $("#btnLoadApi");
    const projectId = (sel && sel.value) ? String(sel.value).trim() : "";
    if (!projectId) {
      alert("Please select a project first.");
      return;
    }
    const old = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Loading...";
    try {
      const report = await fetchJson(`/projects/${encodeURIComponent(projectId)}/observability/report`);
      loadReport(report);
    } catch (e) {
      alert(`Load project failed: ${e}`);
    } finally {
      btn.disabled = false;
      btn.textContent = old;
    }
  };

  $("#fileInput").addEventListener("change", async (e) => {
    const f = e.target.files && e.target.files[0];
    if (!f) return;
    loadText(await f.text(), f.name);
    e.target.value = "";
  });

  $("#btnDemo").addEventListener("click", () => loadReport(exampleReport()));
  $("#btnLoadApi").addEventListener("click", () => { void loadFromApi(); });
  $("#btnFit").addEventListener("click", () => g.fit());
  $("#btnZoomIn").addEventListener("click", () => g.zoomBy(1.2));
  $("#btnZoomOut").addEventListener("click", () => g.zoomBy(1 / 1.2));
  $("#edgeFilter").addEventListener("change", (e) => g.setEdgeFilter(e.target.value));
  $("#search").addEventListener("input", (e) => g.setSearch(e.target.value));
  $("#cardSearch").addEventListener("input", (e) => report && renderCards(report, e.target.value));

  // Selection by click: we map click to node hit (edges are not hit-tested).
  $("#graph").addEventListener("click", (e) => {
    const rect = $("#graph").getBoundingClientRect();
    const px = e.clientX - rect.left;
    const py = e.clientY - rect.top;
    const hit = g.hitNode(px, py);
    if (!hit) return;
    setSelection("node", hit);
  });

  wireDrop(loadText);
  void loadProjectList();
}

main();
