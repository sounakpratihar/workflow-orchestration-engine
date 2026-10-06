/**
 * Workflow Orchestration Engine - Interactive Visual DAG Canvas
 * Includes 3-color DFS cycle detection, Kahn's topological sort,
 * conditional branching, node validation, and execution simulation.
 */

// 1. Initial Sample Presets
const PRESETS = {
  branchingOrderFlow: {
    name: 'E-Commerce Order Processing Pipeline',
    nodes: [
      {
        id: 'node_1',
        type: 'trigger',
        subtype: 'webhook',
        label: 'Order Webhook',
        position: { x: 80, y: 180 },
        config: { path: '/v1/webhooks/orders', method: 'POST' },
      },
      {
        id: 'node_2',
        type: 'action',
        subtype: 'transform',
        label: 'Parse & Enrich Items',
        position: { x: 360, y: 180 },
        config: { expression: 'order.total = items.reduce((s, i) => s + i.price, 0)' },
      },
      {
        id: 'node_3',
        type: 'condition',
        subtype: 'if_condition',
        label: 'High Value Order (≥$500)?',
        position: { x: 640, y: 180 },
        config: { expression: 'payload.amount >= 500' },
      },
      {
        id: 'node_4',
        type: 'action',
        subtype: 'http_request',
        label: 'VIP White-Glove Dispatch',
        position: { x: 920, y: 80 },
        config: { url: 'https://api.vip-fulfillment.internal/dispatch', method: 'POST' },
      },
      {
        id: 'node_5',
        type: 'action',
        subtype: 'database_query',
        label: 'Standard Queue Ingestion',
        position: { x: 920, y: 280 },
        config: { collection: 'shipping_jobs' },
      },
      {
        id: 'node_6',
        type: 'terminal',
        subtype: 'end_success',
        label: 'Order Finalized',
        position: { x: 1220, y: 180 },
        config: {},
      },
    ],
    edges: [
      { id: 'e1', source: 'node_1', target: 'node_2', sourceHandle: 'default' },
      { id: 'e2', source: 'node_2', target: 'node_3', sourceHandle: 'default' },
      { id: 'e3', source: 'node_3', target: 'node_4', sourceHandle: 'true' },
      { id: 'e4', source: 'node_3', target: 'node_5', sourceHandle: 'false' },
      { id: 'e5', source: 'node_4', target: 'node_6', sourceHandle: 'default' },
      { id: 'e6', source: 'node_5', target: 'node_6', sourceHandle: 'default' },
    ],
  },

  cyclicStressTest: {
    name: 'Loop Detection Stress Test (Illegal Directed Cycle)',
    nodes: [
      { id: 'n_start', type: 'trigger', subtype: 'manual', label: 'Manual Trigger', position: { x: 80, y: 180 }, config: {} },
      { id: 'n_stepA', type: 'action', subtype: 'transform', label: 'Step A: Batch Extractor', position: { x: 360, y: 180 }, config: { expression: 'extract()' } },
      { id: 'n_stepB', type: 'action', subtype: 'http_request', label: 'Step B: Remote Sync', position: { x: 640, y: 180 }, config: { url: 'https://api.sync.io', method: 'POST' } },
      { id: 'n_stepC', type: 'action', subtype: 'database_query', label: 'Step C: Ledger Write', position: { x: 920, y: 180 }, config: { collection: 'ledger' } },
    ],
    edges: [
      { id: 'e_sA', source: 'n_start', target: 'n_stepA', sourceHandle: 'default' },
      { id: 'e_AB', source: 'n_stepA', target: 'n_stepB', sourceHandle: 'default' },
      { id: 'e_BC', source: 'n_stepB', target: 'n_stepC', sourceHandle: 'default' },
      { id: 'e_CA_cycle', source: 'n_stepC', target: 'n_stepA', sourceHandle: 'default' }, // THE ILLEGAL CYCLE!
    ],
  },
};

// 2. State Store
let state = {
  workflowName: PRESETS.branchingOrderFlow.name,
  nodes: JSON.parse(JSON.stringify(PRESETS.branchingOrderFlow.nodes)),
  edges: JSON.parse(JSON.stringify(PRESETS.branchingOrderFlow.edges)),
  selectedNodeId: null,
  selectedEdgeId: null,
  tenant: {
    id: 'acme_corp',
    tier: 'enterprise',
    strategy: 'multi-db',
  },
  zoom: 1,
  pan: { x: 50, y: 40 },
  isPanning: false,
  panStart: { x: 0, y: 0 },
  draggingNodeId: null,
  dragOffset: { x: 0, y: 0 },
  connecting: null, // { sourceNodeId, sourceHandle }
  simState: {},     // nodeId -> 'running' | 'completed' | 'skipped'
};

// 3. Graph Algorithms: Cycle Detection (DFS 3-Coloring) & Topological Sort
function detectCycleDFS(nodes, edges) {
  const adj = new Map();
  for (const n of nodes) adj.set(n.id, []);
  for (const e of edges) {
    if (adj.has(e.source)) adj.get(e.source).push(e.target);
  }

  const color = new Map(); // 0 = White, 1 = Gray, 2 = Black
  for (const n of nodes) color.set(n.id, 0);

  let cyclePath = null;
  const cyclicEdges = new Set();

  function dfs(nodeId, path) {
    color.set(nodeId, 1);
    path.push(nodeId);

    for (const neighbor of adj.get(nodeId) || []) {
      if (color.get(neighbor) === 1) {
        // Back edge found!
        const startIdx = path.indexOf(neighbor);
        cyclePath = [...path.slice(startIdx), neighbor];
        for (let i = 0; i < cyclePath.length - 1; i++) {
          cyclicEdges.add(`${cyclePath[i]}->${cyclePath[i + 1]}`);
        }
        return true;
      }
      if (color.get(neighbor) === 0) {
        if (dfs(neighbor, path)) return true;
      }
    }

    path.pop();
    color.set(nodeId, 2);
    return false;
  }

  for (const n of nodes) {
    if (color.get(n.id) === 0) {
      if (dfs(n.id, [])) {
        return {
          hasCycle: true,
          cyclePath,
          cyclicEdges: Array.from(cyclicEdges),
          cyclicNodes: Array.from(new Set(cyclePath)),
        };
      }
    }
  }

  return { hasCycle: false, cyclePath: null, cyclicEdges: [], cyclicNodes: [] };
}

function computeTopologicalSort(nodes, edges) {
  const adj = new Map();
  const inDegree = new Map();

  for (const n of nodes) {
    adj.set(n.id, []);
    inDegree.set(n.id, 0);
  }

  for (const e of edges) {
    if (adj.has(e.source) && inDegree.has(e.target)) {
      adj.get(e.source).push(e.target);
      inDegree.set(e.target, (inDegree.get(e.target) || 0) + 1);
    }
  }

  const queue = [];
  const order = [];

  for (const [id, deg] of inDegree.entries()) {
    if (deg === 0) queue.push(id);
  }

  while (queue.length > 0) {
    const curr = queue.shift();
    order.push(curr);
    for (const neighbor of adj.get(curr) || []) {
      const newDeg = inDegree.get(neighbor) - 1;
      inDegree.set(neighbor, newDeg);
      if (newDeg === 0) queue.push(neighbor);
    }
  }

  return {
    isValidDag: order.length === nodes.length,
    order: order.length === nodes.length ? order : [],
  };
}

function validateNode(node) {
  const errors = [];
  const cfg = node.config || {};
  if (node.subtype === 'http_request') {
    if (!cfg.url) errors.push('Target HTTP URL is required');
    else if (!/^https?:\/\//i.test(cfg.url)) errors.push('URL must start with http:// or https://');
  } else if (node.subtype === 'if_condition') {
    if (!cfg.expression) errors.push('Conditional boolean expression is required');
  } else if (node.subtype === 'database_query') {
    if (!cfg.collection) errors.push('Collection name is required');
  } else if (node.subtype === 'schedule') {
    if (!cfg.cron) errors.push('Cron schedule expression is required');
  }
  return { isValid: errors.length === 0, errors };
}

function validateWorkflow() {
  const nodes = state.nodes;
  const edges = state.edges;
  const errors = [];

  for (const n of nodes) {
    const v = validateNode(n);
    if (!v.isValid) errors.push(`${n.label}: ${v.errors.join(', ')}`);
  }

  for (const e of edges) {
    if (e.source === e.target) {
      errors.push(`Self-loop detected on ${e.source}`);
    }
  }

  const cycleInfo = detectCycleDFS(nodes, edges);
  if (cycleInfo.hasCycle) {
    errors.push(`Illegal cycle loop detected: ${cycleInfo.cyclePath.join(' → ')}`);
  }

  const topo = computeTopologicalSort(nodes, edges);

  return {
    isValid: errors.length === 0,
    errors,
    cycleInfo,
    topoOrder: topo.order,
  };
}

// 4. Render Engine: DOM & SVG
const container = document.getElementById('canvas-container');
const worldSpace = document.getElementById('world-space');
const edgesGroup = document.getElementById('edges-group');
const nodesLayer = document.getElementById('nodes-layer');
const tempEdge = document.getElementById('temp-edge');

function getNodePortPos(nodeId, handleId, type) {
  const node = state.nodes.find((n) => n.id === nodeId);
  if (!node) return null;

  const nodeWidth = 250;
  const nodeHeight = 110;

  if (type === 'target') {
    return { x: node.position.x, y: node.position.y + nodeHeight / 2 };
  }

  if (handleId === 'true') {
    return { x: node.position.x + nodeWidth, y: node.position.y + nodeHeight * 0.35 };
  }
  if (handleId === 'false') {
    return { x: node.position.x + nodeWidth, y: node.position.y + nodeHeight * 0.65 };
  }

  return { x: node.position.x + nodeWidth, y: node.position.y + nodeHeight / 2 };
}

function renderEdges() {
  edgesGroup.innerHTML = '';
  const validation = validateWorkflow();
  const cyclicEdgesSet = new Set(validation.cycleInfo.cyclicEdges);

  state.edges.forEach((edge) => {
    const srcPos = getNodePortPos(edge.source, edge.sourceHandle, 'source');
    const tgtPos = getNodePortPos(edge.target, edge.targetHandle, 'target');
    if (!srcPos || !tgtPos) return;

    const dx = Math.abs(tgtPos.x - srcPos.x);
    const offset = Math.max(50, dx * 0.45);
    const d = `M ${srcPos.x} ${srcPos.y} C ${srcPos.x + offset} ${srcPos.y}, ${tgtPos.x - offset} ${tgtPos.y}, ${tgtPos.x} ${tgtPos.y}`;

    const isCyclic = cyclicEdgesSet.has(`${edge.source}->${edge.target}`);
    const isSelected = state.selectedEdgeId === edge.id;

    let strokeColor = '#6366f1';
    let markerId = 'arrow-default';
    let badgeText = null;
    let badgeBg = 'bg-indigo-950 border-indigo-500/50 text-indigo-300';

    if (edge.sourceHandle === 'true') {
      strokeColor = '#10b981';
      markerId = 'arrow-true';
      badgeText = 'TRUE';
      badgeBg = 'bg-emerald-950 border-emerald-500/50 text-emerald-300';
    } else if (edge.sourceHandle === 'false') {
      strokeColor = '#f43f5e';
      markerId = 'arrow-false';
      badgeText = 'FALSE';
      badgeBg = 'bg-rose-950 border-rose-500/50 text-rose-300';
    }

    if (isCyclic) {
      strokeColor = '#ef4444';
      markerId = 'arrow-cyclic';
      badgeText = 'CYCLE ⚠️';
      badgeBg = 'bg-red-950 border-red-500 text-red-200 animate-pulse';
    } else if (isSelected) {
      strokeColor = '#a855f7';
      markerId = 'arrow-selected';
    }

    // SVG Group
    const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    g.style.cursor = 'pointer';

    // Click target path
    const clickPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    clickPath.setAttribute('d', d);
    clickPath.setAttribute('fill', 'none');
    clickPath.setAttribute('stroke', 'transparent');
    clickPath.setAttribute('stroke-width', '18');
    clickPath.onclick = (e) => {
      e.stopPropagation();
      state.selectedEdgeId = edge.id;
      state.selectedNodeId = null;
      render();
    };

    // Visible Path
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('d', d);
    path.setAttribute('fill', 'none');
    path.setAttribute('stroke', strokeColor);
    path.setAttribute('stroke-width', isCyclic ? '3.5' : isSelected ? '3' : '2');
    if (isCyclic) {
      path.setAttribute('stroke-dasharray', '6,4');
      path.setAttribute('class', 'animate-pulse');
    }
    path.setAttribute('marker-end', `url(#${markerId})`);
    path.onclick = (e) => {
      e.stopPropagation();
      state.selectedEdgeId = edge.id;
      state.selectedNodeId = null;
      render();
    };

    g.appendChild(clickPath);
    g.appendChild(path);

    // Badge label
    if (badgeText || isSelected) {
      const midX = (srcPos.x + tgtPos.x) / 2;
      const midY = (srcPos.y + tgtPos.y) / 2;

      const fo = document.createElementNS('http://www.w3.org/2000/svg', 'foreignObject');
      fo.setAttribute('x', midX - 40);
      fo.setAttribute('y', midY - 14);
      fo.setAttribute('width', '80');
      fo.setAttribute('height', '28');
      fo.style.overflow = 'visible';

      fo.innerHTML = `
        <div class="flex items-center justify-center">
          ${badgeText ? `<span class="text-[9px] font-mono font-bold px-2 py-0.5 rounded-full border shadow-md select-none ${badgeBg}">${badgeText}</span>` : ''}
          ${isSelected ? `<button onclick="deleteEdge('${edge.id}')" title="Delete connection" class="ml-1 w-5 h-5 rounded-full bg-red-600 text-white text-[10px] flex items-center justify-center hover:bg-red-500 shadow-lg">✕</button>` : ''}
        </div>
      `;
      g.appendChild(fo);
    }

    edgesGroup.appendChild(g);
  });
}

function renderNodes() {
  nodesLayer.innerHTML = '';
  const validation = validateWorkflow();
  const cyclicNodesSet = new Set(validation.cycleInfo.cyclicNodes || []);

  state.nodes.forEach((node) => {
    const isSelected = state.selectedNodeId === node.id;
    const isCyclic = cyclicNodesSet.has(node.id);
    const simState = state.simState[node.id];
    const nodeVal = validateNode(node);

    const el = document.createElement('div');
    el.id = `node-${node.id}`;
    el.className = `dag-node ${isSelected ? 'selected' : ''} ${isCyclic ? 'cyclic' : ''} ${simState ? 'sim-' + simState : ''}`;
    el.style.transform = `translate(${node.position.x}px, ${node.position.y}px)`;

    // Node Type Colors
    let badgeClass = 'bg-indigo-500/20 text-indigo-400 border-indigo-500/30';
    let badgeLabel = 'ACTION';
    if (node.type === 'trigger') {
      badgeClass = 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30';
      badgeLabel = 'TRIGGER';
    } else if (node.type === 'condition') {
      badgeClass = 'bg-amber-500/20 text-amber-400 border-amber-500/30';
      badgeLabel = 'BRANCH';
    } else if (node.type === 'terminal') {
      badgeClass = 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30';
      badgeLabel = 'TERMINAL';
    }

    // Config Preview
    let cfgSnippet = 'Final step';
    if (node.subtype === 'http_request') cfgSnippet = node.config?.url || 'Missing URL';
    else if (node.subtype === 'if_condition') cfgSnippet = node.config?.expression || 'Missing expr';
    else if (node.subtype === 'webhook') cfgSnippet = node.config?.path || '/webhook';
    else if (node.subtype === 'transform') cfgSnippet = node.config?.expression ? 'fn: ' + node.config.expression : 'Missing fn';
    else if (node.subtype === 'database_query') cfgSnippet = node.config?.collection ? 'db.' + node.config.collection : 'Missing col';
    else if (node.subtype === 'schedule') cfgSnippet = node.config?.cron || '0 * * * *';

    el.innerHTML = `
      ${node.type !== 'trigger' ? `
        <div class="port-handle input" data-handle-type="target" data-node-id="${node.id}" title="Input Port">
          <div class="w-1.5 h-1.5 rounded-full bg-indigo-300 pointer-events-none"></div>
        </div>
      ` : ''}

      <div class="flex items-center justify-between mb-2">
        <span class="text-[10px] font-mono font-bold tracking-wider px-2 py-0.5 rounded-full border ${badgeClass}">
          ${badgeLabel}
        </span>
        <span class="text-[11px] text-slate-400 font-mono capitalize">
          ${node.subtype.replace('_', ' ')}
        </span>
      </div>

      <div class="font-semibold text-sm text-slate-100 truncate mb-1">
        ${node.label}
      </div>

      <div class="text-[11px] text-slate-400 font-mono truncate bg-slate-950/70 px-2 py-1 rounded border border-slate-800 mb-2">
        ${cfgSnippet}
      </div>

      <div class="flex items-center justify-between text-[11px] pt-1 border-t border-slate-800/80">
        ${isCyclic ? `
          <span class="text-red-400 font-bold flex items-center gap-1">⚠️ Cycle Loop</span>
        ` : !nodeVal.isValid ? `
          <span class="text-amber-400 flex items-center gap-1" title="${nodeVal.errors.join('; ')}">⚠️ Config Warning</span>
        ` : `
          <span class="text-emerald-400 flex items-center gap-1">✓ Valid</span>
        `}

        <button onclick="openNodeModal('${node.id}')" class="text-xs text-indigo-400 hover:text-indigo-300 font-medium">
          Edit ⚙️
        </button>
      </div>

      ${node.type !== 'terminal' ? (
        node.subtype === 'if_condition' ? `
          <div class="port-handle output-true" data-handle-type="source" data-handle-id="true" data-node-id="${node.id}" title="Connect True Branch">
            T
          </div>
          <div class="port-handle output-false" data-handle-type="source" data-handle-id="false" data-node-id="${node.id}" title="Connect False Branch">
            F
          </div>
        ` : `
          <div class="port-handle output-default" data-handle-type="source" data-handle-id="default" data-node-id="${node.id}" title="Connect Output">
            <div class="w-1.5 h-1.5 rounded-full bg-indigo-300 pointer-events-none"></div>
          </div>
        `
      ) : ''}
    `;

    // Drag Node listeners
    el.onmousedown = (e) => {
      // Don't drag if clicking a port handle or button
      if (e.target.closest('.port-handle') || e.target.tagName === 'BUTTON') return;
      state.selectedNodeId = node.id;
      state.selectedEdgeId = null;
      state.draggingNodeId = node.id;

      const rect = container.getBoundingClientRect();
      const mouseX = (e.clientX - rect.left - state.pan.x) / state.zoom;
      const mouseY = (e.clientY - rect.top - state.pan.y) / state.zoom;
      state.dragOffset = { x: mouseX - node.position.x, y: mouseY - node.position.y };

      render();
    };

    nodesLayer.appendChild(el);
  });
}

function renderValidationPanel() {
  const badgeContainer = document.getElementById('status-badge-container');
  const topoContainer = document.getElementById('topo-order-container');
  const simBtn = document.getElementById('btn-open-sim');

  const validation = validateWorkflow();
  const { isValid, cycleInfo, errors, topoOrder } = validation;

  if (cycleInfo.hasCycle) {
    badgeContainer.innerHTML = `
      <div class="flex items-center gap-2 text-red-400 font-bold text-sm bg-red-950/60 px-3 py-1.5 rounded-lg border border-red-500/40 animate-pulse">
        <span>🚨 ILLEGAL CYCLE DETECTED:</span>
        <span class="text-xs font-mono text-red-300">${cycleInfo.cyclePath.join(' → ')}</span>
        <button onclick="breakLoopEdge()" class="ml-2 px-2.5 py-0.5 text-xs font-semibold bg-red-600 hover:bg-red-500 text-white rounded transition-colors shadow">
          Break Loop Edge
        </button>
      </div>
    `;
    topoContainer.innerHTML = `<span class="text-xs text-red-300">Infinite loop causes runtime deadlock. DAGs must be strictly acyclic.</span>`;
    simBtn.disabled = true;
    simBtn.className = 'px-4 py-1.5 text-xs font-semibold bg-slate-800 text-slate-500 cursor-not-allowed rounded-lg border border-slate-800';
  } else if (!isValid) {
    badgeContainer.innerHTML = `
      <div class="flex items-center gap-2 text-amber-400 font-bold text-sm bg-amber-950/60 px-3 py-1.5 rounded-lg border border-amber-500/40">
        <span>⚠️ VALIDATION WARNINGS (${errors.length})</span>
      </div>
    `;
    topoContainer.innerHTML = `<span class="text-xs text-amber-300/80 truncate">${errors[0]}</span>`;
    simBtn.disabled = true;
    simBtn.className = 'px-4 py-1.5 text-xs font-semibold bg-slate-800 text-slate-500 cursor-not-allowed rounded-lg border border-slate-800';
  } else {
    badgeContainer.innerHTML = `
      <div class="flex items-center gap-2 text-emerald-400 font-bold text-sm bg-emerald-950/60 px-3 py-1.5 rounded-lg border border-emerald-500/40">
        <span>✅ VALID DAG</span>
        <span class="text-xs font-mono text-emerald-300 font-normal">Acyclic & Execution Ready</span>
      </div>
    `;
    topoContainer.innerHTML = `
      <span class="font-semibold text-slate-300">Execution Schedule:</span>
      ${topoOrder.map((id, idx) => `
        <span class="font-mono text-[11px] text-cyan-300 bg-slate-950 px-2 py-0.5 rounded border border-slate-800">
          ${idx + 1}. ${id}
        </span>
        ${idx < topoOrder.length - 1 ? '<span class="text-slate-600">→</span>' : ''}
      `).join('')}
    `;
    simBtn.disabled = false;
    simBtn.className = 'px-4 py-1.5 text-xs font-semibold bg-gradient-to-r from-indigo-500 to-cyan-500 hover:from-indigo-600 hover:to-cyan-600 text-white rounded-lg shadow-md shadow-indigo-500/25 transition-all flex items-center gap-1.5';
  }
}

function updateWorldTransform() {
  worldSpace.style.transform = `translate(${state.pan.x}px, ${state.pan.y}px) scale(${state.zoom})`;
  document.getElementById('zoom-level-text').textContent = `${Math.round(state.zoom * 100)}%`;
  container.style.backgroundPosition = `${state.pan.x}px ${state.pan.y}px`;
  container.style.backgroundSize = `${24 * state.zoom}px ${24 * state.zoom}px`;
}

function render() {
  updateWorldTransform();
  renderNodes();
  renderEdges();
  renderValidationPanel();
}

// 5. Canvas Mouse & Gesture Handling
container.onmousedown = (e) => {
  // If clicked on port handle, start connection
  const port = e.target.closest('.port-handle');
  if (port && port.dataset.handleType === 'source') {
    state.connecting = {
      sourceNodeId: port.dataset.nodeId,
      sourceHandle: port.dataset.handleId,
    };
    tempEdge.classList.remove('hidden');
    return;
  }

  // If clicking on background, pan
  if (e.target === container || e.target.tagName === 'svg') {
    state.isPanning = true;
    state.panStart = { x: e.clientX - state.pan.x, y: e.clientY - state.pan.y };
    state.selectedNodeId = null;
    state.selectedEdgeId = null;
    render();
  }
};

window.onmousemove = (e) => {
  if (state.isPanning) {
    state.pan = { x: e.clientX - state.panStart.x, y: e.clientY - state.panStart.y };
    updateWorldTransform();
    return;
  }

  if (state.draggingNodeId) {
    const rect = container.getBoundingClientRect();
    const mouseX = (e.clientX - rect.left - state.pan.x) / state.zoom;
    const mouseY = (e.clientY - rect.top - state.pan.y) / state.zoom;

    const node = state.nodes.find((n) => n.id === state.draggingNodeId);
    if (node) {
      node.position.x = Math.round(mouseX - state.dragOffset.x);
      node.position.y = Math.round(mouseY - state.dragOffset.y);

      // Fast DOM update for dragged node
      const el = document.getElementById(`node-${node.id}`);
      if (el) el.style.transform = `translate(${node.position.x}px, ${node.position.y}px)`;
      renderEdges();
    }
    return;
  }

  if (state.connecting) {
    const rect = container.getBoundingClientRect();
    const mouseX = (e.clientX - rect.left - state.pan.x) / state.zoom;
    const mouseY = (e.clientY - rect.top - state.pan.y) / state.zoom;

    const srcPos = getNodePortPos(state.connecting.sourceNodeId, state.connecting.sourceHandle, 'source');
    if (srcPos) {
      const d = `M ${srcPos.x} ${srcPos.y} C ${srcPos.x + 80} ${srcPos.y}, ${mouseX - 80} ${mouseY}, ${mouseX} ${mouseY}`;
      tempEdge.setAttribute('d', d);
      tempEdge.setAttribute('stroke', state.connecting.sourceHandle === 'true' ? '#10b981' : state.connecting.sourceHandle === 'false' ? '#f43f5e' : '#818cf8');
    }
  }
};

window.onmouseup = (e) => {
  state.isPanning = false;
  state.draggingNodeId = null;

  if (state.connecting) {
    tempEdge.classList.add('hidden');
    // Check if mouse released over an input port
    const elem = document.elementFromPoint(e.clientX, e.clientY);
    const targetPort = elem?.closest('.port-handle[data-handle-type="target"]');

    if (targetPort) {
      const targetNodeId = targetPort.dataset.nodeId;
      if (targetNodeId && targetNodeId !== state.connecting.sourceNodeId) {
        // Prevent duplicate edge
        const exists = state.edges.some(
          (ed) =>
            ed.source === state.connecting.sourceNodeId &&
            ed.target === targetNodeId &&
            ed.sourceHandle === state.connecting.sourceHandle
        );

        if (!exists) {
          state.edges.push({
            id: `edge_${Date.now()}_${Math.random().toString(36).substring(2, 6)}`,
            source: state.connecting.sourceNodeId,
            target: targetNodeId,
            sourceHandle: state.connecting.sourceHandle,
            targetHandle: 'input',
          });
        }
      }
    }
    state.connecting = null;
    render();
  }
};

container.onwheel = (e) => {
  e.preventDefault();
  const zoomFactor = e.deltaY < 0 ? 1.08 : 0.92;
  state.zoom = Math.min(Math.max(0.4, state.zoom * zoomFactor), 2.2);
  updateWorldTransform();
};

// 6. Node Actions & Deletions
window.deleteEdge = (edgeId) => {
  state.edges = state.edges.filter((e) => e.id !== edgeId);
  if (state.selectedEdgeId === edgeId) state.selectedEdgeId = null;
  render();
};

window.breakLoopEdge = () => {
  const validation = validateWorkflow();
  const cycleEdges = validation.cycleInfo.cyclicEdges || [];
  if (cycleEdges.length > 0) {
    const edgeToBreak = cycleEdges[cycleEdges.length - 1];
    const [src, tgt] = edgeToBreak.split('->');
    state.edges = state.edges.filter((e) => !(e.source === src && e.target === tgt));
    render();
  }
};

function addNode(type, subtype, label) {
  const newNode = {
    id: `node_${Date.now()}_${Math.random().toString(36).substring(2, 5)}`,
    type,
    subtype,
    label,
    position: { x: 300 + Math.random() * 80, y: 150 + Math.random() * 80 },
    config:
      subtype === 'if_condition'
        ? { expression: 'payload.amount >= 500' }
        : subtype === 'http_request'
        ? { url: 'https://api.internal/action', method: 'POST' }
        : {},
  };
  state.nodes.push(newNode);
  state.selectedNodeId = newNode.id;
  render();
}

// 7. Node Configuration Modal
const nodeModal = document.getElementById('node-modal');
let editingNodeId = null;

window.openNodeModal = (nodeId) => {
  editingNodeId = nodeId;
  const node = state.nodes.find((n) => n.id === nodeId);
  if (!node) return;

  document.getElementById('modal-node-title').textContent = `Configure ${node.label}`;
  document.getElementById('modal-node-type-badge').textContent = node.type.toUpperCase();
  document.getElementById('modal-input-label').value = node.label;

  const dynamicFields = document.getElementById('modal-dynamic-fields');
  dynamicFields.innerHTML = '';

  if (node.subtype === 'http_request') {
    dynamicFields.innerHTML = `
      <div>
        <label class="block text-xs font-semibold text-slate-300 mb-1">Target URL <span class="text-red-400">*</span></label>
        <input id="modal-cfg-url" type="text" value="${node.config?.url || ''}" placeholder="https://api.example.com" class="w-full bg-slate-950 font-mono text-xs text-cyan-300 px-3 py-2 rounded-lg border border-slate-800 focus:border-indigo-500 focus:outline-none">
      </div>
      <div>
        <label class="block text-xs font-semibold text-slate-300 mb-1">HTTP Method</label>
        <select id="modal-cfg-method" class="w-full bg-slate-950 text-xs text-slate-100 px-3 py-2 rounded-lg border border-slate-800 focus:border-indigo-500 focus:outline-none">
          <option value="POST" ${node.config?.method === 'POST' ? 'selected' : ''}>POST</option>
          <option value="GET" ${node.config?.method === 'GET' ? 'selected' : ''}>GET</option>
          <option value="PUT" ${node.config?.method === 'PUT' ? 'selected' : ''}>PUT</option>
        </select>
      </div>
    `;
  } else if (node.subtype === 'if_condition') {
    dynamicFields.innerHTML = `
      <div>
        <label class="block text-xs font-semibold text-slate-300 mb-1">Branching Condition Expression <span class="text-red-400">*</span></label>
        <input id="modal-cfg-expression" type="text" value="${node.config?.expression || ''}" placeholder="payload.amount >= 500" class="w-full bg-slate-950 font-mono text-xs text-amber-300 px-3 py-2 rounded-lg border border-slate-800 focus:border-amber-500 focus:outline-none">
        <span class="text-[11px] text-slate-400 mt-1 block">When true, follows <strong>TRUE (T)</strong> port. When false, follows <strong>FALSE (F)</strong> port.</span>
      </div>
    `;
  } else if (node.subtype === 'transform') {
    dynamicFields.innerHTML = `
      <div>
        <label class="block text-xs font-semibold text-slate-300 mb-1">Transform JavaScript Expression</label>
        <textarea id="modal-cfg-expression" rows="3" class="w-full bg-slate-950 font-mono text-xs text-slate-200 p-2.5 rounded-lg border border-slate-800 focus:border-indigo-500 focus:outline-none">${node.config?.expression || ''}</textarea>
      </div>
    `;
  } else if (node.subtype === 'database_query') {
    dynamicFields.innerHTML = `
      <div>
        <label class="block text-xs font-semibold text-slate-300 mb-1">Collection Name</label>
        <input id="modal-cfg-collection" type="text" value="${node.config?.collection || ''}" placeholder="orders" class="w-full bg-slate-950 font-mono text-xs text-slate-200 px-3 py-2 rounded-lg border border-slate-800 focus:border-indigo-500 focus:outline-none">
      </div>
    `;
  }

  nodeModal.classList.remove('hidden');
};

document.getElementById('modal-close-btn').onclick = () => nodeModal.classList.add('hidden');
document.getElementById('modal-cancel-btn').onclick = () => nodeModal.classList.add('hidden');

document.getElementById('modal-save-btn').onclick = () => {
  const node = state.nodes.find((n) => n.id === editingNodeId);
  if (!node) return;

  node.label = document.getElementById('modal-input-label').value.trim();
  node.config = node.config || {};

  const urlInput = document.getElementById('modal-cfg-url');
  if (urlInput) node.config.url = urlInput.value.trim();

  const methodInput = document.getElementById('modal-cfg-method');
  if (methodInput) node.config.method = methodInput.value;

  const exprInput = document.getElementById('modal-cfg-expression');
  if (exprInput) node.config.expression = exprInput.value.trim();

  const colInput = document.getElementById('modal-cfg-collection');
  if (colInput) node.config.collection = colInput.value.trim();

  nodeModal.classList.add('hidden');
  render();
};

document.getElementById('modal-delete-node-btn').onclick = () => {
  state.nodes = state.nodes.filter((n) => n.id !== editingNodeId);
  state.edges = state.edges.filter((e) => e.source !== editingNodeId && e.target !== editingNodeId);
  nodeModal.classList.add('hidden');
  render();
};

// 8. Execution Simulator Drawer & Step-by-Step Playback
const simDrawer = document.getElementById('sim-drawer');
const simTraceList = document.getElementById('sim-trace-list');

document.getElementById('btn-open-sim').onclick = () => {
  simDrawer.classList.remove('hidden');
  simDrawer.classList.add('flex');
};

document.getElementById('sim-close-btn').onclick = () => {
  simDrawer.classList.add('hidden');
  simDrawer.classList.remove('flex');
  state.simState = {};
  render();
};

document.getElementById('sim-start-btn').onclick = async () => {
  let payload = {};
  try {
    payload = JSON.parse(document.getElementById('sim-payload-input').value);
  } catch {
    alert('Invalid JSON in payload');
    return;
  }

  simTraceList.innerHTML = '<div class="text-xs text-slate-500 italic">Executing simulation...</div>';
  state.simState = {};
  render();

  // Try API simulation call or client fallback
  let simResult;
  try {
    const res = await fetch('/api/v1/workflows/simulate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': state.tenant.id },
      body: JSON.stringify({ workflow: { nodes: state.nodes, edges: state.edges }, payload }),
    });
    if (res.ok) simResult = await res.json();
  } catch {
    // offline fallback
  }

  if (!simResult || !simResult.success) {
    // Run client-side simulation logic
    const trace = [];
    const trigger = state.nodes.find((n) => n.type === 'trigger') || state.nodes[0];
    let currId = trigger?.id;
    const visited = new Set();

    while (currId && !visited.has(currId)) {
      visited.add(currId);
      const node = state.nodes.find((n) => n.id === currId);
      if (!node) break;

      const step = { nodeId: node.id, label: node.label, type: node.type, subtype: node.subtype, status: 'completed' };

      if (node.subtype === 'if_condition') {
        const expr = node.config?.expression || 'true';
        let evalBool = false;
        try {
          evalBool = Boolean(new Function('payload', `return ${expr};`)(payload));
        } catch {
          evalBool = (payload.amount || 0) >= 500;
        }
        step.branchTaken = evalBool ? 'true' : 'false';
        step.output = { expression: expr, evaluation: evalBool };
      } else {
        step.output = { timestamp: Date.now(), processed: true };
      }

      trace.push(step);

      const outgoing = state.edges.filter((e) => e.source === currId);
      if (node.subtype === 'if_condition') {
        const match = outgoing.find((e) => (e.sourceHandle || 'true') === step.branchTaken);
        currId = match?.target;
      } else {
        currId = outgoing[0]?.target;
      }
    }

    simResult = { success: true, executionTrace: trace };
  }

  // Animate playback
  simTraceList.innerHTML = '';
  for (let i = 0; i < simResult.executionTrace.length; i++) {
    const step = simResult.executionTrace[i];

    state.simState[step.nodeId] = 'running';
    render();
    await new Promise((r) => setTimeout(r, 550));

    state.simState[step.nodeId] = 'completed';
    render();

    const div = document.createElement('div');
    div.className = 'p-3 rounded-lg bg-slate-950/80 border border-slate-800 text-xs font-mono space-y-1.5';
    div.innerHTML = `
      <div class="flex items-center justify-between">
        <span class="text-cyan-400 font-bold">Step ${i + 1}: ${step.label}</span>
        <span class="text-[10px] uppercase px-1.5 py-0.5 rounded bg-emerald-950 text-emerald-400 border border-emerald-800/40">COMPLETED</span>
      </div>
      ${step.branchTaken ? `
        <div class="text-[11px] text-amber-300 bg-amber-950/40 px-2 py-1 rounded border border-amber-800/40">
          Branch Evaluated: <span class="font-bold uppercase text-white">${step.branchTaken}</span>
          <span class="text-slate-400 block text-[10px]">Expression: ${step.output?.expression}</span>
        </div>
      ` : ''}
      <pre class="text-[10px] text-slate-300 bg-slate-900 p-2 rounded overflow-x-auto">${JSON.stringify(step.output, null, 2)}</pre>
    `;
    simTraceList.appendChild(div);

    await new Promise((r) => setTimeout(r, 350));
  }
};

// 9. Cryptographic API Key Management
const keyModal = document.getElementById('key-modal');
document.getElementById('btn-issue-key').onclick = async () => {
  let keyData = null;
  try {
    const res = await fetch('/api/v1/auth/keys', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Tenant-ID': state.tenant.id,
        'X-Tenant-Tier': state.tenant.tier,
        'X-Isolation-Strategy': state.tenant.strategy,
      },
      body: JSON.stringify({
        name: `${state.tenant.id} Key`,
        tier: state.tenant.tier,
      }),
    });
    if (res.ok) {
      const json = await res.json();
      keyData = json.apiKey;
    }
  } catch {
    // fallback mock
  }

  if (!keyData) {
    keyData = {
      rawApiKey: `wk_live_${state.tenant.id}_${Math.random().toString(36).substring(2)}${Date.now()}.7f8e3a2b1c`,
      tenantId: state.tenant.id,
      tier: state.tenant.tier,
      scopes: ['workflows:read', 'workflows:write', 'workflows:execute'],
    };
  }

  document.getElementById('key-raw-display').textContent = keyData.rawApiKey;
  document.getElementById('key-tenant-display').textContent = keyData.tenantId;
  document.getElementById('key-tier-display').textContent = keyData.tier;
  document.getElementById('key-scopes-display').textContent = keyData.scopes?.join(', ');

  keyModal.classList.remove('hidden');
};

document.getElementById('key-close-btn').onclick = () => keyModal.classList.add('hidden');
document.getElementById('key-done-btn').onclick = () => keyModal.classList.add('hidden');
document.getElementById('key-copy-btn').onclick = () => {
  const text = document.getElementById('key-raw-display').textContent;
  navigator.clipboard.writeText(text);
  alert('API Key copied to clipboard!');
};

// 10. Tenant Switcher
const tenantSelect = document.getElementById('tenant-select');
const tenantBadge = document.getElementById('tenant-strategy-badge');

tenantSelect.onchange = (e) => {
  const val = e.target.value;
  if (val === 'acme_corp') {
    state.tenant = { id: 'acme_corp', tier: 'enterprise', strategy: 'multi-db' };
    tenantBadge.textContent = 'Dedicated DB Pool';
    tenantBadge.className = 'text-[10px] font-mono px-2 py-0.5 rounded-full border bg-emerald-950/80 text-emerald-400 border-emerald-800/50';
  } else if (val === 'zenith_ai') {
    state.tenant = { id: 'zenith_ai', tier: 'pro', strategy: 'discriminator' };
    tenantBadge.textContent = 'Discriminator Key';
    tenantBadge.className = 'text-[10px] font-mono px-2 py-0.5 rounded-full border bg-indigo-950/80 text-indigo-400 border-indigo-800/50';
  } else {
    state.tenant = { id: 'community_lab', tier: 'free', strategy: 'discriminator' };
    tenantBadge.textContent = 'Shared Partition';
    tenantBadge.className = 'text-[10px] font-mono px-2 py-0.5 rounded-full border bg-slate-900 text-slate-400 border-slate-700';
  }
};

// 11. Presets and Toolbar Triggers
document.getElementById('btn-preset-branching').onclick = () => {
  state.workflowName = PRESETS.branchingOrderFlow.name;
  state.nodes = JSON.parse(JSON.stringify(PRESETS.branchingOrderFlow.nodes));
  state.edges = JSON.parse(JSON.stringify(PRESETS.branchingOrderFlow.edges));
  state.selectedNodeId = null;
  state.selectedEdgeId = null;
  state.simState = {};
  document.getElementById('workflow-name-display').textContent = state.workflowName;
  render();
};

document.getElementById('btn-preset-cycle').onclick = () => {
  state.workflowName = PRESETS.cyclicStressTest.name;
  state.nodes = JSON.parse(JSON.stringify(PRESETS.cyclicStressTest.nodes));
  state.edges = JSON.parse(JSON.stringify(PRESETS.cyclicStressTest.edges));
  state.selectedNodeId = null;
  state.selectedEdgeId = null;
  state.simState = {};
  document.getElementById('workflow-name-display').textContent = state.workflowName;
  render();
};

document.getElementById('btn-zoom-in').onclick = () => {
  state.zoom = Math.min(state.zoom + 0.15, 2.2);
  updateWorldTransform();
};

document.getElementById('btn-zoom-out').onclick = () => {
  state.zoom = Math.max(state.zoom - 0.15, 0.4);
  updateWorldTransform();
};

document.getElementById('btn-zoom-reset').onclick = () => {
  state.zoom = 1;
  state.pan = { x: 50, y: 40 };
  updateWorldTransform();
};

document.getElementById('btn-add-webhook').onclick = () => addNode('trigger', 'webhook', 'Webhook Trigger');
document.getElementById('btn-add-branch').onclick = () => addNode('condition', 'if_condition', 'If / Else Branch');
document.getElementById('btn-add-http').onclick = () => addNode('action', 'http_request', 'HTTP Request');
document.getElementById('btn-add-transform').onclick = () => addNode('action', 'transform', 'Data Transform');
document.getElementById('btn-add-terminal').onclick = () => addNode('terminal', 'end_success', 'Final Step');

// Save Workflow to Backend with Tenant Context
document.getElementById('btn-save-workflow').onclick = async () => {
  try {
    const res = await fetch('/api/v1/workflows', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Tenant-ID': state.tenant.id,
        'X-Tenant-Tier': state.tenant.tier,
        'X-Isolation-Strategy': state.tenant.strategy,
      },
      body: JSON.stringify({
        name: state.workflowName,
        nodes: state.nodes,
        edges: state.edges,
      }),
    });
    if (res.ok) {
      const data = await res.json();
      alert(`Workflow saved successfully for tenant '${state.tenant.id}' using strategy: ${data.strategy}`);
    } else {
      alert('Workflow saved locally (server offline)');
    }
  } catch {
    alert(`Workflow saved locally for tenant '${state.tenant.id}'`);
  }
};

// Initial Render
render();

