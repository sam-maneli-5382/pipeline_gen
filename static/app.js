/**
 * Pipeline Builder UI
 *
 * A vanilla-JS drag-and-drop designer for Spark DataFrame pipelines.
 * Communicates with the Flask backend via JSON API calls.
 */

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let schema = null;
let nodes = [];
let selectedNodeId = null;
let nodeCounter = 0;
let dragOffset = { x: 0, y: 0 };
let draggedNodeId = null;

const canvas = document.getElementById('canvas');
const nodeForm = document.getElementById('node-form');
const inspectorHint = document.getElementById('inspector-hint');
const modal = document.getElementById('modal');
const modalBody = document.getElementById('modal-body');
const modalTitle = document.getElementById('modal-title');

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
async function init() {
    const response = await fetch('/api/schema');
    schema = await response.json();
    populateSelects();
    setupEventListeners();
}

function populateSelects() {
    fillSelect('node-connector', schema.connectors);
    fillSelect('node-transformation', schema.transformations);
    fillSelect('node-sink-type', schema.sinks);
}

function fillSelect(id, options) {
    const select = document.getElementById(id);
    select.innerHTML = '';
    options.forEach(opt => {
        const option = document.createElement('option');
        option.value = opt;
        option.textContent = opt;
        select.appendChild(option);
    });
}

// ---------------------------------------------------------------------------
// Event listeners
// ---------------------------------------------------------------------------
function setupEventListeners() {
    // Drag templates from palette
    document.querySelectorAll('.node-template').forEach(template => {
        template.addEventListener('dragstart', (e) => {
            e.dataTransfer.setData('node-type', template.dataset.type);
        });
    });

    // Canvas drop zone
    canvas.addEventListener('dragover', (e) => e.preventDefault());
    canvas.addEventListener('drop', handleDrop);

    // Node dragging inside canvas
    canvas.addEventListener('mousedown', handleMouseDown);
    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);

    // Inspector form inputs
    document.getElementById('node-name').addEventListener('input', updateSelectedNode);
    document.getElementById('node-connector').addEventListener('change', handleConnectorChange);
    document.getElementById('node-transformation').addEventListener('change', updateSelectedNode);
    document.getElementById('node-sink-type').addEventListener('change', handleSinkChange);
    document.getElementById('node-input-df').addEventListener('change', updateSelectedNode);
    document.getElementById('btn-delete-node').addEventListener('click', deleteSelectedNode);

    // Toolbar actions
    document.getElementById('btn-save').addEventListener('click', saveSpec);
    document.getElementById('btn-load').addEventListener('click', showLoadModal);
    document.getElementById('btn-export').addEventListener('click', exportYaml);
    document.getElementById('btn-run').addEventListener('click', runPipeline);

    // Modal close
    document.getElementById('modal-close').addEventListener('click', () => modal.classList.add('hidden'));
    document.querySelector('[data-close="load-modal"]').addEventListener('click', () => {
        document.getElementById('load-modal').classList.add('hidden');
    });
}

// ---------------------------------------------------------------------------
// Canvas interactions
// ---------------------------------------------------------------------------
function handleDrop(e) {
    e.preventDefault();
    const type = e.dataTransfer.getData('node-type');
    if (!type || !schema.node_types.includes(type)) return;

    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left - 110;
    const y = e.clientY - rect.top - 30;

    createNode(type, x, y);
}

function createNode(type, x, y) {
    nodeCounter += 1;
    const id = `node-${nodeCounter}`;
    const defaultName = `${type.toLowerCase()}_${nodeCounter}_df`;

    const node = {
        id,
        type,
        name: defaultName,
        x,
        y,
        config: {}
    };

    if (type === 'Ingestion') {
        node.connector = schema.connectors[0];
    } else if (type === 'Transformation') {
        node.transformation = schema.transformations[0];
        node.input_df = '';
    } else if (type === 'Sink') {
        node.sink_type = schema.sinks[0];
    }

    nodes.push(node);
    renderNode(node);
    selectNode(id);
    updateInputDfOptions();
}

function renderNode(node) {
    const el = document.createElement('div');
    el.className = `pipeline-node node-type-${node.type.toLowerCase()}`;
    el.id = node.id;
    el.style.left = `${node.x}px`;
    el.style.top = `${node.y}px`;
    el.dataset.id = node.id;

    el.innerHTML = `
        <div class="node-header">
            <span class="badge ${node.type.toLowerCase()}">${node.type[0]}</span>
            <span>${node.type}</span>
        </div>
        <div class="node-body">
            <div><strong>${node.name}</strong></div>
            ${renderNodeSummary(node)}
        </div>
    `;

    el.addEventListener('click', (e) => {
        e.stopPropagation();
        selectNode(node.id);
    });

    canvas.appendChild(el);
    canvas.classList.add('has-nodes');
}

function renderNodeSummary(node) {
    if (node.type === 'Ingestion') {
        return `<div>${node.connector || ''}</div>`;
    } else if (node.type === 'Transformation') {
        return `<div>${node.transformation || ''}</div><div>in: ${node.input_df || ''}</div>`;
    } else if (node.type === 'Sink') {
        return `<div>${node.sink_type || ''}</div>`;
    }
    return '';
}

function updateNodeElement(node) {
    const el = document.getElementById(node.id);
    if (!el) return;
    el.style.left = `${node.x}px`;
    el.style.top = `${node.y}px`;
    el.querySelector('.node-body').innerHTML = `
        <div><strong>${node.name}</strong></div>
        ${renderNodeSummary(node)}
    `;
}

function selectNode(id) {
    selectedNodeId = id;
    document.querySelectorAll('.pipeline-node').forEach(el => el.classList.remove('selected'));
    const el = document.getElementById(id);
    if (el) el.classList.add('selected');

    inspectorHint.classList.add('hidden');
    nodeForm.classList.remove('hidden');

    populateInspector();
}

function deselectNode() {
    selectedNodeId = null;
    document.querySelectorAll('.pipeline-node').forEach(el => el.classList.remove('selected'));
    inspectorHint.classList.remove('hidden');
    nodeForm.classList.add('hidden');
}

// ---------------------------------------------------------------------------
// Dragging nodes on canvas
// ---------------------------------------------------------------------------
function handleMouseDown(e) {
    const nodeEl = e.target.closest('.pipeline-node');
    if (!nodeEl) {
        deselectNode();
        return;
    }

    draggedNodeId = nodeEl.dataset.id;
    const rect = nodeEl.getBoundingClientRect();
    dragOffset.x = e.clientX - rect.left;
    dragOffset.y = e.clientY - rect.top;
}

function handleMouseMove(e) {
    if (!draggedNodeId) return;

    const node = nodes.find(n => n.id === draggedNodeId);
    if (!node) return;

    const canvasRect = canvas.getBoundingClientRect();
    node.x = e.clientX - canvasRect.left - dragOffset.x;
    node.y = e.clientY - canvasRect.top - dragOffset.y;

    updateNodeElement(node);
}

function handleMouseUp() {
    draggedNodeId = null;
}

// ---------------------------------------------------------------------------
// Inspector
// ---------------------------------------------------------------------------
function populateInspector() {
    const node = nodes.find(n => n.id === selectedNodeId);
    if (!node) return;

    document.getElementById('node-name').value = node.name || '';

    setVisible('connector-group', node.type === 'Ingestion');
    setVisible('transformation-group', node.type === 'Transformation');
    setVisible('sink-type-group', node.type === 'Sink');
    setVisible('input-df-group', node.type !== 'Ingestion');

    if (node.type === 'Ingestion') {
        document.getElementById('node-connector').value = node.connector || schema.connectors[0];
        renderDynamicFields('Ingestion', node.connector, node.config);
    } else if (node.type === 'Transformation') {
        document.getElementById('node-transformation').value = node.transformation || schema.transformations[0];
        updateInputDfOptions();
        document.getElementById('node-input-df').value = node.input_df || '';
    } else if (node.type === 'Sink') {
        document.getElementById('node-sink-type').value = node.sink_type || schema.sinks[0];
        updateInputDfOptions();
        document.getElementById('node-input-df').value = node.input_df || '';
        renderDynamicFields('Sink', node.sink_type, node.config);
    }
}

function setVisible(id, visible) {
    const el = document.getElementById(id);
    if (visible) el.classList.remove('hidden');
    else el.classList.add('hidden');
}

function updateInputDfOptions() {
    const select = document.getElementById('node-input-df');
    select.innerHTML = '<option value="">-- select --</option>';

    nodes
        .filter(n => n.type === 'Ingestion' || n.type === 'Transformation')
        .forEach(n => {
            const option = document.createElement('option');
            option.value = n.name;
            option.textContent = n.name;
            select.appendChild(option);
        });
}

function handleConnectorChange(e) {
    const node = nodes.find(n => n.id === selectedNodeId);
    if (!node) return;
    node.connector = e.target.value;
    node.config = {};
    renderDynamicFields('Ingestion', node.connector, node.config);
    updateSelectedNode();
}

function handleSinkChange(e) {
    const node = nodes.find(n => n.id === selectedNodeId);
    if (!node) return;
    node.sink_type = e.target.value;
    node.config = {};
    renderDynamicFields('Sink', node.sink_type, node.config);
    updateSelectedNode();
}

function renderDynamicFields(nodeType, subtype, configValues) {
    const container = document.getElementById('dynamic-fields');
    container.innerHTML = '';

    const fieldsKey = nodeType === 'Ingestion' ? 'connector_fields' : 'sink_fields';
    const fields = schema.fields[nodeType][fieldsKey][subtype] || [];

    fields.forEach(field => {
        const group = document.createElement('div');
        group.className = 'form-group';

        const label = document.createElement('label');
        label.textContent = field;

        const input = document.createElement('input');
        input.type = 'text';
        input.value = configValues[field] || '';
        input.dataset.field = field;
        input.addEventListener('input', updateDynamicConfig);

        group.appendChild(label);
        group.appendChild(input);
        container.appendChild(group);
    });
}

function updateDynamicConfig(e) {
    const node = nodes.find(n => n.id === selectedNodeId);
    if (!node) return;
    const field = e.target.dataset.field;
    node.config[field] = e.target.value;
    updateNodeElement(node);
}

function updateSelectedNode() {
    const node = nodes.find(n => n.id === selectedNodeId);
    if (!node) return;

    node.name = document.getElementById('node-name').value.trim() || node.name;

    if (node.type === 'Ingestion') {
        node.connector = document.getElementById('node-connector').value;
    } else if (node.type === 'Transformation') {
        node.transformation = document.getElementById('node-transformation').value;
        node.input_df = document.getElementById('node-input-df').value;
    } else if (node.type === 'Sink') {
        node.sink_type = document.getElementById('node-sink-type').value;
        node.input_df = document.getElementById('node-input-df').value;
    }

    updateNodeElement(node);
    updateInputDfOptions();
}

function deleteSelectedNode() {
    if (!selectedNodeId) return;
    nodes = nodes.filter(n => n.id !== selectedNodeId);
    const el = document.getElementById(selectedNodeId);
    if (el) el.remove();
    deselectNode();
    updateInputDfOptions();
    if (nodes.length === 0) canvas.classList.remove('has-nodes');
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------
function buildSpecPayload() {
    const pipelineName = document.getElementById('pipeline-name').value.trim() || 'My_Pipeline';

    const nodeSpecs = nodes.map(node => {
        const spec = {
            type: node.type,
            name: node.name
        };

        if (node.type === 'Ingestion') {
            spec.connector = node.connector;
            spec.config = { ...node.config };
        } else if (node.type === 'Transformation') {
            spec.transformation = node.transformation;
            spec.input_df = node.input_df;
        } else if (node.type === 'Sink') {
            spec.sink_type = node.sink_type;
            spec.input_df = node.input_df;
            spec.config = { ...node.config };
        }

        return spec;
    });

    return {
        name: pipelineName,
        version: '1.0',
        pipelines: [
            {
                name: pipelineName,
                nodes: nodeSpecs
            }
        ]
    };
}

async function saveSpec() {
    const payload = buildSpecPayload();
    const response = await fetch('/api/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    const data = await response.json();
    showModal('Saved', `Pipeline saved as ${data.filename}`);
}

async function showLoadModal() {
    const response = await fetch('/api/list');
    const data = await response.json();
    const list = document.getElementById('saved-specs-list');
    list.innerHTML = '';

    if (data.specs.length === 0) {
        list.innerHTML = '<li>No saved pipelines found</li>';
    } else {
        data.specs.forEach(spec => {
            const li = document.createElement('li');
            li.innerHTML = `<span>${spec.name}</span><span>${spec.saved_at || ''}</span>`;
            li.addEventListener('click', () => loadSpec(spec.filename));
            list.appendChild(li);
        });
    }

    document.getElementById('load-modal').classList.remove('hidden');
}

async function loadSpec(filename) {
    const response = await fetch(`/api/load/${filename}`);
    const data = await response.json();
    const spec = data.spec;

    // Clear current canvas
    nodes.forEach(n => {
        const el = document.getElementById(n.id);
        if (el) el.remove();
    });
    nodes = [];
    nodeCounter = 0;
    deselectNode();

    const pipeline = spec.pipelines[0];
    document.getElementById('pipeline-name').value = pipeline.name;

    pipeline.nodes.forEach((nodeSpec, index) => {
        nodeCounter += 1;
        const id = `node-${nodeCounter}`;
        const node = {
            id,
            type: nodeSpec.type,
            name: nodeSpec.name,
            x: 50 + index * 260,
            y: 100,
            config: nodeSpec.config || {}
        };

        if (nodeSpec.type === 'Ingestion') {
            node.connector = nodeSpec.connector;
        } else if (nodeSpec.type === 'Transformation') {
            node.transformation = nodeSpec.transformation;
            node.input_df = nodeSpec.input_df;
        } else if (nodeSpec.type === 'Sink') {
            node.sink_type = nodeSpec.sink_type;
            node.input_df = nodeSpec.input_df;
        }

        nodes.push(node);
        renderNode(node);
    });

    updateInputDfOptions();
    document.getElementById('load-modal').classList.add('hidden');
    showModal('Loaded', `Loaded pipeline: ${pipeline.name}`);
}

async function exportYaml() {
    const payload = buildSpecPayload();
    const response = await fetch('/api/export', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    const data = await response.json();
    showModal('YAML Output', data.yaml);
}

async function runPipeline() {
    const payload = buildSpecPayload();
    showModal('Running', 'Executing pipeline, please wait...');

    try {
        const response = await fetch('/api/run', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await response.json();

        if (data.status === 'success') {
            showModal('Pipeline Results', JSON.stringify(data.results, null, 2));
        } else {
            showModal('Pipeline Error', data.message);
        }
    } catch (err) {
        showModal('Error', err.message);
    }
}

// ---------------------------------------------------------------------------
// Modal helpers
// ---------------------------------------------------------------------------
function showModal(title, body) {
    modalTitle.textContent = title;
    modalBody.textContent = body;
    modal.classList.remove('hidden');
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------
init();
