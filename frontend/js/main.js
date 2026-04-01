/**
 * DataPilot MCP — Main Chat Logic
 * Handles message sending, response rendering, file upload, schema loading,
 * session management, and all UI interactions.
 *
 * Features added:
 *   1. Auto Data Profile on Upload
 *   2. SSE Streaming Progress
 *   3. Export Results (CSV + PNG chart)
 *   4. Query Suggestions from Schema
 *   5. Conversation Follow-ups (handled backend; frontend unchanged)
 *   6. Anomaly Highlighting in Tables
 *   7. Dark/Light Theme Toggle
 */

// ----------------------------------------------------------------
// Session Management
// ----------------------------------------------------------------

/**
 * Get or create a persistent session ID stored in sessionStorage.
 * @returns {string} UUID session identifier.
 */
function getSessionId() {
  let sid = sessionStorage.getItem('datapilot_session_id');
  if (!sid) {
    sid = _generateUUID();
    sessionStorage.setItem('datapilot_session_id', sid);
  }
  return sid;
}

function _generateUUID() {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  // Fallback
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

const SESSION_ID = getSessionId();

// ----------------------------------------------------------------
// State
// ----------------------------------------------------------------
let _isLoading = false;
let _chartCounter = 0;       // Unique IDs for chart canvases
let _uploadedFiles = [];     // Track uploaded file metadata

// Feature 3: Cache for export (keyed by chartId)
const _responseCache = new Map();

// ----------------------------------------------------------------
// DOM References
// ----------------------------------------------------------------
const chatContainer    = () => document.getElementById('chatContainer');
const messageInput     = () => document.getElementById('messageInput');
const sendBtn          = () => document.getElementById('sendBtn');
const spinnerOverlay   = () => document.getElementById('spinnerOverlay');
const spinnerText      = () => document.getElementById('spinnerText');
const welcomeCard      = () => document.getElementById('welcomeCard');
const filesList        = () => document.getElementById('filesList');
const schemaAccordion  = () => document.getElementById('schemaAccordion');
const dropzone         = () => document.getElementById('dropzone');
const fileInput        = () => document.getElementById('fileInput');
const progressWrap     = () => document.getElementById('uploadProgress');
const progressFill     = () => document.getElementById('progressFill');
const progressLabel    = () => document.getElementById('progressLabel');

// ----------------------------------------------------------------
// Send Message — Feature 2: SSE Streaming Progress
// ----------------------------------------------------------------

/**
 * Read the textarea, validate, then stream the /api/query/stream endpoint.
 */
async function sendMessage() {
  const input = messageInput();
  if (!input) return;

  const message = input.value.trim();
  if (!message || _isLoading) return;

  // Clear input and reset height
  input.value = '';
  _autoResizeTextarea(input);

  // Hide welcome card on first message
  const wc = welcomeCard();
  if (wc) wc.style.display = 'none';

  // Also hide suggestions bar when user submits
  const sugBar = document.getElementById('suggestionsBar');
  if (sugBar) sugBar.style.display = 'none';

  // Add user message bubble
  _appendUserMessage(message);

  // Disable input while streaming
  _setInputDisabled(true);

  // Inject inline progress card into chat
  const progressCard = _appendProgressCard();

  try {
    const url = `/api/query/stream?message=${encodeURIComponent(message)}&session_id=${encodeURIComponent(SESSION_ID)}`;
    const response = await fetch(url);

    if (!response.ok) {
      progressCard.remove();
      _appendErrorMessage(`Server error: ${response.status}`);
      return;
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      const parts = buffer.split('\n\n');
      buffer = parts.pop();

      for (const part of parts) {
        const line = part.trim();
        if (!line.startsWith('data:')) continue;
        const jsonStr = line.slice(5).trim();
        if (!jsonStr) continue;

        let event;
        try { event = JSON.parse(jsonStr); } catch (e) { continue; }

        const stageOrder = ['rewriting','generating','executing','charting','summarizing'];
        if (stageOrder.includes(event.stage)) {
          _advanceProgressCard(progressCard, event.stage, event.message);
        } else if (event.stage === 'done') {
          progressCard.remove();
          renderMessage(event.data);
          loadMetrics();
        } else if (event.stage === 'error') {
          progressCard.remove();
          _appendErrorMessage(event.message || 'An unexpected error occurred.');
        }
      }
    }

  } catch (err) {
    console.error('[DataPilot] sendMessage error:', err);
    progressCard.remove();
    _appendErrorMessage('Network error — could not reach the server. Please try again.');
  } finally {
    _setInputDisabled(false);
  }
}

// ----------------------------------------------------------------
// SSE Progress Card (inline in chat)
// ----------------------------------------------------------------

const _STAGES = [
  { key: 'rewriting',   label: 'Understanding question' },
  { key: 'generating',  label: 'Generating SQL'          },
  { key: 'executing',   label: 'Executing query'         },
  { key: 'charting',    label: 'Building chart'          },
  { key: 'summarizing', label: 'Writing summary'         },
];

function _appendProgressCard() {
  const row = document.createElement('div');
  row.className = 'message-row assistant progress-row';
  row.innerHTML = `
    <div class="message-avatar">
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <path d="M3 12L5.5 8L8 9.5L11 4" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
        <circle cx="11" cy="4" r="1.5" fill="currentColor"/>
      </svg>
    </div>
    <div class="message-content">
      <div class="progress-card">
        <div class="progress-steps">
          ${_STAGES.map(s => `
            <div class="progress-step" data-stage="${s.key}">
              <span class="step-dot"></span>
              <span class="step-label">${s.label}</span>
            </div>
          `).join('')}
        </div>
      </div>
    </div>`;
  chatContainer().appendChild(row);
  _scrollToBottom();
  return row;
}

function _advanceProgressCard(row, stage, message) {
  const stageOrder = _STAGES.map(s => s.key);
  const idx = stageOrder.indexOf(stage);

  // Mark all previous steps as done
  stageOrder.slice(0, idx).forEach(s => {
    const el = row.querySelector(`[data-stage="${s}"]`);
    if (el) { el.classList.remove('active'); el.classList.add('done'); }
  });

  // Mark current step as active
  const current = row.querySelector(`[data-stage="${stage}"]`);
  if (current) { current.classList.add('active'); }

  _scrollToBottom();
}

function _setInputDisabled(state) {
  const sb = sendBtn();
  const input = messageInput();
  if (sb) sb.disabled = state;
  if (input) input.disabled = state;
  _isLoading = state;
}

// ----------------------------------------------------------------
// Render Assistant Response
// ----------------------------------------------------------------

/**
 * Build and append a complete response card to the chat.
 * @param {object} data - API response from /api/query or SSE done event
 */
function renderMessage(data) {
  const chartId = `chart-canvas-${++_chartCounter}`;
  const hasSql     = Boolean(data.sql);
  const hasRows    = data.columns && data.rows && data.rows.length > 0;
  const hasChart   = data.chart_type && data.chart_type !== 'table' && data.chart_config;
  const hasSummary = Boolean(data.summary);

  // Cache data for export + chart explanation
  _responseCache.set(chartId, {
    columns:      data.columns      || [],
    rows:         data.rows         || [],
    chart_type:   data.chart_type   || '',
    chart_config: data.chart_config || null,
  });

  const row = document.createElement('div');
  row.className = 'message-row assistant';

  row.innerHTML = `
    <div class="message-avatar" title="DataPilot">
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <path d="M3 12L5.5 8L8 9.5L11 4" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
        <circle cx="11" cy="4" r="1.5" fill="currentColor"/>
      </svg>
    </div>
    <div class="message-content">
      <div class="assistant-card" id="card-${chartId}">
        <!-- Header -->
        <div class="assistant-card-header">
          <span class="assistant-card-label">DataPilot Response</span>
        </div>

        ${hasSql ? _buildSqlBlock(data.sql) : ''}
        ${hasRows ? _buildTableSection(data.columns, data.rows, data.row_count) : _buildNoDataSection()}
        ${hasChart ? _buildChartSection(chartId) : ''}
        ${hasSummary ? _buildSummarySection(data.summary) : ''}

        <!-- Actions -->
        <div class="card-actions">
          ${hasSummary ? `
            <button
              class="send-summary-btn"
              data-chart-id="${chartId}"
              data-summary="${_escapeAttr(data.summary)}"
              title="Send this summary by email"
            >
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                <path d="M12 7L2 2.5l1.75 4.5L2 11.5 12 7z" fill="currentColor"/>
              </svg>
              Send Summary
            </button>
          ` : ''}
          ${hasRows ? `
            <button class="export-csv-btn" data-chart-id="${chartId}" title="Export data as CSV">
              ↓ CSV
            </button>
          ` : ''}
          ${hasChart ? `
            <button class="export-chart-btn" data-chart-id="${chartId}" title="Export chart as PNG">
              ↓ Chart PNG
            </button>
            <button class="explain-chart-btn" data-chart-id="${chartId}" title="Ask AI to explain this chart">
              <svg width="13" height="13" viewBox="0 0 13 13" fill="none">
                <circle cx="6.5" cy="6.5" r="5.5" stroke="currentColor" stroke-width="1.5"/>
                <path d="M6.5 5.5v4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/>
                <circle cx="6.5" cy="3.5" r="0.75" fill="currentColor"/>
              </svg>
              Explain Chart
            </button>
          ` : ''}
          <span class="response-meta">
            ${data.provider ? `via ${data.provider}` : ''}
            ${data.total_latency_ms ? ` &middot; ${Math.round(data.total_latency_ms)}ms` : ''}
            ${data.attempts && data.attempts > 1 ? ` &middot; ${data.attempts} attempts` : ''}
          </span>
        </div>
      </div>
    </div>
  `;

  chatContainer().appendChild(row);

  // Render chart — use setTimeout so browser finishes layout and canvas has dimensions
  if (hasChart) {
    setTimeout(() => {
      const canvas = document.getElementById(chartId);
      if (canvas) {
        // Ensure canvas fills its wrapper before Chart.js measures it
        const wrapper = canvas.closest('.chart-canvas-wrapper');
        if (wrapper) {
          canvas.width  = wrapper.clientWidth  || 500;
          canvas.height = wrapper.clientHeight || 260;
        }
        renderChart(chartId, data.chart_config);
      }
    }, 150);
  }

  // Feature 6: Highlight anomalies in the table
  if (hasRows) {
    setTimeout(() => {
      const tableEl = row.querySelector('.result-table');
      if (tableEl) _highlightAnomalies(tableEl, data.columns, data.rows);
    }, 160);
  }

  // Attach SQL toggle listener
  if (hasSql) {
    const toggleBtn = row.querySelector('.sql-toggle');
    const codeBlock = row.querySelector('.sql-code-block');
    if (toggleBtn && codeBlock) {
      toggleBtn.addEventListener('click', () => {
        const isOpen = codeBlock.classList.toggle('visible');
        toggleBtn.classList.toggle('open', isOpen);
      });
    }

    // Copy SQL button
    const copyBtn = row.querySelector('.sql-copy-btn');
    if (copyBtn) {
      copyBtn.addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(data.sql);
          copyBtn.innerHTML = '&#10003; Copied!';
          setTimeout(() => {
            copyBtn.innerHTML = `
              <svg width="11" height="11" viewBox="0 0 11 11" fill="none">
                <rect x="3" y="3" width="7" height="7" rx="1.5" stroke="currentColor" stroke-width="1.3"/>
                <path d="M1 8V1h7" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>
              </svg>
              Copy SQL
            `;
          }, 2000);
        } catch (e) {
          showToast('Could not copy to clipboard.', 'error');
        }
      });
    }
  }

  // Attach Send Summary button listener
  const sendSummaryBtn = row.querySelector('.send-summary-btn');
  if (sendSummaryBtn) {
    sendSummaryBtn.addEventListener('click', () => {
      const cId = sendSummaryBtn.dataset.chartId;
      const summaryHtml = sendSummaryBtn.dataset.summary || '';
      const chartImage = cId ? captureChartImage(cId) : null;
      openEmailModal(summaryHtml, chartImage);
    });
  }

  // Feature 3: CSV export button
  const csvBtn = row.querySelector('.export-csv-btn');
  if (csvBtn) {
    csvBtn.addEventListener('click', () => _exportCsv(csvBtn.dataset.chartId));
  }

  // Feature 3: Chart PNG export button
  const pngBtn = row.querySelector('.export-chart-btn');
  if (pngBtn) {
    pngBtn.addEventListener('click', () => _exportChartPng(pngBtn.dataset.chartId));
  }

  // Explain Chart button
  const explainBtn = row.querySelector('.explain-chart-btn');
  if (explainBtn) {
    explainBtn.addEventListener('click', () => _explainChart(explainBtn, row));
  }

  _scrollToBottom();
}

// ----------------------------------------------------------------
// Message Builders
// ----------------------------------------------------------------

function _buildSqlBlock(sql) {
  return `
    <div class="sql-block">
      <button class="sql-toggle" title="Toggle SQL query">
        <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
          <path d="M1 4l3.5 3L8 4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/>
          <rect x="1" y="1" width="10" height="10" rx="2" stroke="currentColor" stroke-width="1.2" fill="none"/>
        </svg>
        <span style="margin-left:4px;">SQL Query</span>
        <svg class="sql-toggle-icon" width="12" height="12" viewBox="0 0 12 12" fill="none">
          <path d="M2 4l4 4 4-4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
      </button>
      <div class="sql-code-block">
        <pre>${_escapeHtml(sql)}</pre>
        <button class="sql-copy-btn">
          <svg width="11" height="11" viewBox="0 0 11 11" fill="none">
            <rect x="3" y="3" width="7" height="7" rx="1.5" stroke="currentColor" stroke-width="1.3"/>
            <path d="M1 8V1h7" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>
          </svg>
          Copy SQL
        </button>
      </div>
    </div>
  `;
}

function _buildTableSection(columns, rows, rowCount) {
  return `
    <div class="table-section">
      <div class="table-meta">${rowCount} row${rowCount !== 1 ? 's' : ''} returned</div>
      ${renderTable(columns, rows)}
    </div>
  `;
}

function _buildNoDataSection() {
  return `
    <div class="table-section">
      <p style="font-size:0.82rem;color:var(--text-muted);">Query returned no rows.</p>
    </div>
  `;
}

function _buildChartSection(chartId) {
  return `
    <div class="chart-section">
      <div class="chart-canvas-wrapper">
        <canvas id="${chartId}" width="500" height="260"></canvas>
      </div>
    </div>
  `;
}

function _buildSummarySection(summaryHtml) {
  return `
    <div class="summary-section">
      <div class="summary-label">Executive Summary</div>
      <div class="summary-content">${summaryHtml}</div>
    </div>
  `;
}

// ----------------------------------------------------------------
// Table Renderer
// ----------------------------------------------------------------

/**
 * Build a scrollable HTML table from columns and rows.
 * @param {string[]} columns
 * @param {any[][]} rows
 * @returns {string} HTML string
 */
function renderTable(columns, rows) {
  if (!columns || columns.length === 0) return '';

  const headers = columns
    .map(col => `<th title="${_escapeAttr(col)}">${_escapeHtml(String(col))}</th>`)
    .join('');

  const MAX_DISPLAY_ROWS = 200;
  const displayRows = rows.slice(0, MAX_DISPLAY_ROWS);
  const truncated = rows.length > MAX_DISPLAY_ROWS;

  const bodyRows = displayRows.map(row => {
    const cells = (Array.isArray(row) ? row : []).map(val => {
      const display = val === null || val === undefined ? '<span style="color:var(--text-muted)">null</span>' : _escapeHtml(String(val));
      return `<td title="${_escapeAttr(val === null ? '' : String(val))}">${display}</td>`;
    }).join('');
    return `<tr>${cells}</tr>`;
  }).join('');

  const truncatedNote = truncated
    ? `<tr><td colspan="${columns.length}" style="text-align:center;color:var(--text-muted);font-style:italic;padding:8px;">
        Showing ${MAX_DISPLAY_ROWS} of ${rows.length} rows
       </td></tr>`
    : '';

  return `
    <div class="table-scroll">
      <table class="result-table">
        <thead><tr>${headers}</tr></thead>
        <tbody>${bodyRows}${truncatedNote}</tbody>
      </table>
    </div>
  `;
}

// ----------------------------------------------------------------
// Feature 6: Anomaly Highlighting
// ----------------------------------------------------------------

/**
 * Highlight numeric outliers (|value - mean| > 2 * std) in a result table.
 * @param {HTMLElement} tableEl  - The .result-table element
 * @param {string[]} columns
 * @param {any[][]} rows
 */
function _highlightAnomalies(tableEl, columns, rows) {
  if (!tableEl || !columns || !rows || rows.length < 3) return;

  const tbody = tableEl.querySelector('tbody');
  if (!tbody) return;

  const trows = Array.from(tbody.querySelectorAll('tr'));

  // For each column, check if it is numeric
  columns.forEach((col, colIdx) => {
    const vals = rows
      .map(r => (Array.isArray(r) ? r[colIdx] : undefined))
      .filter(v => v !== null && v !== undefined && v !== '')
      .map(v => parseFloat(v));

    const allNumeric = vals.length > 0 && vals.every(v => !isNaN(v));
    if (!allNumeric) return;

    // Compute mean
    const mean = vals.reduce((s, v) => s + v, 0) / vals.length;

    // Compute std dev
    const variance = vals.reduce((s, v) => s + Math.pow(v - mean, 2), 0) / vals.length;
    const std = Math.sqrt(variance);

    if (std === 0) return; // All values identical — no outliers

    // Apply classes to table cells
    let anomalyFound = false;
    trows.forEach((tr, rowIdx) => {
      if (rowIdx >= rows.length) return; // skip truncation note row
      const cells = tr.querySelectorAll('td');
      const cell = cells[colIdx];
      if (!cell) return;

      const rawVal = Array.isArray(rows[rowIdx]) ? rows[rowIdx][colIdx] : undefined;
      if (rawVal === null || rawVal === undefined) return;
      const numVal = parseFloat(rawVal);
      if (isNaN(numVal)) return;

      if (Math.abs(numVal - mean) > 2 * std) {
        anomalyFound = true;
        cell.classList.add(numVal > mean ? 'anomaly-high' : 'anomaly-low');
      }
    });

    if (anomalyFound) {
      // Add legend below the table (only once, check if already added)
      const tableSection = tableEl.closest('.table-section');
      if (tableSection && !tableSection.querySelector('.anomaly-legend')) {
        const legend = document.createElement('div');
        legend.className = 'anomaly-legend';
        legend.innerHTML = '🔴 High outlier &nbsp; 🔵 Low outlier';
        tableSection.appendChild(legend);
      }
    }
  });
}

// ----------------------------------------------------------------
// Append User Message
// ----------------------------------------------------------------

function _appendUserMessage(text) {
  const row = document.createElement('div');
  row.className = 'message-row user';
  row.innerHTML = `
    <div class="message-avatar" title="You">U</div>
    <div class="message-content">
      <div class="user-bubble">${_escapeHtml(text)}</div>
    </div>
  `;
  chatContainer().appendChild(row);
  _scrollToBottom();
}

// ----------------------------------------------------------------
// Append Error Message
// ----------------------------------------------------------------

function _appendErrorMessage(errorText) {
  const row = document.createElement('div');
  row.className = 'message-row assistant';
  row.innerHTML = `
    <div class="message-avatar">
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <path d="M8 5v4M8 11v.5" stroke="#ef4444" stroke-width="2" stroke-linecap="round"/>
        <circle cx="8" cy="8" r="7" stroke="#ef4444" stroke-width="1.5"/>
      </svg>
    </div>
    <div class="message-content">
      <div class="error-card">
        <div class="error-title">Could not complete request</div>
        <div>${_escapeHtml(errorText)}</div>
      </div>
    </div>
  `;
  chatContainer().appendChild(row);
  _scrollToBottom();
}

// ----------------------------------------------------------------
// File Upload
// ----------------------------------------------------------------

async function handleFileUpload(file) {
  if (!file) return;

  const ext = file.name.split('.').pop().toLowerCase();
  if (!['csv', 'parquet'].includes(ext)) {
    showToast('Only .csv and .parquet files are supported.', 'error');
    return;
  }

  // Show progress bar
  _showUploadProgress('Uploading file…');

  const formData = new FormData();
  formData.append('file', file);

  try {
    const response = await fetch('/api/upload', {
      method: 'POST',
      body: formData,
    });

    const data = await response.json();

    if (!response.ok || !data.success) {
      showToast(data.error || 'Upload failed.', 'error');
      return;
    }

    // Track uploaded file
    _uploadedFiles.push({
      name: data.filename || file.name,
      table: data.table_name,
      rows: data.row_count,
    });

    _renderFilesList();
    showToast(`File "${data.filename}" loaded as table "${data.table_name}" (${data.row_count.toLocaleString()} rows)`, 'success');

    // Feature 1: Render data profile
    if (data.profile && data.profile.length > 0) {
      _renderDataProfile(data.profile, data.table_name);
    }

    // Feature 4: Load suggestions
    _loadSuggestions(data.table_name);

    // Refresh schema display
    loadSchema();

  } catch (err) {
    console.error('[DataPilot] Upload error:', err);
    showToast('Upload failed — could not reach the server.', 'error');
  } finally {
    _hideUploadProgress();
    // Reset file input so the same file can be re-uploaded
    const fi = fileInput();
    if (fi) fi.value = '';
  }
}

// ----------------------------------------------------------------
// Feature 1: Data Profile Renderer
// ----------------------------------------------------------------

/**
 * Render the per-column data profile panel in the sidebar.
 * @param {Array} profile  - Array of column profile objects
 * @param {string} tableName
 */
function _renderDataProfile(profile, tableName) {
  const panel = document.getElementById('profilePanel');
  const content = document.getElementById('profileContent');
  if (!panel || !content) return;

  panel.classList.remove('hidden');

  const rows = profile.map(col => {
    const isNumeric = col.mean !== null && col.mean !== undefined;
    const nullPct = typeof col.null_pct === 'number' ? col.null_pct.toFixed(1) : '0.0';
    const nullBarWidth = Math.min(100, Math.max(0, col.null_pct || 0));

    const nullCell = `
      <td title="${nullPct}%">
        ${nullPct}%
        <span class="null-bar-wrap" style="margin-left:4px">
          <span class="null-bar" style="width:${nullBarWidth}%"></span>
        </span>
      </td>
    `;

    const meanCell = isNumeric
      ? `<td title="${col.mean}">${_formatNumber(col.mean)}</td>`
      : `<td style="color:var(--text-muted)">—</td>`;

    return `
      <tr>
        <td title="${_escapeAttr(col.column)}" style="color:var(--text-primary);font-weight:500">${_escapeHtml(col.column)}</td>
        <td style="font-family:monospace;font-size:0.68rem;color:var(--text-muted)">${_escapeHtml(col.type)}</td>
        ${nullCell}
        <td>${_formatNumber(col.unique_count)}</td>
        <td title="${_escapeAttr(col.min || '')}">${_escapeHtml(col.min != null ? String(col.min) : '—')}</td>
        <td title="${_escapeAttr(col.max || '')}">${_escapeHtml(col.max != null ? String(col.max) : '—')}</td>
        ${meanCell}
      </tr>
    `;
  }).join('');

  content.innerHTML = `
    <div style="font-size:0.7rem;color:var(--text-muted);margin-bottom:6px;">
      Table: <strong style="color:var(--accent-light)">${_escapeHtml(tableName)}</strong>
      &middot; ${profile.length} column${profile.length !== 1 ? 's' : ''}
    </div>
    <div class="profile-tbl-scroll">
      <table class="profile-tbl">
        <thead>
          <tr>
            <th>Column</th>
            <th>Type</th>
            <th>Null%</th>
            <th>Unique</th>
            <th>Min</th>
            <th>Max</th>
            <th>Mean</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

// ----------------------------------------------------------------
// Feature 3: Export CSV
// ----------------------------------------------------------------

/**
 * Export the cached query result as a CSV file download.
 * @param {string} chartId
 */
function _exportCsv(chartId) {
  const cached = _responseCache.get(chartId);
  if (!cached || !cached.columns || !cached.rows) {
    showToast('No data to export.', 'error');
    return;
  }

  const { columns, rows } = cached;

  const _csvEscape = (val) => {
    if (val === null || val === undefined) return '';
    const s = String(val);
    if (s.includes('"') || s.includes(',') || s.includes('\n') || s.includes('\r')) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  };

  const lines = [];
  lines.push(columns.map(_csvEscape).join(','));
  for (const row of rows) {
    lines.push((Array.isArray(row) ? row : []).map(_csvEscape).join(','));
  }

  const csv = lines.join('\r\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);

  const a = document.createElement('a');
  a.href = url;
  a.download = `datapilot_export_${chartId}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);

  showToast('CSV exported successfully.', 'success', 2500);
}

// ----------------------------------------------------------------
// Feature 3: Export Chart PNG
// ----------------------------------------------------------------

/**
 * Export the chart canvas as a PNG file download.
 * @param {string} chartId
 */
function _exportChartPng(chartId) {
  const dataUrl = captureChartImage(chartId);
  if (!dataUrl) {
    showToast('No chart to export.', 'info');
    return;
  }

  const a = document.createElement('a');
  a.href = dataUrl;
  a.download = `datapilot_chart_${chartId}.png`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);

  showToast('Chart PNG exported.', 'success', 2500);
}

// ----------------------------------------------------------------
// Explain Chart
// ----------------------------------------------------------------

/**
 * Call POST /api/explain-chart and render the explanation inside the card.
 * @param {HTMLButtonElement} btn   - The clicked "Explain Chart" button.
 * @param {HTMLElement}       row   - The parent message-row element.
 */
async function _explainChart(btn, row) {
  const chartId = btn.dataset.chartId;
  const cached  = _responseCache.get(chartId);
  if (!cached || !cached.chart_config) {
    showToast('No chart data available to explain.', 'info');
    return;
  }

  // If explanation already shown, toggle it
  const existing = row.querySelector('.chart-explanation');
  if (existing) {
    existing.style.display = existing.style.display === 'none' ? '' : 'none';
    btn.textContent = existing.style.display === 'none' ? '💡 Explain Chart' : '💡 Hide Explanation';
    return;
  }

  // Disable button and show loading state
  btn.disabled = true;
  const originalHTML = btn.innerHTML;
  btn.innerHTML = '<span class="btn-spinner"></span> Explaining…';

  try {
    const res = await fetch('/api/explain-chart', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chart_type:   cached.chart_type,
        chart_config: cached.chart_config,
        columns:      cached.columns,
        rows:         cached.rows,
      }),
    });

    const json = await res.json();

    if (!res.ok || json.error) {
      showToast(json.error || 'Could not explain the chart.', 'error');
      return;
    }

    // Insert explanation block after the chart section
    const chartSection = row.querySelector('.chart-section');
    const explanationEl = document.createElement('div');
    explanationEl.className = 'chart-explanation';
    explanationEl.innerHTML = `
      <div class="explanation-label">
        <svg width="13" height="13" viewBox="0 0 13 13" fill="none">
          <circle cx="6.5" cy="6.5" r="5.5" stroke="currentColor" stroke-width="1.5"/>
          <path d="M6.5 5.5v4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/>
          <circle cx="6.5" cy="3.5" r="0.75" fill="currentColor"/>
        </svg>
        Chart Explanation
      </div>
      <p class="explanation-text">${_escapeHtml(json.explanation)}</p>
    `;
    if (chartSection) {
      chartSection.insertAdjacentElement('afterend', explanationEl);
    } else {
      row.querySelector('.assistant-card').appendChild(explanationEl);
    }

    btn.innerHTML = originalHTML;
    btn.textContent = '💡 Hide Explanation';
    _scrollToBottom();

  } catch (err) {
    console.error('[DataPilot] _explainChart error:', err);
    showToast('Network error — could not explain the chart.', 'error');
    btn.innerHTML = originalHTML;
  } finally {
    btn.disabled = false;
  }
}

// ----------------------------------------------------------------
// Feature 4: Query Suggestions
// ----------------------------------------------------------------

/**
 * Fetch 4 query suggestions for a table and render them.
 * @param {string} tableName
 */
async function _loadSuggestions(tableName) {
  try {
    const res = await fetch('/api/suggestions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ table_name: tableName, session_id: SESSION_ID }),
    });

    if (!res.ok) return;
    const data = await res.json();
    if (!data.suggestions || !data.suggestions.length) return;

    _renderSuggestions(data.suggestions);
  } catch (err) {
    console.warn('[DataPilot] Could not load suggestions:', err);
  }
}

/**
 * Render suggestion chips.
 * If welcome card is visible, replace its chips; otherwise show a suggestions bar above input.
 * @param {string[]} suggestions
 */
function _renderSuggestions(suggestions) {
  const wc = welcomeCard();
  const wcVisible = wc && wc.style.display !== 'none' && !wc.classList.contains('hidden');

  if (wcVisible) {
    // Replace example chips inside welcome card
    const examplesDiv = wc.querySelector('.example-queries');
    if (examplesDiv) {
      const label = examplesDiv.querySelector('.examples-label');
      examplesDiv.innerHTML = '';
      if (label) examplesDiv.appendChild(label);
      suggestions.forEach(q => {
        const btn = document.createElement('button');
        btn.className = 'example-chip';
        btn.dataset.query = q;
        btn.textContent = q.length > 60 ? q.slice(0, 60) + '…' : q;
        examplesDiv.appendChild(btn);
      });
    }
    return;
  }

  // Welcome card is hidden — show/update suggestions bar above the input area
  let sugBar = document.getElementById('suggestionsBar');
  if (!sugBar) {
    sugBar = document.createElement('div');
    sugBar.id = 'suggestionsBar';
    sugBar.className = 'suggestions-bar';

    const mainArea = document.querySelector('.main-area');
    const inputArea = document.querySelector('.chat-input-area');
    if (mainArea && inputArea) {
      mainArea.insertBefore(sugBar, inputArea);
    }
  }

  sugBar.innerHTML = `<span class="suggestions-label">Try:</span>`;
  suggestions.forEach(q => {
    const btn = document.createElement('button');
    btn.className = 'example-chip';
    btn.dataset.query = q;
    btn.textContent = q.length > 55 ? q.slice(0, 55) + '…' : q;
    sugBar.appendChild(btn);
  });

  sugBar.style.display = 'flex';
}

// ----------------------------------------------------------------
// Feature 7: Dark / Light Theme Toggle
// ----------------------------------------------------------------

function _initThemeToggle() {
  const saved = localStorage.getItem('datapilot_theme') || 'dark';
  _applyTheme(saved);

  const btn = document.getElementById('themeToggle');
  if (!btn) return;

  btn.addEventListener('click', () => {
    const current = document.body.getAttribute('data-theme') || 'dark';
    const next = current === 'dark' ? 'light' : 'dark';
    _applyTheme(next);
    localStorage.setItem('datapilot_theme', next);
  });
}

function _applyTheme(theme) {
  document.body.setAttribute('data-theme', theme);

  const moonPath = document.querySelector('.icon-moon');
  const sunGroup = document.querySelector('.icon-sun');

  if (theme === 'light') {
    if (moonPath) moonPath.style.display = 'none';
    if (sunGroup) sunGroup.style.display = '';
  } else {
    if (moonPath) moonPath.style.display = '';
    if (sunGroup) sunGroup.style.display = 'none';
  }
}

// ----------------------------------------------------------------
// Upload Progress UI
// ----------------------------------------------------------------

function _showUploadProgress(label) {
  const pw = progressWrap();
  const pl = progressLabel();
  const pf = progressFill();
  if (pw) pw.classList.remove('hidden');
  if (pl) pl.textContent = label;
  // Animate to 90%
  let pct = 0;
  if (pf) {
    pf.style.width = '0%';
    const interval = setInterval(() => {
      pct = Math.min(pct + 10, 90);
      pf.style.width = pct + '%';
      if (pct >= 90) clearInterval(interval);
    }, 80);
  }
}

function _hideUploadProgress() {
  const pw = progressWrap();
  const pf = progressFill();
  if (pf) pf.style.width = '100%';
  setTimeout(() => {
    if (pw) pw.classList.add('hidden');
    if (pf) pf.style.width = '0%';
  }, 500);
}

function _renderFilesList() {
  const list = filesList();
  if (!list) return;

  if (_uploadedFiles.length === 0) {
    list.innerHTML = '<li class="files-empty">No files uploaded yet.</li>';
    return;
  }

  list.innerHTML = _uploadedFiles.map(f => `
    <li class="file-item">
      <span class="file-item-icon">
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <rect x="2" y="1" width="8" height="11" rx="1.5" stroke="currentColor" stroke-width="1.3"/>
          <path d="M5 5h4M5 7.5h3M5 10h2" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/>
        </svg>
      </span>
      <span class="file-item-name" title="${_escapeAttr(f.name)}">${_escapeHtml(f.name)}</span>
      <span class="file-item-rows">${_formatNumber(f.rows)} rows</span>
    </li>
  `).join('');
}

// ----------------------------------------------------------------
// Schema Loader
// ----------------------------------------------------------------

async function loadSchema() {
  try {
    const response = await fetch('/api/schema');
    const data = await response.json();

    if (!response.ok || !data.tables) return;
    _renderSchema(data.tables);
  } catch (err) {
    console.warn('[DataPilot] Could not load schema:', err);
  }
}

function _renderSchema(tables) {
  const accordion = schemaAccordion();
  if (!accordion) return;

  const tableNames = Object.keys(tables || {});

  if (tableNames.length === 0) {
    accordion.innerHTML = '<p class="schema-empty">No tables loaded yet.</p>';
    return;
  }

  accordion.innerHTML = tableNames.map(tableName => {
    const cols = tables[tableName] || [];
    const colsHtml = cols.map(col => `
      <div class="schema-col">
        <span class="schema-col-name">${_escapeHtml(col.name)}</span>
        <span class="schema-col-type">${_escapeHtml(col.type)}</span>
      </div>
    `).join('');

    return `
      <div class="schema-table-item" data-table="${_escapeAttr(tableName)}">
        <button class="schema-table-toggle" title="Toggle columns for ${_escapeAttr(tableName)}">
          <span>${_escapeHtml(tableName)}</span>
          <span style="color:var(--text-muted);font-size:0.7rem;margin-left:6px;">${cols.length} cols</span>
          <svg class="schema-toggle-icon" width="12" height="12" viewBox="0 0 12 12" fill="none">
            <path d="M2 4l4 4 4-4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </button>
        <div class="schema-cols">${colsHtml}</div>
      </div>
    `;
  }).join('');

  // Attach toggle listeners
  accordion.querySelectorAll('.schema-table-toggle').forEach(btn => {
    btn.addEventListener('click', () => {
      const item = btn.closest('.schema-table-item');
      if (item) item.classList.toggle('open');
    });
  });
}

// ----------------------------------------------------------------
// Clear History
// ----------------------------------------------------------------

async function clearHistory() {
  try {
    await fetch('/api/history', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: SESSION_ID }),
    });
  } catch (e) {
    // non-critical
  }

  // Clear chat UI
  const container = chatContainer();
  if (container) {
    container.innerHTML = '';
    const wc = document.createElement('div');
    wc.className = 'welcome-card';
    wc.id = 'welcomeCard';
    wc.innerHTML = `
      <div class="welcome-icon">
        <svg width="48" height="48" viewBox="0 0 48 48" fill="none">
          <rect width="48" height="48" rx="16" fill="#7c3aed" fill-opacity="0.15"/>
          <path d="M12 34L18 24L24 29L32 16" stroke="#7c3aed" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
          <circle cx="32" cy="16" r="3" fill="#7c3aed"/>
        </svg>
      </div>
      <h2>Conversation cleared</h2>
      <p>Ask a new question to get started.</p>
    `;
    container.appendChild(wc);
  }

  // Remove suggestions bar on clear
  const sugBar = document.getElementById('suggestionsBar');
  if (sugBar) sugBar.remove();

  showToast('Conversation history cleared.', 'info');
}

// ----------------------------------------------------------------
// Toast Notifications
// ----------------------------------------------------------------

/**
 * Show a toast notification.
 * @param {string} message - Toast message text.
 * @param {'success'|'error'|'info'} type
 * @param {number} duration - Auto-dismiss in ms (default 3500)
 */
function showToast(message, type = 'info', duration = 3500) {
  const container = document.getElementById('toastContainer');
  if (!container) return;

  const icons = {
    success: '&#10003;',
    error:   '&#10005;',
    info:    '&#8505;',
  };

  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <span class="toast-icon">${icons[type] || icons.info}</span>
    <span class="toast-message">${_escapeHtml(message)}</span>
  `;

  container.appendChild(toast);

  // Auto-dismiss
  const dismissTimer = setTimeout(() => _dismissToast(toast), duration);

  // Manual dismiss on click
  toast.addEventListener('click', () => {
    clearTimeout(dismissTimer);
    _dismissToast(toast);
  });
}

function _dismissToast(toast) {
  toast.classList.add('toast-out');
  toast.addEventListener('animationend', () => toast.remove(), { once: true });
  // Fallback
  setTimeout(() => toast.remove(), 400);
}

// ----------------------------------------------------------------
// UI Helpers
// ----------------------------------------------------------------

function _setLoading(state, label = '') {
  _isLoading = state;

  const overlay = spinnerOverlay();
  const st = spinnerText();
  const sb = sendBtn();
  const input = messageInput();

  if (overlay) overlay.classList.toggle('hidden', !state);
  if (st && label) st.textContent = label;
  if (sb) sb.disabled = state;
  if (input) input.disabled = state;
}

function _updateSpinnerText(text) {
  const st = spinnerText();
  if (st) st.textContent = text;
}

function _scrollToBottom() {
  const container = chatContainer();
  if (container) {
    requestAnimationFrame(() => {
      container.scrollTop = container.scrollHeight;
    });
  }
}

function _autoResizeTextarea(textarea) {
  textarea.style.height = 'auto';
  const maxH = 140;
  textarea.style.height = Math.min(textarea.scrollHeight, maxH) + 'px';
}

function _formatNumber(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString();
}

function _escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function _escapeAttr(str) {
  if (str === null || str === undefined) return '';
  return String(str).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// ----------------------------------------------------------------
// Sidebar Toggle (Responsive)
// ----------------------------------------------------------------

function _initSidebarToggle() {
  const sidebar    = document.getElementById('sidebar');
  const openBtn    = document.getElementById('sidebarOpen');
  const closeBtn   = document.getElementById('sidebarClose');

  if (openBtn && sidebar) {
    openBtn.addEventListener('click', () => sidebar.classList.add('open'));
  }
  if (closeBtn && sidebar) {
    closeBtn.addEventListener('click', () => sidebar.classList.remove('open'));
  }

  // Close sidebar when clicking outside on mobile
  document.addEventListener('click', (e) => {
    if (!sidebar) return;
    if (
      window.innerWidth <= 768 &&
      sidebar.classList.contains('open') &&
      !sidebar.contains(e.target) &&
      e.target !== openBtn
    ) {
      sidebar.classList.remove('open');
    }
  });
}

// ----------------------------------------------------------------
// Dropzone
// ----------------------------------------------------------------

function _initDropzone() {
  const dz = dropzone();
  const fi = fileInput();
  if (!dz || !fi) return;

  // Click to browse
  dz.addEventListener('click', () => fi.click());

  // File input change
  fi.addEventListener('change', (e) => {
    if (e.target.files && e.target.files[0]) {
      handleFileUpload(e.target.files[0]);
    }
  });

  // Drag events
  dz.addEventListener('dragover', (e) => {
    e.preventDefault();
    dz.classList.add('dragover');
  });

  dz.addEventListener('dragleave', (e) => {
    if (!dz.contains(e.relatedTarget)) {
      dz.classList.remove('dragover');
    }
  });

  dz.addEventListener('drop', (e) => {
    e.preventDefault();
    dz.classList.remove('dragover');
    const file = e.dataTransfer.files[0];
    if (file) handleFileUpload(file);
  });
}

// ----------------------------------------------------------------
// Input Handlers
// ----------------------------------------------------------------

function _initInputHandlers() {
  const input = messageInput();
  const sb = sendBtn();

  if (input) {
    // Enter to send, Shift+Enter for newline
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
      }
    });

    // Auto-resize textarea
    input.addEventListener('input', () => _autoResizeTextarea(input));
  }

  if (sb) {
    sb.addEventListener('click', sendMessage);
  }
}

// ----------------------------------------------------------------
// Example Query Chips (+ Feature 4 suggestion chips)
// ----------------------------------------------------------------

function _initExampleChips() {
  document.addEventListener('click', (e) => {
    if (e.target.classList.contains('example-chip')) {
      const query = e.target.dataset.query;
      if (query) {
        const input = messageInput();
        if (input) {
          input.value = query;
          _autoResizeTextarea(input);
          input.focus();
          sendMessage();
        }
      }
    }
  });
}

// ----------------------------------------------------------------
// Clear History Button
// ----------------------------------------------------------------

function _initClearHistory() {
  const btn = document.getElementById('clearHistoryBtn');
  if (btn) {
    btn.addEventListener('click', () => {
      if (confirm('Clear conversation history for this session?')) {
        clearHistory();
      }
    });
  }
}

// ----------------------------------------------------------------
// Refresh Schema Button
// ----------------------------------------------------------------

function _initRefreshSchema() {
  const btn = document.getElementById('refreshSchema');
  if (btn) {
    btn.addEventListener('click', () => {
      loadSchema();
      showToast('Schema refreshed.', 'info', 2000);
    });
  }
}

// ----------------------------------------------------------------
// Evaluation Metrics
// ----------------------------------------------------------------

async function loadMetrics() {
  try {
    const res = await fetch(`/api/metrics?session_id=${SESSION_ID}`);
    if (!res.ok) return;
    const data = await res.json();
    _renderMetrics(data.metrics || []);
  } catch (e) {
    console.warn('[DataPilot] Could not load metrics:', e);
  }
}

function _renderMetrics(metrics) {
  const wrap    = document.getElementById('metricsTableWrap');
  const empty   = document.getElementById('metricsEmpty');
  const summary = document.getElementById('metricsSummary');
  if (!wrap) return;

  if (!metrics.length) {
    if (empty) empty.style.display = '';
    if (summary) summary.style.display = 'none';
    return;
  }

  if (empty) empty.style.display = 'none';

  // Summary pills
  if (summary) {
    summary.style.display = 'flex';
    const total      = metrics.length;
    const successes  = metrics.filter(m => m.success).length;
    const successPct = Math.round((successes / total) * 100);
    const avgLatency = Math.round(metrics.reduce((s, m) => s + (m.latency_ms || 0), 0) / total);
    const totalRetries = metrics.reduce((s, m) => s + (m.retry_count || 0), 0);

    document.getElementById('mTotalVal').textContent    = total;
    document.getElementById('mSuccessVal').textContent  = successPct + '%';
    document.getElementById('mLatencyVal').textContent  = avgLatency + 'ms';
    document.getElementById('mRetriesVal').textContent  = totalRetries;
  }

  // Per-query rows (most recent first, max 20 shown)
  const rows = metrics.slice(0, 20).map(m => {
    const question  = m.question ? _escapeHtml(m.question.slice(0, 40)) + (m.question.length > 40 ? '…' : '') : '—';
    const provider  = m.llm_provider_used || m.provider || '—';
    const latency   = m.latency_ms != null ? Math.round(m.latency_ms) + 'ms' : '—';
    const retries   = m.retry_count != null ? m.retry_count : '—';
    const tokens    = m.token_count != null ? _formatNumber(m.token_count) : '—';
    const ok        = m.success;
    const statusDot = `<span class="status-dot ${ok ? 'ok' : 'fail'}" title="${ok ? 'Success' : 'Failed'}"></span>`;

    return `<tr>
      <td>${statusDot}</td>
      <td class="mq-question" title="${_escapeAttr(m.question || '')}">${question}</td>
      <td><span class="provider-badge ${provider}">${provider}</span></td>
      <td class="mq-num">${latency}</td>
      <td class="mq-num ${retries > 0 ? 'warn' : ''}">${retries}</td>
      <td class="mq-num">${tokens}</td>
    </tr>`;
  }).join('');

  wrap.innerHTML = `
    <div class="metrics-tbl-scroll">
      <table class="metrics-tbl">
        <thead>
          <tr>
            <th></th>
            <th>Query</th>
            <th>LLM</th>
            <th>Latency</th>
            <th>Retries</th>
            <th>Tokens</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function _initRefreshMetrics() {
  const btn = document.getElementById('refreshMetrics');
  if (btn) {
    btn.addEventListener('click', () => {
      loadMetrics();
      showToast('Metrics refreshed.', 'info', 2000);
    });
  }
}

// ----------------------------------------------------------------
// Connect DB Modal
// ----------------------------------------------------------------

function _initConnectDb() {
  const openBtn    = document.getElementById('connectDbBtn');
  const backdrop   = document.getElementById('connectDbModalBackdrop');
  const closeBtn   = document.getElementById('connectDbModalClose');
  const cancelBtn  = document.getElementById('connectDbCancelBtn');
  const submitBtn  = document.getElementById('connectDbSubmitBtn');
  const feedback   = document.getElementById('connectDbFeedback');
  const dbStatus   = document.getElementById('dbStatus');
  const statusText = document.getElementById('dbStatusText');
  const disconnBtn = document.getElementById('dbDisconnectBtn');
  const aliasRow   = document.getElementById('dbAliasRow');
  const portInput  = document.getElementById('dbPort');
  const typeBtns   = document.querySelectorAll('.db-type-btn');

  if (!openBtn || !backdrop) return;

  // Track selected DB type
  let _dbType = 'postgres';

  // DB type toggle
  typeBtns.forEach((btn) => {
    btn.addEventListener('click', () => {
      typeBtns.forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
      _dbType = btn.dataset.type;

      // Swap defaults
      if (_dbType === 'mssql') {
        portInput.value = 5432 === parseInt(portInput.value) ? 1433 : portInput.value;
        if (aliasRow) aliasRow.style.display = 'none';
        document.getElementById('dbUsername').placeholder = 'sa';
      } else {
        portInput.value = 1433 === parseInt(portInput.value) ? 5432 : portInput.value;
        if (aliasRow) aliasRow.style.display = '';
        document.getElementById('dbUsername').placeholder = 'postgres';
      }
      _clearFeedback();
    });
  });

  function _openModal() {
    backdrop.classList.remove('hidden');
    document.getElementById('dbHost').focus();
    _clearFeedback();
  }

  function _closeModal() {
    backdrop.classList.add('hidden');
    _clearFeedback();
  }

  function _clearFeedback() {
    if (feedback) {
      feedback.textContent = '';
      feedback.className = 'connect-db-feedback hidden';
    }
  }

  function _showFeedback(msg, type) {
    feedback.textContent = msg;
    feedback.className = `connect-db-feedback connect-db-feedback--${type}`;
  }

  openBtn.addEventListener('click', _openModal);
  closeBtn.addEventListener('click', _closeModal);
  cancelBtn.addEventListener('click', _closeModal);
  backdrop.addEventListener('click', (e) => { if (e.target === backdrop) _closeModal(); });

  submitBtn.addEventListener('click', async () => {
    const host     = (document.getElementById('dbHost').value     || '').trim();
    const port     = parseInt(portInput.value || (_dbType === 'mssql' ? '1433' : '5432'), 10);
    const database = (document.getElementById('dbDatabase').value || '').trim();
    const username = (document.getElementById('dbUsername').value || '').trim();
    const password = document.getElementById('dbPassword').value || '';
    const alias    = (document.getElementById('dbAlias').value    || 'pg').trim();

    if (!host || !database || !username) {
      _showFeedback('Host, database, and username are required.', 'error');
      return;
    }

    submitBtn.disabled = true;
    _showFeedback('Connecting…', 'loading');

    try {
      const payload = { db_type: _dbType, host, port, database, username, password };
      if (_dbType === 'postgres') payload.alias = alias;

      const res  = await fetch('/api/connect-db', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(payload),
      });
      const json = await res.json();

      if (!res.ok || json.error) {
        _showFeedback(json.error || 'Connection failed.', 'error');
        return;
      }

      // Success — update sidebar status badge
      const dbLabel   = _dbType === 'mssql' ? 'SQL Server' : 'PostgreSQL';
      const tableInfo = `${json.table_count} table${json.table_count !== 1 ? 's' : ''}`;
      statusText.textContent = `${dbLabel} · ${tableInfo}`;
      dbStatus.classList.remove('hidden');
      openBtn.classList.add('hidden');

      showToast(`Connected to ${database} (${dbLabel}) — ${json.table_count} tables available.`, 'success', 4000);
      _closeModal();
      loadSchema();

    } catch (err) {
      console.error('[DataPilot] connect-db error:', err);
      _showFeedback('Network error — could not reach the server.', 'error');
    } finally {
      submitBtn.disabled = false;
    }
  });

  if (disconnBtn) {
    disconnBtn.addEventListener('click', async () => {
      // Call server to close mssql connection
      try { await fetch('/api/disconnect-db', { method: 'POST' }); } catch (_) {}
      dbStatus.classList.add('hidden');
      openBtn.classList.remove('hidden');
      showToast('Disconnected from external database.', 'info', 3000);
      loadSchema();
    });
  }
}

// ----------------------------------------------------------------
// ----------------------------------------------------------------
// Voice Query — Web Speech API
// ----------------------------------------------------------------

/**
 * Sets up the mic button for speech-to-text using the Web Speech API.
 * Transcribed text is inserted into the message input.
 * Falls back gracefully when the API is unavailable.
 */
function _initVoiceInput() {
  const btn = document.getElementById('micBtn');
  if (!btn) return;

  const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;

  if (!SpeechRecognition) {
    btn.title = 'Voice input not supported in this browser';
    btn.disabled = true;
    return;
  }

  const recognition = new SpeechRecognition();
  recognition.lang = 'en-US';
  recognition.interimResults = true;   // show partial results while speaking
  recognition.maxAlternatives = 1;
  recognition.continuous = false;

  let _listening = false;
  let _partialStart = 0;               // cursor position where interim text begins

  function _startListening() {
    if (_isLoading) return;
    _listening = true;
    btn.classList.add('recording');
    btn.title = 'Listening… click to stop';

    const input = messageInput();
    _partialStart = input.value.length;
    // Add a space separator if the input already has text
    if (_partialStart > 0 && !input.value.endsWith(' ')) {
      input.value += ' ';
      _partialStart = input.value.length;
    }

    recognition.start();
  }

  function _stopListening() {
    _listening = false;
    btn.classList.remove('recording');
    btn.title = 'Voice input';
    recognition.stop();
  }

  btn.addEventListener('click', () => {
    if (_listening) {
      _stopListening();
    } else {
      _startListening();
    }
  });

  // Replace interim text with the latest result on each update
  recognition.addEventListener('result', (e) => {
    const input = messageInput();
    if (!input) return;

    let transcript = '';
    for (const result of e.results) {
      transcript += result[0].transcript;
    }

    // Replace everything from _partialStart onwards with the new transcript
    input.value = input.value.slice(0, _partialStart) + transcript;
    _autoResizeTextarea(input);
  });

  // Auto-stop and trim trailing space when speech ends
  recognition.addEventListener('end', () => {
    if (_listening) _stopListening();
    const input = messageInput();
    if (input) input.value = input.value.trimEnd();
  });

  recognition.addEventListener('error', (e) => {
    _stopListening();
    const msgs = {
      'not-allowed': 'Microphone access denied. Please allow mic permission and try again.',
      'no-speech':   'No speech detected. Please try again.',
      'network':     'Network error during voice recognition.',
    };
    showToast(msgs[e.error] || `Voice error: ${e.error}`, 'error');
  });
}

// ----------------------------------------------------------------
// Initialization
// ----------------------------------------------------------------

document.addEventListener('DOMContentLoaded', () => {
  _initSidebarToggle();
  _initDropzone();
  _initInputHandlers();
  _initExampleChips();
  _initClearHistory();
  _initRefreshSchema();
  _initRefreshMetrics();
  _initThemeToggle();   // Feature 7
  _initVoiceInput();    // Voice Query
  _initConnectDb();     // Connect DB

  loadSchema();
  loadMetrics();

  const input = messageInput();
  if (input) input.focus();
});
