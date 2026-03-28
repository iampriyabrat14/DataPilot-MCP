/**
 * DataPilot MCP — Chart.js Rendering
 * Renders Chart.js charts from server-provided config objects.
 * Handles line, bar, pie, scatter; falls back to a table message.
 */

// Track active Chart.js instances by canvas ID to allow re-render
const _chartInstances = {};

/**
 * Render a Chart.js chart on the given canvas element.
 *
 * @param {string} canvasId   - The ID of the <canvas> element.
 * @param {object} chartConfig - Chart.js config object from the server.
 * @returns {Chart|null} The Chart.js instance, or null on failure.
 */
function renderChart(canvasId, chartConfig) {
  if (!chartConfig || !canvasId) {
    console.warn('[DataPilot] renderChart: missing canvasId or chartConfig');
    return null;
  }

  const canvas = document.getElementById(canvasId);
  if (!canvas) {
    console.warn('[DataPilot] renderChart: canvas not found:', canvasId);
    return null;
  }

  // Destroy existing chart instance on this canvas
  destroyChart(canvasId);

  // Validate chart type
  const supportedTypes = ['line', 'bar', 'pie', 'doughnut', 'scatter'];
  const chartType = chartConfig.type;
  if (!supportedTypes.includes(chartType)) {
    console.warn('[DataPilot] Unsupported chart type:', chartType);
    _showFallbackMessage(canvas, `Chart type "${chartType}" is not supported.`);
    return null;
  }

  try {
    const ctx = canvas.getContext('2d');

    // Apply global Chart.js defaults for dark theme
    _applyDarkDefaults();

    const instance = new Chart(ctx, chartConfig);
    _chartInstances[canvasId] = instance;

    return instance;
  } catch (err) {
    console.error('[DataPilot] Chart render error:', err);
    _showFallbackMessage(canvas, 'Chart could not be rendered.');
    return null;
  }
}

/**
 * Destroy a chart instance and free the canvas for reuse.
 *
 * @param {string} canvasId - The ID of the canvas whose chart should be destroyed.
 */
function destroyChart(canvasId) {
  if (_chartInstances[canvasId]) {
    try {
      _chartInstances[canvasId].destroy();
    } catch (e) {
      // ignore
    }
    delete _chartInstances[canvasId];
  }
}

/**
 * Capture the chart canvas as a base64 PNG image string.
 * Returns null if the canvas or chart is not found.
 *
 * @param {string} canvasId
 * @returns {string|null} base64 data URL or null
 */
function captureChartImage(canvasId) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return null;
  try {
    return canvas.toDataURL('image/png');
  } catch (e) {
    console.warn('[DataPilot] Could not capture chart image:', e);
    return null;
  }
}

// -----------------------------------------------------------------
// Private helpers
// -----------------------------------------------------------------

function _applyDarkDefaults() {
  Chart.defaults.color = '#94a3b8';
  Chart.defaults.borderColor = 'rgba(255,255,255,0.06)';
  Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
  Chart.defaults.font.size = 12;
}

function _showFallbackMessage(canvas, message) {
  const wrapper = canvas.closest('.chart-canvas-wrapper');
  if (wrapper) {
    wrapper.innerHTML = `
      <div style="
        display:flex; align-items:center; justify-content:center;
        height:100%; color:#64748b; font-size:0.82rem; text-align:center;
        padding:16px;
      ">
        <span>${message}</span>
      </div>`;
  }
}
