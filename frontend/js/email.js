/**
 * DataPilot MCP — Email Modal Logic
 * Handles the email modal open/close/send cycle.
 */

// Module-level state for the current email send context
let _currentSummaryHtml = '';
let _currentChartImage = null;
let _isSendingEmail = false;

/**
 * Open the email modal with the given summary and optional chart image.
 *
 * @param {string} summaryHtml   - HTML summary string to preview and send.
 * @param {string|null} chartImg - Base64 PNG image string (data URL) or null.
 */
function openEmailModal(summaryHtml, chartImg) {
  _currentSummaryHtml = summaryHtml || '';
  _currentChartImage = chartImg || null;
  _isSendingEmail = false;

  // Populate preview
  const preview = document.getElementById('summaryPreview');
  if (preview) {
    preview.innerHTML = summaryHtml || '<em style="color:#64748b">No summary available.</em>';
  }

  // Reset fields
  const recipientInput = document.getElementById('emailRecipient');
  if (recipientInput) recipientInput.value = '';

  const subjectInput = document.getElementById('emailSubject');
  if (subjectInput) subjectInput.value = 'DataPilot — Business Summary';

  // Reset send button
  const sendBtn = document.getElementById('modalSendBtn');
  if (sendBtn) {
    sendBtn.disabled = false;
    sendBtn.innerHTML = `
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <path d="M14 8L2 3l2 5-2 5 12-5z" fill="currentColor"/>
      </svg>
      Send Email
    `;
  }

  // Show modal
  const backdrop = document.getElementById('emailModalBackdrop');
  if (backdrop) {
    backdrop.classList.remove('hidden');
    backdrop.setAttribute('aria-hidden', 'false');
  }

  // Focus recipient input
  setTimeout(() => {
    if (recipientInput) recipientInput.focus();
  }, 60);
}

/**
 * Close the email modal without sending.
 */
function closeEmailModal() {
  const backdrop = document.getElementById('emailModalBackdrop');
  if (backdrop) {
    backdrop.classList.add('hidden');
    backdrop.setAttribute('aria-hidden', 'true');
  }
  _currentSummaryHtml = '';
  _currentChartImage = null;
  _isSendingEmail = false;
}

/**
 * Send the email by calling POST /api/send-email.
 * Shows a toast on success or failure.
 */
async function sendEmail() {
  if (_isSendingEmail) return;

  const recipientInput = document.getElementById('emailRecipient');
  const subjectInput = document.getElementById('emailSubject');
  const sendBtn = document.getElementById('modalSendBtn');

  const recipient = (recipientInput ? recipientInput.value : '').trim();
  const subject = (subjectInput ? subjectInput.value : '').trim() || 'DataPilot — Business Summary';

  // Client-side validation
  if (!recipient) {
    recipientInput && recipientInput.focus();
    showToast('Please enter a recipient email address.', 'error');
    return;
  }

  if (!_validateEmail(recipient)) {
    showToast('Please enter a valid email address.', 'error');
    recipientInput && recipientInput.focus();
    return;
  }

  if (!_currentSummaryHtml) {
    showToast('No summary content to send.', 'error');
    return;
  }

  // Update button state
  _isSendingEmail = true;
  if (sendBtn) {
    sendBtn.disabled = true;
    sendBtn.innerHTML = `
      <span style="display:inline-block;width:14px;height:14px;border:2px solid #fff;border-top-color:transparent;border-radius:50%;animation:spin 0.8s linear infinite;"></span>
      Sending…
    `;
  }

  try {
    const payload = {
      recipient,
      subject,
      summary: _currentSummaryHtml,
      chart_image: _currentChartImage || null,
    };

    const response = await fetch('/api/send-email', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    const data = await response.json();

    if (response.ok && data.success) {
      closeEmailModal();
      showToast(`Email sent to ${recipient}`, 'success');
    } else {
      const errMsg = data.error || 'Failed to send email.';
      showToast(errMsg, 'error');

      // Re-enable button
      if (sendBtn) {
        sendBtn.disabled = false;
        sendBtn.innerHTML = `
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
            <path d="M14 8L2 3l2 5-2 5 12-5z" fill="currentColor"/>
          </svg>
          Send Email
        `;
      }
    }
  } catch (err) {
    console.error('[DataPilot] Email send error:', err);
    showToast('Network error. Please check your connection.', 'error');

    if (sendBtn) {
      sendBtn.disabled = false;
      sendBtn.innerHTML = `
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
          <path d="M14 8L2 3l2 5-2 5 12-5z" fill="currentColor"/>
        </svg>
        Send Email
      `;
    }
  } finally {
    _isSendingEmail = false;
  }
}

// -----------------------------------------------------------------
// Event listeners (attached when DOM is ready)
// -----------------------------------------------------------------

document.addEventListener('DOMContentLoaded', () => {
  // Close on backdrop click (outside the modal)
  const backdrop = document.getElementById('emailModalBackdrop');
  if (backdrop) {
    backdrop.addEventListener('click', (e) => {
      if (e.target === backdrop) closeEmailModal();
    });
  }

  // Close on X button
  const closeBtn = document.getElementById('modalClose');
  if (closeBtn) closeBtn.addEventListener('click', closeEmailModal);

  // Cancel button
  const cancelBtn = document.getElementById('modalCancelBtn');
  if (cancelBtn) cancelBtn.addEventListener('click', closeEmailModal);

  // Send button
  const sendBtn = document.getElementById('modalSendBtn');
  if (sendBtn) sendBtn.addEventListener('click', sendEmail);

  // Close on Escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      const backdrop = document.getElementById('emailModalBackdrop');
      if (backdrop && !backdrop.classList.contains('hidden')) {
        closeEmailModal();
      }
    }
  });

  // Allow Enter key in recipient field to submit
  const recipientInput = document.getElementById('emailRecipient');
  if (recipientInput) {
    recipientInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        sendEmail();
      }
    });
  }
});

// -----------------------------------------------------------------
// Private helpers
// -----------------------------------------------------------------

function _validateEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}
