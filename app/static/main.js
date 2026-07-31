/**
 * Farros Media Bot - Dashboard Interactive Features & Mobile Navigation
 */
document.addEventListener('DOMContentLoaded', () => {
  initMobileNav();
  initFormLoadingStates();
  initAutoDismissAlerts();
});

/**
 * Initialize responsive mobile navigation drawer
 */
function initMobileNav() {
  const toggleBtn = document.querySelector('.nav-toggle');
  const navLinks = document.querySelector('.nav-links');

  if (toggleBtn && navLinks) {
    toggleBtn.addEventListener('click', () => {
      const isOpen = navLinks.classList.toggle('open');
      toggleBtn.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
    });

    // Close mobile nav when clicking outside
    document.addEventListener('click', (e) => {
      if (!toggleBtn.contains(e.target) && !navLinks.contains(e.target)) {
        navLinks.classList.remove('open');
        toggleBtn.setAttribute('aria-expanded', 'false');
      }
    });
  }
}

/**
 * Add loading & disabled state to forms on submit to prevent duplicate submissions
 */
function initFormLoadingStates() {
  document.querySelectorAll('form').forEach((form) => {
    form.addEventListener('submit', function (e) {
      if (this.dataset.submitting === 'true') {
        e.preventDefault();
        return;
      }

      const submitBtn = this.querySelector('button[type="submit"]');
      if (submitBtn) {
        this.dataset.submitting = 'true';
        submitBtn.disabled = true;
        const originalText = submitBtn.innerHTML;
        submitBtn.dataset.originalText = originalText;
        submitBtn.classList.add('loading');
        submitBtn.innerHTML = '<span>Memproses...</span>';
      }
    });
  });
}

/**
 * Auto-dismiss alerts after 5 seconds if not closed
 */
function initAutoDismissAlerts() {
  const alerts = document.querySelectorAll('.alert-success');
  alerts.forEach((alert) => {
    setTimeout(() => {
      alert.style.transition = 'opacity 0.5s ease';
      alert.style.opacity = '0';
      setTimeout(() => alert.remove(), 500);
    }, 5000);
  });
}

/**
 * HTMX Event Listener for re-binding interactive elements after HTMX swaps
 */
if (typeof document.body !== 'undefined' && document.body) {
  document.body.addEventListener('htmx:afterSwap', () => {
    initFormLoadingStates();
  });
}
