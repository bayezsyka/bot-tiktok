document.addEventListener('DOMContentLoaded', function () {
    // Ensure HTMX automatically sends CSRF header if available
    const csrfMeta = document.querySelector('meta[name="csrf-token"]');
    if (csrfMeta && typeof htmx !== 'undefined') {
        const token = csrfMeta.getAttribute('content');
        document.body.addEventListener('htmx:configRequest', function (evt) {
            evt.detail.headers['X-CSRF-Token'] = token;
        });
    }

    // Auto-dismiss alert notifications after 6 seconds
    const alerts = document.querySelectorAll('.alert');
    alerts.forEach(function (alert) {
        setTimeout(function () {
            alert.style.transition = 'opacity 0.5s ease';
            alert.style.opacity = '0';
            setTimeout(function () {
                if (alert.parentNode) {
                    alert.parentNode.removeChild(alert);
                }
            }, 500);
        }, 6000);
    });
});
