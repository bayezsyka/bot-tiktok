document.addEventListener('DOMContentLoaded', () => {
  // Modal logic
  const modalTriggers = document.querySelectorAll('[data-toggle="modal"]');
  const modalBackdrops = document.querySelectorAll('.modal-backdrop');
  const modalCloses = document.querySelectorAll('.modal-close, [data-dismiss="modal"]');

  modalTriggers.forEach(trigger => {
    trigger.addEventListener('click', (e) => {
      e.preventDefault();
      const targetId = trigger.getAttribute('data-target');
      const targetModal = document.querySelector(targetId);
      if (targetModal) {
        targetModal.classList.add('show');
      }
    });
  });

  const closeModal = (modal) => {
    if (modal) {
      modal.classList.remove('show');
    }
  };

  modalCloses.forEach(closeBtn => {
    closeBtn.addEventListener('click', () => {
      const modal = closeBtn.closest('.modal-backdrop');
      closeModal(modal);
    });
  });

  modalBackdrops.forEach(backdrop => {
    backdrop.addEventListener('click', (e) => {
      if (e.target === backdrop) {
        closeModal(backdrop);
      }
    });
  });

  // Esc key to close modal
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      const openModal = document.querySelector('.modal-backdrop.show');
      closeModal(openModal);
    }
  });
});
