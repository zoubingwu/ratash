use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

type Interrupt = Arc<dyn Fn() + Send + Sync>;

#[derive(Default)]
struct CancellationState {
    cancelled: AtomicBool,
    next_interrupt_id: AtomicU64,
    interrupts: Mutex<BTreeMap<u64, Interrupt>>,
}

#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: Arc<CancellationState>,
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl CancellationToken {
    pub fn cancel(&self) {
        if self.inner.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        let interrupts = {
            let mut registered = self
                .inner
                .interrupts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *registered)
        };
        for interrupt in interrupts.into_values() {
            interrupt();
        }
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn register_interrupt(
        &self,
        interrupt: impl Fn() + Send + Sync + 'static,
    ) -> CancellationRegistration {
        let interrupt: Interrupt = Arc::new(interrupt);
        let mut registered = self
            .inner
            .interrupts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.inner.cancelled.load(Ordering::Acquire) {
            drop(registered);
            interrupt();
            return CancellationRegistration::default();
        }
        let interrupt_id = self
            .inner
            .next_interrupt_id
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1)
            .max(1);
        registered.insert(interrupt_id, interrupt);
        CancellationRegistration {
            inner: Arc::downgrade(&self.inner),
            interrupt_id: Some(interrupt_id),
        }
    }
}

#[derive(Default)]
pub struct CancellationRegistration {
    inner: Weak<CancellationState>,
    interrupt_id: Option<u64>,
}

impl fmt::Debug for CancellationRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationRegistration")
            .field("active", &self.interrupt_id.is_some())
            .finish()
    }
}

impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        let Some(interrupt_id) = self.interrupt_id.take() else {
            return;
        };
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        inner
            .interrupts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&interrupt_id);
    }
}
