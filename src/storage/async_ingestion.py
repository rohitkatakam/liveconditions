"""Standalone background ingestion worker using stdlib threading primitives."""
import logging
import queue
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional


logger = logging.getLogger(__name__)

_SENTINEL = object()


@dataclass
class IngestionJob:
    """Encapsulates one enqueued batch ingestion request."""

    patient_id: str
    raw_conditions: list
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    enqueued_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    on_complete: Optional[Callable] = field(default=None, repr=False)


class BackgroundIngestionWorker:
    """Daemon worker thread that processes IngestionJob items from a queue.

    - A single daemon Thread is used; it does not block process exit.
    - Per-job exceptions are caught and counted — the worker keeps running.
    - All counter mutations are protected by a Lock for thread-safe reads.
    """

    def __init__(
        self,
        process_fn: Callable[[IngestionJob], dict],
        max_queue_size: int = 0,
    ) -> None:
        self._process_fn = process_fn
        self._max_queue_size = max_queue_size
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue_size)
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        # Stats
        self._jobs_enqueued: int = 0
        self._jobs_processed: int = 0
        self._jobs_failed: int = 0
        self._last_error: Optional[str] = None
        self._last_processed_at: Optional[datetime] = None
        self._last_heartbeat: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the worker thread. Idempotent — no-op if already running."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run, daemon=True, name="BackgroundIngestionWorker"
            )
            self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal worker to drain and stop, wait up to *timeout* seconds. Idempotent."""
        with self._lock:
            thread = self._thread
        if thread is None or not thread.is_alive():
            return
        self._stop_event.set()
        try:
            self._queue.put_nowait(_SENTINEL)
        except queue.Full:
            pass  # stop_event is set; worker will exit on next idle check
        thread.join(timeout=timeout)
        with self._lock:
            if not thread.is_alive():
                self._thread = None

    @property
    def is_running(self) -> bool:
        """True if the worker thread is alive."""
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def enqueue(self, job: IngestionJob) -> None:
        """Enqueue a job. Raises queue.Full if max_queue_size is exceeded."""
        self._queue.put_nowait(job)
        with self._lock:
            self._jobs_enqueued += 1

    def flush(self, timeout: float = 10.0) -> bool:
        """Block until the queue is drained or *timeout* seconds pass.

        Returns True if fully drained, False if timed out.
        """
        # queue.join() blocks indefinitely; we wrap it with a timer.
        result = {"done": False}

        def _join():
            self._queue.join()
            result["done"] = True

        joiner = threading.Thread(target=_join, daemon=True)
        joiner.start()
        joiner.join(timeout=timeout)
        return result["done"]

    def get_status(self) -> dict:
        """Return a thread-safe snapshot of worker health."""
        with self._lock:
            return {
                "worker_running": self._thread is not None and self._thread.is_alive(),
                "queue_depth": self._queue.qsize(),
                "jobs_enqueued": self._jobs_enqueued,
                "jobs_processed": self._jobs_processed,
                "jobs_failed": self._jobs_failed,
                "last_error": self._last_error,
                "last_processed_at": self._last_processed_at,
                "last_heartbeat": self._last_heartbeat,
            }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Main worker loop — runs on the worker thread."""
        thread_name = threading.current_thread().name
        logger.info(
            "background_worker_started",
            extra={"thread_name": thread_name, "max_queue_size": self._max_queue_size},
        )
        while True:
            # Update heartbeat even when idle
            with self._lock:
                self._last_heartbeat = datetime.now(timezone.utc)

            if self._stop_event.is_set():
                break

            try:
                job = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue

            # Sentinel => graceful shutdown
            if job is _SENTINEL:
                self._queue.task_done()
                with self._lock:
                    processed = self._jobs_processed
                    failed = self._jobs_failed
                logger.info(
                    "background_worker_stopped",
                    extra={"jobs_processed": processed, "jobs_failed": failed},
                )
                break

            try:
                result = self._process_fn(job)
                with self._lock:
                    self._jobs_processed += 1
                    self._last_processed_at = datetime.now(timezone.utc)
                logger.info(
                    "background_job_processed",
                    extra={
                        "patient_id": job.patient_id,
                        "batch_id": job.batch_id,
                        "conditions_ingested": result.get("conditions_ingested"),
                        "conditions_failed": result.get("conditions_failed"),
                    },
                )
                if job.on_complete is not None:
                    try:
                        job.on_complete(job, result)
                    except Exception:
                        pass  # callback errors must not crash the worker
            except Exception as exc:
                with self._lock:
                    self._jobs_failed += 1
                    self._last_error = str(exc)
                logger.error(
                    "background_job_failed",
                    extra={
                        "patient_id": job.patient_id,
                        "batch_id": job.batch_id,
                        "error": str(exc),
                    },
                )
            finally:
                self._queue.task_done()
