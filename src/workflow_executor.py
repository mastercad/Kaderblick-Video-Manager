"""Workflow-Executor mit konservativer Pipeline-Ausführung.

Transfer, GPU-lastige Verarbeitung und Uploads werden kontrolliert überlappt:
    1. Ein Transfer-Worker im Executor-Thread
    2. Ein Verarbeitungs-Worker für Convert / Merge / FFmpeg-Ausgaben
    3. Ein Upload-Worker für YouTube / Kaderblick

Merge-Gruppen behalten ihre Barriere: Einzeldateien können vorab konvertiert
werden, das Zusammenführen startet aber erst, wenn alle Transfer-Aufgaben
abgeschlossen und die zugehörigen Einzelverarbeitungen erledigt sind.
"""

from collections import defaultdict
from queue import Queue
import threading
import time
from pathlib import Path
from typing import Any

from PySide6.QtCore import QObject, Signal, Slot

from .settings import AppSettings
from .converter import ConvertJob, run_convert, run_youtube_convert, run_concat
from .downloader import download_device
from .youtube import get_youtube_service
from .workflow import Workflow, WorkflowJob, FileEntry
from .workflow_steps import (
    ConvertItem,
    PreparedOutput,
    ExecutorSupport,
    TransferStep,
    TransferPhase,
    ConvertStep,
    MergeGroupStep,
    OutputStepStack,
    ProcessingPhase,
)


class _QueuedSignalEmitter:
    def __init__(self, event_queue: Queue[tuple[str, tuple[Any, ...]]], event_name: str):
        self._event_queue = event_queue
        self._event_name = event_name

    def emit(self, *args: Any) -> None:
        self._event_queue.put((self._event_name, args))


class _PipelineWorkerView:
    def __init__(self, owner: "WorkflowExecutor", event_queue: Queue[tuple[str, tuple[Any, ...]]]):
        self._owner = owner
        self._event_queue = event_queue
        self._cancel = owner._cancel
        self._convert_func = owner._convert_func
        self._concat_func = owner._concat_func
        self._youtube_convert_func = owner._youtube_convert_func
        self.log_message = _QueuedSignalEmitter(event_queue, "log_message")
        self.job_progress = _QueuedSignalEmitter(event_queue, "job_progress")
        self.phase_changed = _QueuedSignalEmitter(event_queue, "phase_changed")
        self.file_progress = _QueuedSignalEmitter(event_queue, "file_progress")
        self.convert_progress = _QueuedSignalEmitter(event_queue, "convert_progress")
        self.source_status = _QueuedSignalEmitter(event_queue, "source_status")
        self.source_progress = _QueuedSignalEmitter(event_queue, "source_progress")

    def _set_job_status(self, orig_idx: int, status: str) -> None:
        self._event_queue.put(("job_status", (orig_idx, status)))

    @staticmethod
    def _set_step_status(job: WorkflowJob, step: str, status: str) -> None:
        WorkflowExecutor._set_step_status(job, step, status)

    def _find_file_entry(self, job: WorkflowJob, file_path: str) -> FileEntry | None:
        return self._owner._find_file_entry(job, file_path)

    def _build_job_settings(self, job: WorkflowJob) -> AppSettings:
        return self._owner._build_job_settings(job)

    def _get_merge_group_id(self, job: WorkflowJob, file_path: str) -> str:
        return self._owner._get_merge_group_id(job, file_path)

    def _resolve_youtube_title(self, job: WorkflowJob, file_path: str) -> str:
        return self._owner._resolve_youtube_title(job, file_path)


class WorkflowExecutor(QObject):
    """Führt einen Workflow zweiphasig aus."""

    log_message      = Signal(str)
    job_status       = Signal(int, str)    # (original_job_idx, status_text)
    job_progress     = Signal(int, int)    # (original_job_idx, 0-100)
    overall_progress = Signal(int, int)    # (done, total)
    file_progress    = Signal(str, str, float, float, float)
    phase_changed    = Signal(str)
    finished         = Signal(int, int, int)

    # Rückwärtskompatibilität
    source_status    = Signal(int, str)
    source_progress  = Signal(int, int)
    convert_progress = Signal(int, int)

    def __init__(self, workflow: Workflow, settings: AppSettings):
        super().__init__()
        self._workflow = workflow
        self._settings = settings
        self._cancel   = threading.Event()
        self._convert_func = run_convert
        self._concat_func = run_concat
        self._youtube_convert_func = run_youtube_convert
        self._download_func = download_device
        self._support = ExecutorSupport()
        self._transfer_step = TransferStep()
        self._transfer_phase = TransferPhase()
        self._convert_step = ConvertStep()
        self._merge_step = MergeGroupStep()
        self._output_step_stack = OutputStepStack()
        self._processing_phase = ProcessingPhase()
        self._transfer_fail = 0

    def cancel(self) -> None:
        self._cancel.set()

    def _handle_direct_files(self, job: WorkflowJob) -> list[str]:
        return self._transfer_step._files_step.execute(self, 0, job)

    def _scan_folder(self, job: WorkflowJob) -> list[str]:
        return self._transfer_step._folder_scan_step.execute(self, 0, job)

    def _download_from_pi(self, orig_idx: int, job: WorkflowJob) -> list[str]:
        return self._transfer_step._pi_download_step.execute(self, orig_idx, job)

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    @Slot()
    def run(self) -> None:
        active = [(idx, job)
                  for idx, job in enumerate(self._workflow.jobs)
                  if job.enabled]
        if not active:
            self.finished.emit(0, 0, 0)
            return

        ok, skip, fail = self._run_pipelined(active)
        if self._cancel.is_set():
            self.log_message.emit("Phase 1 abgebrochen.")
            self.finished.emit(0, 0, 0)
            return

        total_fail = fail
        icon = "✅" if total_fail == 0 else "❌"
        self.log_message.emit(
            f"\n{icon} Fertig: {ok} OK, {skip} übersprungen, {total_fail} Fehler")
        self.finished.emit(ok, skip, total_fail)

    def _run_pipelined(self, active: list[tuple[int, WorkflowJob]]) -> tuple[int, int, int]:
        executor = self
        stats = {"ok": 0, "skip": 0, "fail": 0}
        stats_lock = threading.Lock()
        progress_lock = threading.Lock()
        merge_lock = threading.Lock()
        process_queue: Queue[tuple[str, Any] | None] = Queue()
        upload_queue: Queue[PreparedOutput | None] = Queue()
        event_queue: Queue[tuple[str, tuple[Any, ...]]] = Queue()
        merge_groups: dict[str, list[ConvertItem]] = defaultdict(list)
        total_steps = max(1, self._estimate_pipeline_total_steps(active))
        progress_done = 0
        worker_executor = _PipelineWorkerView(self, event_queue)

        def _bump_stat(key: str, amount: int = 1) -> None:
            with stats_lock:
                stats[key] += amount

        def _advance_progress() -> None:
            nonlocal progress_done
            with progress_lock:
                progress_done += 1
                self.overall_progress.emit(min(progress_done, total_steps), total_steps)

        needs_delivery_worker = any(job.upload_youtube or job.upload_kaderblick for _, job in active)
        yt_service = None
        if any(job.upload_youtube for _, job in active):
            self.log_message.emit("YouTube-Anmeldung …")
            yt_service = self._get_youtube_service()
            if not yt_service:
                self.log_message.emit(
                    "⚠ YouTube-Upload deaktiviert (Anmeldung fehlgeschlagen)"
                )

        kb_sort_index = self._build_pipeline_kaderblick_sort_index(active)

        def _process_worker() -> None:
            while True:
                task = process_queue.get()
                try:
                    if task is None:
                        return

                    kind, payload = task
                    if kind == "item":
                        ok_inc, skip_inc, fail_inc = self._process_pipeline_item(
                            worker_executor,
                            payload,
                            upload_queue if needs_delivery_worker else None,
                            yt_service,
                            kb_sort_index,
                        )
                    elif kind == "merge":
                        gid, group = payload
                        ok_inc, skip_inc, fail_inc = self._process_pipeline_merge_group(
                            worker_executor,
                            gid,
                            group,
                            upload_queue if needs_delivery_worker else None,
                            yt_service,
                            kb_sort_index,
                        )
                    else:
                        raise ValueError(f"Unbekannter Pipeline-Task: {kind!r}")

                    if ok_inc:
                        _bump_stat("ok", ok_inc)
                    if skip_inc:
                        _bump_stat("skip", skip_inc)
                    if fail_inc:
                        _bump_stat("fail", fail_inc)
                    _advance_progress()
                except Exception as exc:
                    _bump_stat("fail", 1)
                    self.log_message.emit(f"❌ Pipeline-Verarbeitung fehlgeschlagen: {exc}")
                    _advance_progress()
                finally:
                    process_queue.task_done()

        def _upload_worker() -> None:
            while True:
                prepared = upload_queue.get()
                try:
                    if prepared is None:
                        return
                    failures = self._output_step_stack.execute_delivery_steps(
                        worker_executor,
                        prepared,
                        yt_service,
                        kb_sort_index,
                    )
                    if failures:
                        _bump_stat("fail", failures)
                    _advance_progress()
                except Exception as exc:
                    _bump_stat("fail", 1)
                    self.log_message.emit(f"❌ Upload fehlgeschlagen: {exc}")
                    _advance_progress()
                finally:
                    upload_queue.task_done()

        process_thread = threading.Thread(target=_process_worker, name="workflow-process", daemon=True)
        process_thread.start()
        upload_thread = None
        if needs_delivery_worker:
            upload_thread = threading.Thread(target=_upload_worker, name="workflow-upload", daemon=True)
            upload_thread.start()

        self.phase_changed.emit("Pipeline – Transfer & Verarbeitung …")
        self.log_message.emit(
            f"\n{'═' * 60}"
            f"\n  🚦 Pipeline-Workflow  ({len(active)} Auftrag/Aufträge)"
            f"\n{'═' * 60}"
        )

        for active_pos, (orig_idx, job) in enumerate(active):
            if self._cancel.is_set():
                break

            dispatched_paths: set[str] = set()

            def _on_file_ready(file_path: str) -> None:
                dispatched_paths.add(file_path)
                self._dispatch_pipeline_item(orig_idx, job, file_path, process_queue, merge_groups, merge_lock)

            self._set_step_status(job, "transfer", "running")
            self._set_job_status(orig_idx, "Transfer …")
            try:
                files = self._transfer_step.execute(self, orig_idx, job, on_file_ready=_on_file_ready)
            except Exception as exc:
                self._set_step_status(job, "transfer", f"error: {exc}")
                self._set_job_status(orig_idx, f"Fehler: {exc}")
                job.error_msg = str(exc)
                self.log_message.emit(f"❌ {job.name}: {exc}")
                _bump_stat("fail", 1)
                _advance_progress()
                continue

            if self._cancel.is_set():
                break

            for file_path in files:
                if file_path in dispatched_paths:
                    continue
                self._dispatch_pipeline_item(orig_idx, job, file_path, process_queue, merge_groups, merge_lock)

            self._set_step_status(job, "transfer", "done")
            self._set_job_status(orig_idx, "Transfer OK")
            self._transfer_fail = stats["fail"]
            self.overall_progress.emit(active_pos + 1, max(len(active), total_steps))
            _advance_progress()
            self._drain_pipeline_events(event_queue)

        self._wait_for_queue(process_queue, event_queue)

        if not self._cancel.is_set():
            grouped_items = []
            with merge_lock:
                grouped_items = list(merge_groups.items())
            for gid, group in grouped_items:
                process_queue.put(("merge", (gid, group)))
            self._wait_for_queue(process_queue, event_queue)

        process_queue.put(None)
        process_thread.join()
        self._drain_pipeline_events(event_queue)

        if upload_thread is not None:
            self._wait_for_queue(upload_queue, event_queue)
            upload_queue.put(None)
            upload_thread.join()
            self._drain_pipeline_events(event_queue)

        self._transfer_fail = stats["fail"]
        return stats["ok"], stats["skip"], stats["fail"]

    def _dispatch_pipeline_item(
        self,
        orig_idx: int,
        job: WorkflowJob,
        file_path: str,
        process_queue: Queue[tuple[str, Any] | None],
        merge_groups: dict[str, list[ConvertItem]],
        merge_lock: threading.Lock,
    ) -> None:
        if self._cancel.is_set():
            return

        if job.convert_enabled:
            cv_job = self._build_convert_job(job, file_path)
        else:
            cv_job = ConvertJob(
                source_path=Path(file_path),
                job_type="convert",
                youtube_title=self._resolve_youtube_title(job, file_path),
                youtube_playlist=job.default_youtube_playlist,
            )
            cv_job.status = "Fertig"
            cv_job.output_path = Path(file_path)

        item = ConvertItem(orig_idx=orig_idx, job=job, cv_job=cv_job)
        merge_group_id = self._get_merge_group_id(job, file_path)
        if merge_group_id:
            with merge_lock:
                merge_groups[merge_group_id].append(item)
            if job.convert_enabled:
                process_queue.put(("item", item))
            return

        process_queue.put(("item", item))

    def _process_pipeline_item(
        self,
        executor_view: Any,
        item: ConvertItem,
        upload_queue: Queue[PreparedOutput | None] | None,
        yt_service,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> tuple[int, int, int]:
        merge_group_id = self._get_merge_group_id(item.job, str(item.cv_job.source_path))
        per_settings = self._build_job_settings(item.job)

        if item.job.convert_enabled:
            result = self._convert_step.execute(
                executor_view,
                item.orig_idx,
                item.job,
                item.cv_job,
                per_settings,
                0,
                1,
            )
        else:
            result = "ready"

        if self._cancel.is_set():
            return 0, 0, 0

        if result == "skipped":
            return 0, 1, 0

        if result not in {"ok", "ready"}:
            self._set_job_status(item.orig_idx, f"Fehler: {item.cv_job.error_msg[:60]}")
            self.job_progress.emit(item.orig_idx, 0)
            return 0, 0, 1

        if merge_group_id:
            return 1, 0, 0

        prepared = PreparedOutput(
            orig_idx=item.orig_idx,
            job=item.job,
            cv_job=item.cv_job,
            per_settings=per_settings,
            mark_finished=not self._needs_delivery(item.job),
        )
        failures = self._output_step_stack.execute_processing_steps(
            executor_view,
            prepared,
        )
        if failures:
            return 0, 0, failures

        if self._needs_delivery(item.job):
            if upload_queue is not None:
                upload_queue.put(prepared)
            else:
                failures = self._output_step_stack.execute_delivery_steps(
                    executor_view,
                    prepared,
                    yt_service,
                    kb_sort_index,
                )
                if failures:
                    return 0, 0, failures
        else:
            self._output_step_stack.execute_delivery_steps(
                executor_view,
                prepared,
                yt_service,
                kb_sort_index,
            )
        return 1, 0, 0

    def _process_pipeline_merge_group(
        self,
        executor_view: Any,
        merge_group_id: str,
        group: list[ConvertItem],
        upload_queue: Queue[PreparedOutput | None] | None,
        yt_service,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> tuple[int, int, int]:
        prepared, merge_fail = self._merge_step.execute(executor_view, merge_group_id, group)
        if merge_fail:
            return 0, 0, merge_fail
        if prepared is None:
            return 0, 0, 0

        prepared.mark_finished = not self._needs_delivery(prepared.job)
        failures = self._output_step_stack.execute_processing_steps(executor_view, prepared)
        if failures:
            return 0, 0, failures

        if self._needs_delivery(prepared.job):
            if upload_queue is not None:
                upload_queue.put(prepared)
            else:
                failures = self._output_step_stack.execute_delivery_steps(
                    executor_view,
                    prepared,
                    yt_service,
                    kb_sort_index,
                )
                if failures:
                    return 0, 0, failures
        else:
            self._output_step_stack.execute_delivery_steps(
                executor_view,
                prepared,
                yt_service,
                kb_sort_index,
            )
        return 1, 0, 0

    def _drain_pipeline_events(self, event_queue: Queue[tuple[str, tuple[Any, ...]]]) -> None:
        while not event_queue.empty():
            event_name, args = event_queue.get()
            try:
                if event_name == "job_status":
                    self._set_job_status(*args)
                elif event_name == "job_progress":
                    self.job_progress.emit(*args)
                elif event_name == "phase_changed":
                    self.phase_changed.emit(*args)
                elif event_name == "log_message":
                    self.log_message.emit(*args)
                elif event_name == "file_progress":
                    self.file_progress.emit(*args)
                elif event_name == "convert_progress":
                    self.convert_progress.emit(*args)
                elif event_name == "source_status":
                    self.source_status.emit(*args)
                elif event_name == "source_progress":
                    self.source_progress.emit(*args)
            finally:
                event_queue.task_done()

    def _wait_for_queue(self, work_queue: Queue[Any], event_queue: Queue[tuple[str, tuple[Any, ...]]]) -> None:
        while work_queue.unfinished_tasks:
            self._drain_pipeline_events(event_queue)
            time.sleep(0.01)
        self._drain_pipeline_events(event_queue)

    @staticmethod
    def _needs_delivery(job: WorkflowJob) -> bool:
        return bool(job.upload_youtube or job.upload_kaderblick)

    def _build_pipeline_kaderblick_sort_index(
        self,
        active: list[tuple[int, WorkflowJob]],
    ) -> dict[tuple[str, str], int]:
        kb_by_game: dict[str, list[str]] = defaultdict(list)
        for _, job in active:
            if not (job.upload_kaderblick and job.upload_youtube):
                continue

            candidates: list[tuple[str, str]] = []
            if job.files:
                for entry in job.files:
                    game_id = entry.kaderblick_game_id or job.default_kaderblick_game_id
                    name = Path(entry.source_path).name if entry.source_path else ""
                    if game_id and name:
                        candidates.append((game_id, name))
            elif job.source_mode == "folder_scan" and job.source_folder:
                src_dir = Path(job.source_folder)
                pattern = job.file_pattern or "*.mp4"
                if src_dir.exists():
                    for path in sorted(src_dir.glob(pattern)):
                        if not path.is_file():
                            continue
                        game_id = job.default_kaderblick_game_id
                        if game_id:
                            candidates.append((game_id, path.name))

            for game_id, name in candidates:
                kb_by_game[game_id].append(name)

        kb_sort_index: dict[tuple[str, str], int] = {}
        for game_id, names in kb_by_game.items():
            for pos, name in enumerate(sorted(set(names)), start=1):
                kb_sort_index[(game_id, name)] = pos
        return kb_sort_index

    def _estimate_pipeline_total_steps(self, active: list[tuple[int, WorkflowJob]]) -> int:
        total = len(active)
        for _, job in active:
            file_count = self._estimate_job_file_count(job)
            merge_groups = {
                entry.merge_group_id
                for entry in job.files
                if getattr(entry, "merge_group_id", "")
            }

            if job.convert_enabled:
                total += file_count
            total += len(merge_groups)

            if job.upload_youtube:
                if merge_groups:
                    total += len(merge_groups)
                else:
                    total += max(1, file_count)
        return total

    @staticmethod
    def _estimate_job_file_count(job: WorkflowJob) -> int:
        if job.files:
            return len(job.files)
        if job.source_mode == "folder_scan" and job.source_folder:
            src_dir = Path(job.source_folder)
            pattern = job.file_pattern or "*.mp4"
            if src_dir.exists():
                return len([path for path in src_dir.glob(pattern) if path.is_file()])
        return 1

    # ── ConvertJob erstellen ──────────────────────────────────

    def _build_convert_job(self, job: WorkflowJob, file_path: str) -> ConvertJob:
        return self._support.build_convert_job(self, job, file_path)

    @staticmethod
    def _find_file_entry(job: WorkflowJob, file_path: str) -> FileEntry | None:
        return ExecutorSupport.find_file_entry(job, file_path)

    @classmethod
    def _get_merge_group_id(cls, job: WorkflowJob, file_path: str) -> str:
        return ExecutorSupport.get_merge_group_id(job, file_path)

    @staticmethod
    def _resolve_youtube_title(job: WorkflowJob, file_path: str) -> str:
        return ExecutorSupport.resolve_youtube_title(job, file_path)

    # ── Settings pro Job ─────────────────────────────────────

    def _build_job_settings(self, job: WorkflowJob) -> AppSettings:
        return self._support.build_job_settings(self, job)

    def _run_output_steps(
        self,
        prepared: PreparedOutput,
        yt_service,
        kb_sort_index: dict[tuple[str, str], int],
        *,
        include_title_card: bool = True,
        include_youtube_version: bool = True,
    ) -> int:
        """Zentraler Step-Stack für finale Ausgabe-Artefakte.

        Jede Ergebnisdatei oder Merge-Gruppe läuft durch dieselben optionalen Steps,
        zusammengesetzt aus der Job-Konfiguration.
        """
        return self._output_step_stack.execute(
            self,
            prepared,
            yt_service,
            kb_sort_index,
            include_title_card=include_title_card,
            include_youtube_version=include_youtube_version,
        )

    def _get_youtube_service(self):
        return get_youtube_service(log_callback=self.log_message.emit)

    # ── Status-Helfer ─────────────────────────────────────────

    def _set_job_status(self, orig_idx: int, status: str) -> None:
        """Emit job_status and keep backward-compat alias in sync."""
        self.job_status.emit(orig_idx, status)
        self.source_status.emit(orig_idx, status)

    @staticmethod
    def _set_step_status(job: WorkflowJob, step: str, status: str) -> None:
        if not isinstance(job.step_statuses, dict):
            job.step_statuses = {}
        job.step_statuses[step] = status
        job.current_step_key = step

