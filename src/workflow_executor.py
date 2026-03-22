"""Workflow-Executor: Führt einen Workflow aus.

Wandelt WorkflowJob-Objekte in ausführbare Schritte um:
  1. Transfer-Phase  (Download / Kopieren / Verschieben)
  2. Verarbeitungs-Phase  (Konvertierung, YouTube-Upload)

Signale
-------
log_message(str)
job_status(int, str)         – (original_job_idx, status_text)  → aktualisiert Tabellenzeile
job_progress(int, int)       – (original_job_idx, percent 0-100) → Fortschritt des aktuellen Steps pro Zeile
overall_progress(int, int)   – (done, total)
file_progress(str, str, float, float, float)  – Download-Fortschritt
phase_changed(str)           – z. B. „Phase 1 – Downloads …"
finished(int, int, int)      – (ok, skipped, failed)
"""

import threading
from pathlib import Path

from PySide6.QtCore import QObject, Signal, Slot

from .settings import AppSettings
from .converter import ConvertJob, run_convert, run_youtube_convert, run_concat
from .downloader import download_device
from .youtube import get_youtube_service
from .workflow import Workflow, WorkflowJob, FileEntry
from .workflow_steps import (
    PreparedOutput,
    ExecutorSupport,
    TransferStep,
    TransferPhase,
    ConvertStep,
    MergeGroupStep,
    OutputStepStack,
    ProcessingPhase,
)


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

        transfer_result = self._transfer_phase.execute(self, active)
        self._transfer_fail = transfer_result.transfer_fail
        if transfer_result.cancelled:
            self.log_message.emit("Phase 1 abgebrochen.")
            self.finished.emit(0, 0, 0)
            return

        processing_result = self._processing_phase.execute(self, transfer_result.convert_items)
        total_fail = processing_result.fail + self._transfer_fail
        icon = "✅" if total_fail == 0 else "❌"
        self.log_message.emit(
            f"\n{icon} Fertig: {processing_result.ok} OK, {processing_result.skip} übersprungen, {total_fail} Fehler")
        self.finished.emit(processing_result.ok, processing_result.skip, total_fail)

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

