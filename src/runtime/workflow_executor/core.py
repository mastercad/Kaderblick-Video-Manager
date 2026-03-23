"""Workflow-Executor mit konservativer Pipeline-Ausführung."""

import threading

from PySide6.QtCore import QObject, Signal, Slot

from ...settings import AppSettings
from ...workflow import Workflow
from ...workflow_steps import (
    ConvertStep,
    ExecutorSupport,
    MergeGroupStep,
    OutputStepStack,
    ProcessingPhase,
    TransferPhase,
    TransferStep,
)
from .pipeline import WorkflowExecutorPipelineMixin
from .support import WorkflowExecutorSupportMixin


class WorkflowExecutor(WorkflowExecutorPipelineMixin, WorkflowExecutorSupportMixin, QObject):
    """Führt einen Workflow zweiphasig aus."""

    log_message = Signal(str)
    job_status = Signal(int, str)
    job_progress = Signal(int, int)
    overall_progress = Signal(int, int)
    file_progress = Signal(str, str, float, float, float)
    phase_changed = Signal(str)
    finished = Signal(int, int, int)

    source_status = Signal(int, str)
    source_progress = Signal(int, int)
    convert_progress = Signal(int, int)

    def __init__(self, workflow: Workflow, settings: AppSettings, *, active_indices: set[int] | None = None):
        super().__init__()
        from . import download_device, run_concat, run_convert, run_youtube_convert

        self._workflow = workflow
        self._settings = settings
        self._active_indices = set(active_indices or set())
        self._cancel = threading.Event()
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

    def _handle_direct_files(self, job):
        return self._transfer_step._files_step.execute(self, 0, job)

    def _scan_folder(self, job):
        return self._transfer_step._folder_scan_step.execute(self, 0, job)

    def _download_from_pi(self, orig_idx, job):
        return self._transfer_step._pi_download_step.execute(self, orig_idx, job)

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    @Slot()
    def run(self) -> None:
        active = [
            (index, job)
            for index, job in enumerate(self._workflow.jobs)
            if job.enabled and (not self._active_indices or index in self._active_indices)
        ]
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
        self.log_message.emit(f"\n{icon} Fertig: {ok} OK, {skip} übersprungen, {total_fail} Fehler")
        self.finished.emit(ok, skip, total_fail)