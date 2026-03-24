"""Workflow execution actions and slots for the main window."""

from __future__ import annotations

import subprocess
import time
from datetime import datetime

from PySide6.QtCore import Slot

from .helpers import _compute_job_overall_progress, _format_resume_tooltip, _job_has_source_config, format_elapsed_seconds


def _start_selected_workflows(self):
    from . import QMessageBox

    selected_rows = self._selected_job_rows()
    if not selected_rows:
        active_jobs = [job for job in self._workflow.jobs if job.enabled]
        if not active_jobs:
            QMessageBox.information(self, "Hinweis", "Kein aktiver Workflow vorhanden.")
            return
        choice = QMessageBox.question(
            self,
            "Keine Auswahl",
            "Es ist kein Workflow ausgewählt. Sollen alle aktiven Workflows gestartet werden?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if choice != QMessageBox.StandardButton.Yes:
            return
        self._start_workflow(active_indices={index for index, job in enumerate(self._workflow.jobs) if job.enabled})
        return
    self._start_workflow(active_indices=set(selected_rows))


def _start_all_active_workflows(self):
    self._start_workflow(active_indices={index for index, job in enumerate(self._workflow.jobs) if job.enabled})


def _start_workflow(self, *, active_indices: set[int] | None = None):
    from . import QThread, QMessageBox, WorkflowExecutor

    active_jobs = [
        job
        for index, job in enumerate(self._workflow.jobs)
        if job.enabled and (active_indices is None or index in active_indices)
    ]
    if not active_jobs:
        QMessageBox.information(self, "Hinweis", "Kein aktiver Workflow in der Auswahl vorhanden.")
        return

    if self._wf_thread and self._wf_thread.isRunning():
        return

    resume_existing = False
    if any(
        self._workflow.jobs[index].enabled
        and _job_has_source_config(self._workflow.jobs[index])
        and (self._workflow.jobs[index].resume_status or self._workflow.jobs[index].step_statuses)
        for index in (active_indices or set(range(len(self._workflow.jobs))))
        if 0 <= index < len(self._workflow.jobs)
    ):
        choice = self._ask_resume_behavior()
        if choice == QMessageBox.StandardButton.Cancel:
            return
        resume_existing = choice == QMessageBox.StandardButton.Yes

    if not resume_existing:
        self._reset_status_column()
        for job in active_jobs:
            job.resume_status = ""
            job.step_statuses = {}
            job.step_details = {}
            job.progress_pct = 0
            job.overall_progress_pct = 0
            job.current_step_key = ""
            job.transfer_status = ""
            job.transfer_progress_pct = 0
            job.run_started_at = ""
            job.run_finished_at = ""
            job.run_elapsed_seconds = 0.0
        self._workflow.last_run_started_at = ""
        self._workflow.last_run_finished_at = ""
        self._workflow.last_run_elapsed_seconds = 0.0
        self._save_last_workflow()
    else:
        self._append_log("Fortsetzen vorhandener Workflow-Sitzung …")

    self.status_label.setStyleSheet("")
    self._set_busy(True)
    self._append_log(f"\n{'═'*60}\n  ▶ Workflow {'fortgesetzt' if resume_existing else 'gestartet'}\n{'═'*60}")
    self.progress.setMaximum(1)
    self.progress.setValue(0)

    self._workflow.shutdown_after = self._shutdown_cb.isChecked()
    self._wf_thread = QThread(self)
    self._wf_executor = WorkflowExecutor(
        self._workflow,
        self.settings,
        active_indices=active_indices,
        allow_reuse_existing=resume_existing,
    )
    self._wf_executor.moveToThread(self._wf_thread)

    self._wf_thread.started.connect(self._wf_executor.run)
    self._wf_executor.log_message.connect(self._append_log)
    self._wf_executor.job_status.connect(self._on_job_status)
    self._wf_executor.job_progress.connect(self._on_job_progress)
    if hasattr(self._wf_executor, "source_status"):
        self._wf_executor.source_status.connect(self._on_source_status)
    if hasattr(self._wf_executor, "source_progress"):
        self._wf_executor.source_progress.connect(self._on_source_progress)
    self._wf_executor.file_progress.connect(self._on_dl_progress)
    self._wf_executor.overall_progress.connect(self._on_overall_progress)
    self._wf_executor.phase_changed.connect(self._on_phase_changed)
    self._wf_executor.finished.connect(self._on_workflow_done)

    now_monotonic = time.monotonic()
    now_iso = _now_iso()
    self._active_run_indices = {
        index
        for index, job in enumerate(self._workflow.jobs)
        if job.enabled and (active_indices is None or index in active_indices)
    }
    self._wf_start_time = now_monotonic
    self._workflow_run_elapsed_base_seconds = float(self._workflow.last_run_elapsed_seconds or 0.0) if resume_existing else 0.0
    self._workflow_run_started_monotonic = now_monotonic
    if not resume_existing or not self._workflow.last_run_started_at:
        self._workflow.last_run_started_at = now_iso
    self._workflow.last_run_finished_at = ""
    self._job_run_started_monotonic = {}
    self._job_run_elapsed_base_seconds = {}
    for index in self._active_run_indices:
        if not (0 <= index < len(self._workflow.jobs)):
            continue
        job = self._workflow.jobs[index]
        base_seconds = float(job.run_elapsed_seconds or 0.0) if resume_existing else 0.0
        self._job_run_elapsed_base_seconds[job.id] = base_seconds
        self._job_run_started_monotonic[job.id] = now_monotonic
        if not resume_existing or not job.run_started_at:
            job.run_started_at = now_iso
        job.run_finished_at = ""
        if not resume_existing:
            job.run_elapsed_seconds = 0.0

    self._wf_thread.start()
    if hasattr(self, "_duration_timer"):
        self._duration_timer.start()
    self._refresh_runtime_durations()


def _cancel_workflow(self):
    if self._wf_executor:
        self._wf_executor.cancel()
    self._append_log("Abbruch angefordert …")


@Slot(int, str)
def _on_job_status(self, orig_idx: int, status: str):
    if 0 <= orig_idx < self.table.rowCount():
        self._set_row_status(orig_idx, status)
    if 0 <= orig_idx < len(self._workflow.jobs):
        job = self._workflow.jobs[orig_idx]
        job.resume_status = status
        overall_pct = _compute_job_overall_progress(job, status, job.progress_pct)
        job.overall_progress_pct = overall_pct
        if _is_terminal_job_status(status, overall_pct):
            _freeze_job_duration(self, orig_idx)
        else:
            _touch_job_duration(self, orig_idx)
        item = self.table.item(orig_idx, 4)
        if item is not None:
            item.setToolTip(_format_resume_tooltip(job))
        self._set_row_job_progress(orig_idx, overall_pct)
        self._set_row_duration(orig_idx, job.run_elapsed_seconds)
        self._save_last_workflow()


@Slot(int, int)
def _on_job_progress(self, orig_idx: int, pct: int):
    if 0 <= orig_idx < self.table.rowCount():
        self._set_row_progress(orig_idx, pct)
    if 0 <= orig_idx < len(self._workflow.jobs):
        job = self._workflow.jobs[orig_idx]
        job.progress_pct = pct
        overall_pct = _compute_job_overall_progress(job, job.resume_status or job.status, pct)
        job.overall_progress_pct = overall_pct
        _touch_job_duration(self, orig_idx)
        if 0 <= orig_idx < self.table.rowCount():
            self._set_row_job_progress(orig_idx, overall_pct)
            self._set_row_duration(orig_idx, job.run_elapsed_seconds)


@Slot(int, str)
def _on_source_status(self, orig_idx: int, status: str):
    if 0 <= orig_idx < len(self._workflow.jobs):
        self._workflow.jobs[orig_idx].transfer_status = status


@Slot(int, int)
def _on_source_progress(self, orig_idx: int, pct: int):
    if 0 <= orig_idx < len(self._workflow.jobs):
        self._workflow.jobs[orig_idx].transfer_progress_pct = pct


@Slot(str, str, float, float, float)
def _on_dl_progress(self, device: str, filename: str, transferred: float, total: float, speed_bps: float):
    if total > 0:
        pct = int(transferred / total * 100)
        info = f"⬇ {device}: {filename}  {pct}%"
        if speed_bps > 0:
            speed_mb = speed_bps / 1048576
            remaining = total - transferred
            eta_s = remaining / speed_bps
            if eta_s >= 3600:
                eta_str = f"{int(eta_s // 3600)}h {int((eta_s % 3600) // 60)}min"
            elif eta_s >= 60:
                eta_str = f"{int(eta_s // 60)}min {int(eta_s % 60)}s"
            else:
                eta_str = f"{int(eta_s)}s"
            info += f"  –  {speed_mb:.1f} MB/s  ETA {eta_str}"
        self.status_label.setText(info)
    else:
        self.status_label.setText(f"⬇ {device}: {filename}")


@Slot(str)
def _on_phase_changed(self, phase: str):
    self.status_label.setText(phase)


@Slot(int, int)
def _on_overall_progress(self, done: int, total: int):
    elapsed = _effective_workflow_elapsed_seconds(self)
    self.progress.setMaximum(total)
    self.progress.setValue(done)
    self.status_label.setText(f"Schritt {done}/{total}  ({self._format_duration(elapsed)})")
    if hasattr(self, "duration_label"):
        self.duration_label.setText(f"Gesamtdauer: {format_elapsed_seconds(elapsed)}")


@Slot(int, int, int)
def _on_workflow_done(self, ok: int, skip: int, fail: int):
    _snapshot_runtime_durations(self)
    self._workflow.last_run_finished_at = _now_iso()
    if self._wf_thread:
        self._wf_thread.quit()
        self._wf_thread.wait()
        self._wf_thread = None
        self._wf_executor = None
    if hasattr(self, "_duration_timer"):
        self._duration_timer.stop()

    elapsed = float(self._workflow.last_run_elapsed_seconds or 0.0)
    if fail > 0:
        msg = f"❌ FEHLER: {fail} Fehler, {ok} OK, {skip} übersprungen  ({self._format_duration(elapsed)})"
        self.status_label.setStyleSheet("color: white; background: #c0392b; font-weight: bold; padding: 2px 6px;")
    else:
        msg = f"✅ Fertig: {ok} OK, {skip} übersprungen  ({self._format_duration(elapsed)})"
        self.status_label.setStyleSheet("color: white; background: #27ae60; font-weight: bold; padding: 2px 6px;")
    self._append_log(f"\n{msg}")
    self.status_label.setText(msg)
    if hasattr(self, "duration_label"):
        self.duration_label.setText(f"Gesamtdauer: {format_elapsed_seconds(elapsed)}")
    self._set_busy(False)
    self._save_last_workflow()

    if self._workflow.shutdown_after and fail == 0:
        from ..ui.dialogs import ShutdownCountdownDialog

        dialog = ShutdownCountdownDialog(seconds=30, parent=self)
        if dialog.exec():
            self._append_log("\n⏻ Rechner wird heruntergefahren …")
            subprocess.Popen(["shutdown", "now"])
        else:
            self._append_log("\n⚠ Herunterfahren durch Benutzer abgebrochen.")
    elif self._workflow.shutdown_after and fail > 0:
        self._append_log("\n⚠ Herunterfahren übersprungen wegen Fehlern.")


def _refresh_runtime_durations(self):
    _snapshot_runtime_durations(self, persist=False)


def _snapshot_runtime_durations(self, *, persist: bool = False):
    if not hasattr(self, "_active_run_indices"):
        self._active_run_indices = set()
    for orig_idx in sorted(self._active_run_indices):
        if not (0 <= orig_idx < len(self._workflow.jobs)):
            continue
        job = self._workflow.jobs[orig_idx]
        job.run_elapsed_seconds = _effective_job_elapsed_seconds(self, job)
        if 0 <= orig_idx < self.table.rowCount():
            self._set_row_duration(orig_idx, job.run_elapsed_seconds)
            item = self.table.item(orig_idx, 4)
            if item is not None:
                item.setToolTip(_format_resume_tooltip(job))

    self._workflow.last_run_elapsed_seconds = _effective_workflow_elapsed_seconds(self)
    if hasattr(self, "duration_label"):
        self.duration_label.setText(
            f"Gesamtdauer: {('–' if self._workflow.last_run_elapsed_seconds <= 0 else format_elapsed_seconds(self._workflow.last_run_elapsed_seconds))}"
        )

    if persist:
        self._save_last_workflow()


def _effective_job_elapsed_seconds(self, job) -> float:
    if not getattr(job, "id", ""):
        return float(getattr(job, "run_elapsed_seconds", 0.0) or 0.0)
    base = float(self._job_run_elapsed_base_seconds.get(job.id, getattr(job, "run_elapsed_seconds", 0.0) or 0.0))
    started = self._job_run_started_monotonic.get(job.id)
    if started is None or getattr(job, "run_finished_at", ""):
        return base
    return base + max(0.0, time.monotonic() - started)


def _effective_workflow_elapsed_seconds(self) -> float:
    base = float(getattr(self, "_workflow_run_elapsed_base_seconds", 0.0) or 0.0)
    started = getattr(self, "_workflow_run_started_monotonic", 0.0) or 0.0
    if not started or getattr(self._workflow, "last_run_finished_at", ""):
        return float(getattr(self._workflow, "last_run_elapsed_seconds", base) or base)
    return base + max(0.0, time.monotonic() - started)


def _touch_job_duration(self, orig_idx: int):
    if not (0 <= orig_idx < len(self._workflow.jobs)):
        return
    job = self._workflow.jobs[orig_idx]
    if job.id not in self._job_run_started_monotonic:
        self._job_run_elapsed_base_seconds[job.id] = float(job.run_elapsed_seconds or 0.0)
        self._job_run_started_monotonic[job.id] = time.monotonic()
        if not job.run_started_at:
            job.run_started_at = _now_iso()
    job.run_elapsed_seconds = _effective_job_elapsed_seconds(self, job)


def _freeze_job_duration(self, orig_idx: int):
    if not (0 <= orig_idx < len(self._workflow.jobs)):
        return
    _touch_job_duration(self, orig_idx)
    job = self._workflow.jobs[orig_idx]
    base = _effective_job_elapsed_seconds(self, job)
    job.run_elapsed_seconds = base
    job.run_finished_at = _now_iso()
    self._job_run_elapsed_base_seconds[job.id] = base
    self._job_run_started_monotonic.pop(job.id, None)


def _is_terminal_job_status(status: str, overall_pct: int) -> bool:
    if status in {"Übersprungen", "Abgebrochen"}:
        return True
    if status.startswith("Fehler"):
        return True
    if status.startswith("Fertig") and overall_pct >= 100:
        return True
    return False


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")