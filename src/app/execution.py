"""Workflow execution actions and slots for the main window."""

from __future__ import annotations

import subprocess
import time

from PySide6.QtCore import Slot

from .helpers import _compute_job_overall_progress, _format_resume_tooltip, _job_has_source_config


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
    self._wf_executor = WorkflowExecutor(self._workflow, self.settings, active_indices=active_indices)
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

    self._wf_start_time = time.monotonic()
    self._wf_thread.start()


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
        item = self.table.item(orig_idx, 4)
        if item is not None:
            item.setToolTip(_format_resume_tooltip(job))
        self._set_row_job_progress(orig_idx, overall_pct)
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
        if 0 <= orig_idx < self.table.rowCount():
            self._set_row_job_progress(orig_idx, overall_pct)


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
    elapsed = time.monotonic() - self._wf_start_time
    self.progress.setMaximum(total)
    self.progress.setValue(done)
    self.status_label.setText(f"Schritt {done}/{total}  ({self._format_duration(elapsed)})")


@Slot(int, int, int)
def _on_workflow_done(self, ok: int, skip: int, fail: int):
    if self._wf_thread:
        self._wf_thread.quit()
        self._wf_thread.wait()
        self._wf_thread = None
        self._wf_executor = None

    elapsed = time.monotonic() - self._wf_start_time
    if fail > 0:
        msg = f"❌ FEHLER: {fail} Fehler, {ok} OK, {skip} übersprungen  ({self._format_duration(elapsed)})"
        self.status_label.setStyleSheet("color: white; background: #c0392b; font-weight: bold; padding: 2px 6px;")
    else:
        msg = f"✅ Fertig: {ok} OK, {skip} übersprungen  ({self._format_duration(elapsed)})"
        self.status_label.setStyleSheet("color: white; background: #27ae60; font-weight: bold; padding: 2px 6px;")
    self._append_log(f"\n{msg}")
    self.status_label.setText(msg)
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