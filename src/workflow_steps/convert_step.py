from __future__ import annotations

from pathlib import Path
from typing import Any

from ..converter import ConvertJob
from ..settings import AppSettings
from ..workflow import WorkflowJob


class ConvertStep:
    name = "convert"

    def execute(
        self,
        executor: Any,
        orig_idx: int,
        job: WorkflowJob,
        cv_job: ConvertJob,
        per_settings: AppSettings,
        done_count: int,
        total_count: int,
    ) -> str:
        existing_output = self._find_existing_output(cv_job, job, per_settings)
        if existing_output is not None and existing_output.exists() and not per_settings.video.overwrite:
            cv_job.output_path = existing_output
            cv_job.status = "Fertig"
            executor._set_step_status(job, "convert", "reused-target")
            executor._set_job_status(orig_idx, f"Konvertierung OK (vorhanden): {existing_output.name}")
            executor.job_progress.emit(orig_idx, 100)
            return "ready"

        cv_job.status = "Läuft"
        cv_job.progress_pct = 0
        executor._set_step_status(job, "convert", "running")
        executor._set_job_status(orig_idx, "Konvertiere …")
        executor.job_progress.emit(orig_idx, 0)

        def _progress(pct: int, _oi=orig_idx, _done=done_count, _tot=total_count, _cv=cv_job):
            _cv.progress_pct = pct
            composite = int((_done + pct / 100.0) / _tot * 100) if _tot else pct
            executor.job_progress.emit(_oi, composite)
            executor.convert_progress.emit(_done, pct)

        success = executor._convert_func(
            cv_job,
            per_settings,
            cancel_flag=executor._cancel,
            log_callback=executor.log_message.emit,
            progress_callback=_progress,
        )

        if success and cv_job.status == "Fertig":
            executor._set_step_status(job, "convert", "done")
            return "ok"
        if cv_job.status == "Übersprungen":
            executor._set_step_status(job, "convert", "reused-target")
            executor.job_progress.emit(orig_idx, 100)
            return "ready"
        executor._set_step_status(job, "convert", "error")
        return "error"

    @staticmethod
    def _find_existing_output(cv_job: ConvertJob, job: WorkflowJob, per_settings: AppSettings) -> Path | None:
        if cv_job.output_path:
            return cv_job.output_path

        ext = "mp4" if per_settings.video.output_format == "mp4" else "avi"
        out_path = cv_job.source_path.with_suffix(f".{ext}")
        if out_path == cv_job.source_path:
            out_path = cv_job.source_path.with_stem(f"{cv_job.source_path.stem}_converted").with_suffix(f".{ext}")
        return out_path
