from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import PreparedOutput


class YoutubeVersionStep:
    name = "yt_version"

    def execute(self, executor: Any, prepared: PreparedOutput) -> int:
        if not (prepared.job.create_youtube_version and prepared.cv_job.output_path):
            return 0
        existing = self._existing_youtube_version(prepared)
        if existing is not None and existing.exists() and not prepared.per_settings.video.overwrite:
            executor._set_step_status(prepared.job, "yt_version", "reused-target")
            executor._set_job_status(prepared.orig_idx, f"YT-Version OK (vorhanden): {existing.name}")
            executor.job_progress.emit(prepared.orig_idx, 100)
            return 0
        if not prepared.cv_job.output_path.exists():
            return 0
        executor._set_step_status(prepared.job, "yt_version", "running")
        executor._set_job_status(prepared.orig_idx, "YT-Version erstellen …")
        executor.job_progress.emit(prepared.orig_idx, 0)
        ok = executor._youtube_convert_func(
            prepared.cv_job,
            prepared.per_settings,
            cancel_flag=executor._cancel,
            log_callback=executor.log_message.emit,
            progress_callback=lambda pct: executor.job_progress.emit(prepared.orig_idx, pct),
        )
        if not ok:
            executor._set_step_status(prepared.job, "yt_version", "error")
            executor._set_job_status(prepared.orig_idx, "YT-Version fehlgeschlagen")
            executor.log_message.emit(
                "⚠ YouTube-Version konnte nicht erstellt werden – Original wird hochgeladen."
            )
            return 0
        executor._set_step_status(prepared.job, "yt_version", "done")
        executor.job_progress.emit(prepared.orig_idx, 100)
        return 0

    @staticmethod
    def _existing_youtube_version(prepared: PreparedOutput) -> Path | None:
        output_path = prepared.cv_job.output_path
        if output_path is None:
            return None
        return output_path.with_stem(output_path.stem + "_youtube")
