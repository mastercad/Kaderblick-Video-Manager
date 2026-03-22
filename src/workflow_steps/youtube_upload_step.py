from __future__ import annotations

from typing import Any

from ..youtube import get_video_id_for_output, upload_to_youtube
from .models import PreparedOutput


class YoutubeUploadStep:
    name = "youtube_upload"

    def execute(self, executor: Any, prepared: PreparedOutput, yt_service: Any) -> int:
        if not (prepared.job.upload_youtube and yt_service):
            return 0
        existing_video_id = None
        if prepared.cv_job.output_path:
            existing_video_id = get_video_id_for_output(prepared.cv_job.output_path)
        if existing_video_id:
            executor.phase_changed.emit("YouTube-Upload …")
            executor._set_step_status(prepared.job, "youtube_upload", "reused-target")
            executor._set_job_status(prepared.orig_idx, f"YouTube-Upload OK (vorhanden): {existing_video_id}")
            executor.job_progress.emit(prepared.orig_idx, 100)
            return 0
        executor.phase_changed.emit("YouTube-Upload …")
        executor._set_step_status(prepared.job, "youtube_upload", "running")
        executor._set_job_status(prepared.orig_idx, prepared.status_prefix or "YouTube-Upload …")
        executor.job_progress.emit(prepared.orig_idx, 0)
        yt_ok = self._upload_to_youtube(
            executor,
            prepared.cv_job,
            prepared.per_settings,
            yt_service,
            prepared.orig_idx,
        )
        if not yt_ok:
            executor._set_step_status(prepared.job, "youtube_upload", "error")
            executor._set_job_status(prepared.orig_idx, "Fehler: YouTube-Upload fehlgeschlagen")
            return 1
        executor._set_step_status(prepared.job, "youtube_upload", "done")
        executor.job_progress.emit(prepared.orig_idx, 100)
        return 0

    @staticmethod
    def _upload_to_youtube(executor: Any, cv_job, settings, yt_service: Any, orig_idx: int) -> bool:
        return upload_to_youtube(
            cv_job,
            settings,
            yt_service,
            log_callback=executor.log_message.emit,
            cancel_flag=executor._cancel,
            progress_callback=lambda pct: executor.job_progress.emit(orig_idx, pct),
        )
