from __future__ import annotations

import time
from typing import Any

from ..integrations.youtube import get_registry_entry_for_output, get_video_id_for_output, upload_to_youtube
from .models import PreparedOutput


class YoutubeUploadStep:
    name = "youtube_upload"

    def execute(self, executor: Any, prepared: PreparedOutput, yt_service: Any) -> int:
        youtube_upload_enabled = prepared.youtube_upload_enabled_override
        if youtube_upload_enabled is None:
            youtube_upload_enabled = prepared.job.upload_youtube
        if not (youtube_upload_enabled and yt_service):
            return 0
        existing_video_id = None
        if prepared.cv_job.output_path:
            existing_video_id = get_video_id_for_output(prepared.cv_job.output_path)
        if existing_video_id:
            executor.phase_changed.emit("YouTube-Upload …")
            executor._set_step_status(prepared.job, "youtube_upload", "reused-target")
            executor._set_step_detail(
                prepared.job,
                "youtube_upload",
                self._build_summary(prepared, existing_video_id, 0.0, yt_service, existing=True),
            )
            executor._set_job_status(prepared.orig_idx, f"YouTube-Upload OK (vorhanden): {existing_video_id}")
            executor.job_progress.emit(prepared.orig_idx, 100)
            return 0
        executor.phase_changed.emit("YouTube-Upload …")
        executor._set_step_status(prepared.job, "youtube_upload", "running")
        executor._set_job_status(prepared.orig_idx, prepared.status_prefix or "YouTube-Upload …")
        executor.job_progress.emit(prepared.orig_idx, 0)
        started_at = time.monotonic()
        yt_ok = self._upload_to_youtube(
            executor,
            prepared.cv_job,
            prepared.per_settings,
            yt_service,
            prepared.orig_idx,
        )
        if not yt_ok:
            executor._set_step_status(prepared.job, "youtube_upload", "error")
            executor._set_step_detail(prepared.job, "youtube_upload", self._build_error_summary(prepared))
            executor._set_job_status(prepared.orig_idx, "Fehler: YouTube-Upload fehlgeschlagen")
            return 1
        executor._set_step_status(prepared.job, "youtube_upload", "done")
        video_id = get_video_id_for_output(prepared.cv_job.output_path) if prepared.cv_job.output_path else None
        executor._set_step_detail(
            prepared.job,
            "youtube_upload",
            self._build_summary(prepared, video_id, time.monotonic() - started_at, yt_service, existing=False),
        )
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

    @staticmethod
    def _build_error_summary(prepared: PreparedOutput) -> str:
        output = prepared.cv_job.output_path
        upload_file = output.with_stem(output.stem + "_youtube") if output is not None else None
        if upload_file is not None and not upload_file.exists():
            upload_file = output
        file_name = upload_file.name if upload_file is not None else "unbekannt"
        title = prepared.cv_job.youtube_title or (upload_file.stem if upload_file is not None else "")
        playlist = prepared.cv_job.youtube_playlist or "-"
        return f"Quelldatei: {file_name} | Titel: {title} | Playlist: {playlist} | Ergebnis: Upload fehlgeschlagen"

    @staticmethod
    def _build_summary(prepared: PreparedOutput, video_id: str | None, upload_seconds: float, yt_service: Any, *, existing: bool) -> str:
        output = prepared.cv_job.output_path
        upload_file = output.with_stem(output.stem + "_youtube") if output is not None else None
        if upload_file is not None and not upload_file.exists():
            upload_file = output
        file_name = upload_file.name if upload_file is not None else "unbekannt"
        registry_entry = get_registry_entry_for_output(output) if output is not None else None
        title = prepared.cv_job.youtube_title or (registry_entry.get("title") if registry_entry else "") or (upload_file.stem if upload_file is not None else "")
        playlist = prepared.cv_job.youtube_playlist or "-"
        result = "vorhanden" if existing else "hochgeladen"
        if video_id and yt_service:
            try:
                response = yt_service.videos().list(part="status,processingDetails", id=video_id).execute()
                item = next(iter(response.get("items") or []), {})
                status = item.get("status") or {}
                processing = item.get("processingDetails") or {}
                upload_status = status.get("uploadStatus") or "unbekannt"
                processing_status = processing.get("processingStatus") or "unbekannt"
                result = f"YT: {upload_status}/{processing_status}"
            except Exception:
                result = f"YT-ID: {video_id}"
        parts = [
            f"Quelldatei: {file_name}",
            f"Titel: {title}",
            f"Playlist: {playlist}",
        ]
        if upload_seconds > 0:
            parts.append(f"Upload-Dauer: {upload_seconds:.1f}s")
        parts.append(f"Ergebnis: {result}")
        if video_id:
            parts.append(f"Link: https://www.youtube.com/watch?v={video_id}")
        return " | ".join(parts)
