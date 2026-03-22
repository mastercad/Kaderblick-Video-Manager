from __future__ import annotations

from typing import Any

from ..kaderblick import get_recorded_kaderblick_id, post_to_kaderblick as kaderblick_post
from ..youtube import get_video_id_for_output
from .models import PreparedOutput


class KaderblickPostStep:
    name = "kaderblick"

    def execute(
        self,
        executor: Any,
        prepared: PreparedOutput,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> int:
        if not (prepared.job.upload_kaderblick and prepared.job.upload_youtube):
            return 0
        existing_video_id = None
        if prepared.cv_job.output_path:
            existing_video_id = get_video_id_for_output(prepared.cv_job.output_path)
        existing_kaderblick_id = get_recorded_kaderblick_id(existing_video_id or "")
        if existing_kaderblick_id is not None:
            executor._set_step_status(prepared.job, "kaderblick", "reused-target")
            executor._set_job_status(prepared.orig_idx, f"Kaderblick OK (vorhanden): {existing_kaderblick_id}")
            executor.job_progress.emit(prepared.orig_idx, 100)
            return 0
        executor._set_step_status(prepared.job, "kaderblick", "running")
        executor._set_job_status(prepared.orig_idx, "Kaderblick senden …")
        executor.job_progress.emit(prepared.orig_idx, 0)
        sort_idx = self._resolve_sort_index(executor, prepared, kb_sort_index)
        ok = self._post_to_kaderblick(
            executor,
            prepared.cv_job,
            prepared.job,
            sort_idx,
            prepared.per_settings,
        )
        if not ok:
            executor._set_step_status(prepared.job, "kaderblick", "error")
            executor._set_job_status(prepared.orig_idx, "Kaderblick fehlgeschlagen")
            return 1
        executor._set_step_status(prepared.job, "kaderblick", "done")
        executor.job_progress.emit(prepared.orig_idx, 100)
        return 0

    @staticmethod
    def _resolve_sort_index(
        executor: Any,
        prepared: PreparedOutput,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> int:
        entry = executor._find_file_entry(prepared.job, str(prepared.cv_job.source_path))
        game_id = (
            (entry.kaderblick_game_id if entry and entry.kaderblick_game_id else "")
            or prepared.job.default_kaderblick_game_id
        )
        return kb_sort_index.get((game_id, prepared.cv_job.source_path.name), 1)

    @staticmethod
    def _post_to_kaderblick(executor: Any, cv_job, job, sort_index: int, per_settings) -> bool:
        output = cv_job.output_path
        if not output:
            return True

        video_id = get_video_id_for_output(output)
        if not video_id:
            executor.log_message.emit(
                "  ⚠ Kaderblick: YouTube-Video-ID nicht in Registry – übersprungen"
            )
            return True

        youtube_url = f"https://www.youtube.com/watch?v={video_id}"
        entry = executor._find_file_entry(job, str(cv_job.source_path))
        game_id = (
            (entry.kaderblick_game_id if entry and entry.kaderblick_game_id else None)
            or job.default_kaderblick_game_id
        )
        game_start = entry.kaderblick_game_start if entry else 0
        video_type_id = (
            (entry.kaderblick_video_type_id if entry and entry.kaderblick_video_type_id else 0)
            or job.default_kaderblick_video_type_id
        )
        camera_id = (
            (entry.kaderblick_camera_id if entry and entry.kaderblick_camera_id else 0)
            or job.default_kaderblick_camera_id
        )

        return kaderblick_post(
            settings=per_settings,
            game_id=game_id,
            video_name=cv_job.youtube_title or cv_job.source_path.stem,
            youtube_video_id=video_id,
            youtube_url=youtube_url,
            file_path=cv_job.source_path,
            output_file_path=output,
            game_start_seconds=game_start,
            video_type_id=video_type_id,
            camera_id=camera_id,
            sort_index=sort_index,
            log_callback=executor.log_message.emit,
        )
