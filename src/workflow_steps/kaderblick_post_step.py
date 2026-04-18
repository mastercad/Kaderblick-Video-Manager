from __future__ import annotations

from pathlib import Path
from typing import Any

from ..integrations.kaderblick import get_recorded_kaderblick_id, post_to_kaderblick as kaderblick_post
from ..integrations.youtube import _youtube_variant_candidates, get_video_id_for_output
from .executor_support import ExecutorSupport
from .models import PreparedOutput


class KaderblickPostStep:
    name = "kaderblick"

    @staticmethod
    def _resolve_target_ids(job, entry) -> tuple[int, int]:
        video_type_id = (
            (
                job.merge_output_kaderblick_video_type_id
                if entry and entry.merge_group_id and job.merge_output_kaderblick_video_type_id
                else (entry.kaderblick_video_type_id if entry and entry.kaderblick_video_type_id else 0)
            )
            or job.default_kaderblick_video_type_id
        )
        camera_id = (
            (
                job.merge_output_kaderblick_camera_id
                if entry and entry.merge_group_id and job.merge_output_kaderblick_camera_id
                else (entry.kaderblick_camera_id if entry and entry.kaderblick_camera_id else 0)
            )
            or job.default_kaderblick_camera_id
        )
        return int(video_type_id or 0), int(camera_id or 0)

    def execute(
        self,
        executor: Any,
        prepared: PreparedOutput,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> int:
        kaderblick_enabled = prepared.kaderblick_enabled_override
        if kaderblick_enabled is None:
            kaderblick_enabled = prepared.job.upload_kaderblick and prepared.job.upload_youtube
        if not kaderblick_enabled:
            return 0
        existing_video_id = None
        upload_file = self._upload_artifact(prepared)
        if upload_file is not None:
            existing_video_id = get_video_id_for_output(upload_file)
        existing_kaderblick_id = get_recorded_kaderblick_id(existing_video_id or "")
        if existing_kaderblick_id is not None and ExecutorSupport.allow_reuse_existing(executor):
            executor._set_step_status(prepared.job, "kaderblick", "reused-target")
            executor._set_step_detail(
                prepared.job,
                "kaderblick",
                self._build_summary(executor, prepared, existing_video_id, existing_kaderblick_id, kb_sort_index),
            )
            executor._set_job_status(prepared.orig_idx, f"Kaderblick OK (vorhanden): {existing_kaderblick_id}")
            executor.job_progress.emit(prepared.orig_idx, 100)
            return 0
        executor._set_step_status(prepared.job, "kaderblick", "running")
        executor._set_job_status(prepared.orig_idx, "Kaderblick senden …")
        executor.job_progress.emit(prepared.orig_idx, 0)
        if ExecutorSupport.is_job_cancelled(executor, prepared.orig_idx):
            executor._set_step_status(prepared.job, "kaderblick", "cancelled")
            executor._set_step_detail(prepared.job, "kaderblick", self._build_cancelled_summary(executor, prepared, kb_sort_index))
            executor._set_job_status(prepared.orig_idx, "Kaderblick abgebrochen")
            return 0
        sort_idx = self._resolve_sort_index(executor, prepared, kb_sort_index)
        ok = self._post_to_kaderblick(
            executor,
            prepared.cv_job,
            prepared.job,
            sort_idx,
            prepared.per_settings,
        )
        if ExecutorSupport.is_job_cancelled(executor, prepared.orig_idx):
            executor._set_step_status(prepared.job, "kaderblick", "cancelled")
            executor._set_step_detail(prepared.job, "kaderblick", self._build_cancelled_summary(executor, prepared, kb_sort_index))
            executor._set_job_status(prepared.orig_idx, "Kaderblick abgebrochen")
            return 0
        if not ok:
            executor._set_step_status(prepared.job, "kaderblick", "error")
            executor._set_step_detail(prepared.job, "kaderblick", self._build_error_summary(executor, prepared, kb_sort_index))
            executor._set_job_status(prepared.orig_idx, "Kaderblick fehlgeschlagen")
            return 1
        executor._set_step_status(prepared.job, "kaderblick", "done")
        current_video_id = get_video_id_for_output(upload_file) if upload_file is not None else None
        current_kaderblick_id = get_recorded_kaderblick_id(current_video_id or "")
        executor._set_step_detail(
            prepared.job,
            "kaderblick",
            self._build_summary(executor, prepared, current_video_id, current_kaderblick_id, kb_sort_index),
        )
        executor.job_progress.emit(prepared.orig_idx, 100)
        return 0

    @staticmethod
    def _resolve_sort_index(
        executor: Any,
        prepared: PreparedOutput,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> int:
        entry = executor._find_file_entry(prepared.job, str(prepared.cv_job.source_path))
        explicit_game_id = entry.kaderblick_game_id if entry and entry.kaderblick_game_id else ""
        game_id = ExecutorSupport.resolve_kaderblick_game_id(getattr(executor, "_settings", None), prepared.job, explicit_game_id)
        return kb_sort_index.get((game_id, prepared.cv_job.source_path.name), 1)

    @staticmethod
    def _post_to_kaderblick(executor: Any, cv_job, job, sort_index: int, per_settings) -> bool:
        output = cv_job.output_path
        if not output:
            return True

        upload_file = KaderblickPostStep._upload_artifact_from_cv_job(cv_job)
        video_id = get_video_id_for_output(upload_file)
        if not video_id:
            executor.log_message.emit(
                "  ⚠ Kaderblick: YouTube-Video-ID nicht in Registry – übersprungen"
            )
            return True

        youtube_url = f"https://www.youtube.com/watch?v={video_id}"
        entry = executor._find_file_entry(job, str(cv_job.source_path))
        explicit_game_id = entry.kaderblick_game_id if entry and entry.kaderblick_game_id else ""
        game_id = ExecutorSupport.resolve_kaderblick_game_id(getattr(executor, "_settings", None), job, explicit_game_id)
        game_start = entry.kaderblick_game_start if entry else 0
        video_type_id, camera_id = KaderblickPostStep._resolve_target_ids(job, entry)

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

    @staticmethod
    def _build_error_summary(executor: Any, prepared: PreparedOutput, kb_sort_index: dict[tuple[str, str], int]) -> str:
        entry = executor._find_file_entry(prepared.job, str(prepared.cv_job.source_path))
        explicit_game_id = entry.kaderblick_game_id if entry and entry.kaderblick_game_id else ""
        game_id = ExecutorSupport.resolve_kaderblick_game_id(getattr(executor, "_settings", None), prepared.job, explicit_game_id)
        return f"Video: {prepared.cv_job.youtube_title or prepared.cv_job.source_path.stem} | Spiel: {game_id or '-'} | Ergebnis: Kaderblick fehlgeschlagen"

    @staticmethod
    def _build_cancelled_summary(executor: Any, prepared: PreparedOutput, kb_sort_index: dict[tuple[str, str], int]) -> str:
        entry = executor._find_file_entry(prepared.job, str(prepared.cv_job.source_path))
        explicit_game_id = entry.kaderblick_game_id if entry and entry.kaderblick_game_id else ""
        game_id = ExecutorSupport.resolve_kaderblick_game_id(getattr(executor, "_settings", None), prepared.job, explicit_game_id)
        return f"Video: {prepared.cv_job.youtube_title or prepared.cv_job.source_path.stem} | Spiel: {game_id or '-'} | Ergebnis: Durch Benutzer abgebrochen"

    @staticmethod
    def _build_summary(executor: Any, prepared: PreparedOutput, youtube_video_id: str | None, kaderblick_id: int | None, kb_sort_index: dict[tuple[str, str], int]) -> str:
        entry = executor._find_file_entry(prepared.job, str(prepared.cv_job.source_path))
        explicit_game_id = entry.kaderblick_game_id if entry and entry.kaderblick_game_id else ""
        game_id = ExecutorSupport.resolve_kaderblick_game_id(getattr(executor, "_settings", None), prepared.job, explicit_game_id)
        video_type_id, camera_id = KaderblickPostStep._resolve_target_ids(prepared.job, entry)
        sort_index = kb_sort_index.get((game_id, Path(prepared.cv_job.source_path).name), 1)
        parts = [
            f"Video: {prepared.cv_job.youtube_title or prepared.cv_job.source_path.stem}",
            f"YouTube-ID: {youtube_video_id or '-'}",
            f"Spiel: {game_id or '-'}",
            f"Video-Typ: {video_type_id or '-'}",
            f"Kamera: {camera_id or '-'}",
            f"Sortierung: {sort_index}",
        ]
        if kaderblick_id is not None:
            parts.append(f"Kaderblick-ID: {kaderblick_id}")
        return " | ".join(parts)

    @staticmethod
    def _upload_artifact(prepared: PreparedOutput) -> Path | None:
        return KaderblickPostStep._upload_artifact_from_cv_job(prepared.cv_job)

    @staticmethod
    def _upload_artifact_from_cv_job(cv_job) -> Path | None:
        output = cv_job.output_path
        if output is None:
            return None
        derived_dir = str(getattr(cv_job, "derived_output_dir", "") or "")
        for youtube_variant in _youtube_variant_candidates(output, derived_dir):
            if youtube_variant.exists():
                return youtube_variant
        return output
