from __future__ import annotations

from pathlib import Path
from typing import Any

from ..media.ffmpeg_runner import validate_media_output
from ..media.step_reporting import format_encoder_summary, format_source_target_summary
from ..workflow import graph_path_exists_between_types
from .executor_support import ExecutorSupport
from .models import PreparedOutput


class YoutubeVersionStep:
    name = "yt_version"

    def execute(self, executor: Any, prepared: PreparedOutput) -> int:
        youtube_version_enabled = prepared.youtube_version_enabled_override
        if youtube_version_enabled is None:
            youtube_version_enabled = prepared.job.create_youtube_version
        if not (youtube_version_enabled and prepared.cv_job.output_path):
            return 0
        existing = self._existing_youtube_version(prepared)
        if existing is not None and existing.exists() and not prepared.per_settings.video.overwrite:
            if validate_media_output(existing, require_video=True, decode_probe=True, log_callback=executor.log_message.emit):
                executor._set_step_status(prepared.job, "yt_version", "reused-target")
                executor._set_step_detail(
                    prepared.job,
                    "yt_version",
                    f"{format_source_target_summary(prepared.cv_job.output_path, existing)} | {format_encoder_summary(prepared.per_settings.video.encoder)}",
                )
                executor._set_job_status(prepared.orig_idx, f"YT-Version OK (vorhanden): {existing.name}")
                executor.job_progress.emit(prepared.orig_idx, 100)
                return 0
            executor.log_message.emit(
                f"⚠ Vorhandene YT-Version ist defekt – erstelle neu {existing.name}"
            )
            try:
                existing.unlink()
            except OSError:
                executor._set_step_status(prepared.job, "yt_version", "error")
                executor._set_job_status(prepared.orig_idx, "YT-Version defekt und nicht loeschbar")
                return 1
        if not prepared.cv_job.output_path.exists():
            return 0
        executor._set_step_status(prepared.job, "yt_version", "running")
        executor._set_job_status(prepared.orig_idx, "YT-Version erstellen …")
        executor.job_progress.emit(prepared.orig_idx, 0)
        has_graph = bool(getattr(prepared.job, "graph_nodes", None))
        merge_before_yt = graph_path_exists_between_types(prepared.job, {"merge"}, "yt_version") if has_graph else False
        convert_before_yt = graph_path_exists_between_types(prepared.job, {"convert"}, "yt_version") if has_graph else bool(prepared.job.convert_enabled)
        inherited_encoder = prepared.per_settings.video.encoder
        inherited_crf: int | None = None
        if merge_before_yt:
            inherited_encoder = (
                prepared.job.merge_encoder
                if prepared.job.merge_encoder not in {"", "inherit"}
                else prepared.per_settings.video.encoder
            )
            inherited_crf = prepared.job.merge_crf if prepared.job.merge_crf > 0 else prepared.per_settings.video.crf
        elif convert_before_yt:
            inherited_encoder = prepared.per_settings.video.encoder
            inherited_crf = prepared.per_settings.video.crf
        cancel_flag = executor._cancel_flag_for_job(prepared.orig_idx)
        ok = executor._youtube_convert_func(
            prepared.cv_job,
            prepared.per_settings,
            cancel_flag=cancel_flag,
            log_callback=executor.log_message.emit,
            progress_callback=lambda pct: executor.job_progress.emit(prepared.orig_idx, pct),
            encoder=(prepared.job.yt_version_encoder if prepared.job.yt_version_encoder not in {"", "inherit"} else inherited_encoder),
            crf=prepared.job.yt_version_crf if prepared.job.yt_version_crf > 0 else inherited_crf,
            preset=prepared.job.yt_version_preset or prepared.per_settings.video.preset,
            fps=prepared.job.yt_version_fps if prepared.job.yt_version_fps > 0 else None,
            no_bframes=prepared.job.yt_version_no_bframes,
            output_format=prepared.job.yt_version_output_format,
            output_resolution=prepared.job.yt_version_output_resolution,
        )
        if executor._is_job_cancelled(prepared.orig_idx):
            executor._set_step_status(prepared.job, "yt_version", "cancelled")
            executor._set_step_detail(prepared.job, "yt_version", f"Durch Benutzer abgebrochen: {prepared.cv_job.output_path.name}")
            executor._set_job_status(prepared.orig_idx, "YT-Version abgebrochen")
            return 0
        if not ok:
            executor._set_step_status(prepared.job, "yt_version", "error")
            executor._set_step_detail(prepared.job, "yt_version", f"YT-Version fehlgeschlagen für {prepared.cv_job.output_path.name}")
            executor._set_job_status(prepared.orig_idx, "YT-Version fehlgeschlagen")
            executor.log_message.emit(
                "⚠ YouTube-Version konnte nicht erstellt werden – Original wird hochgeladen."
            )
            return 0
        executor._set_step_status(prepared.job, "yt_version", "done")
        executor._set_step_detail(
            prepared.job,
            "yt_version",
            f"{format_source_target_summary(prepared.cv_job.output_path, self._existing_youtube_version(prepared))} | {format_encoder_summary(prepared.per_settings.video.encoder)}",
        )
        executor.job_progress.emit(prepared.orig_idx, 100)
        return 0

    @staticmethod
    def _existing_youtube_version(prepared: PreparedOutput) -> Path | None:
        output_path = prepared.cv_job.output_path
        if output_path is None:
            return None
        target_extension = ExecutorSupport.resolve_container_extension(
            prepared.job.yt_version_output_format,
            output_path,
        )
        return ExecutorSupport.derived_output_path(
            prepared.cv_job,
            output_path,
            suffix="_youtube",
            extension=target_extension,
        )
