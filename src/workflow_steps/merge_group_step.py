from __future__ import annotations

from typing import Any

from ..media.converter import ConvertJob
from ..media.ffmpeg_runner import validate_media_output
from ..media.merge_analysis import analyze_merge_sources
from ..media.step_reporting import format_encoder_summary, format_list_summary, format_media_artifact
from ..workflow import WorkflowJob
from ..integrations.youtube_title_editor import MatchData, SegmentData, build_output_filename_from_title, build_playlist_title, build_video_description, build_video_tags, build_video_title
from .delete_sources_step import DeleteSourcesStep
from .executor_support import ExecutorSupport
from .models import ConvertItem, PreparedOutput


class MergeGroupStep:
    name = "merge"

    def __init__(self):
        self._delete_step = DeleteSourcesStep()

    def execute(
        self,
        executor: Any,
        gid: str,
        group: list[ConvertItem],
    ) -> tuple[PreparedOutput | None, int]:
        first_orig_idx = group[0].orig_idx
        first_job = group[0].job
        first_cv = group[0].cv_job
        per_settings = executor._build_job_settings(first_job)
        self._apply_merge_output_metadata(first_job, first_cv)
        merged_path = self._expected_merged_path(first_job, first_cv)
        group_source_names = [item.cv_job.source_path.name for item in group]

        if merged_path.exists() and not per_settings.video.overwrite:
            if not validate_media_output(merged_path, require_video=True, decode_probe=True, log_callback=executor.log_message.emit):
                executor.log_message.emit(
                    f"⚠ Merge-Gruppe {gid}: vorhandenes Ergebnis ist defekt – erstelle neu {merged_path.name}"
                )
                try:
                    merged_path.unlink()
                except OSError:
                    executor.log_message.emit(
                        f"❌ Defektes Merge-Ergebnis kann nicht geloescht werden: {merged_path.name}"
                    )
                    return None, 1
            else:
                executor.log_message.emit(
                    f"↩ Merge-Gruppe {gid}: vorhandenes Ergebnis gefunden – nutze {merged_path.name}"
                )
                executor._set_step_status(first_job, "merge", "reused-target")
                executor._set_step_detail(
                    first_job,
                    "merge",
                    f"{format_list_summary('Quellen', group_source_names)} | {format_media_artifact(merged_path)} | {format_encoder_summary(per_settings.video.encoder)}",
                )
                executor._set_job_status(first_orig_idx, "Zusammenführen OK")
                executor.job_progress.emit(first_orig_idx, 100)
                first_cv.output_path = merged_path
                return PreparedOutput(first_orig_idx, first_job, first_cv, per_settings, graph_origin_kind="merge"), 0

        source_paths = [item.cv_job.output_path for item in group if item.cv_job.output_path and item.cv_job.output_path.exists()]
        generated_outputs = [
            item.cv_job.output_path
            for item in group
            if item.cv_job.output_path
            and item.cv_job.output_path.exists()
            and item.cv_job.output_path != item.cv_job.source_path
        ]
        if not source_paths:
            return None, 0

        if len(source_paths) < 2:
            if len(source_paths) == 1:
                executor.log_message.emit(
                    f"ℹ Merge-Gruppe {gid}: nur eine fertige Datei – kein Concat, nur ein Upload."
                )
                executor._set_step_status(first_job, "merge", "reused-target")
                executor._set_step_detail(
                    first_job,
                    "merge",
                    f"{format_list_summary('Quellen', [path.name for path in source_paths])} | {format_media_artifact(source_paths[0])} | Einzeldatei, kein Concat",
                )
                executor._set_job_status(first_orig_idx, "Zusammenführen OK")
                executor.job_progress.emit(first_orig_idx, 100)
                first_cv.output_path = source_paths[0]
                return PreparedOutput(first_orig_idx, first_job, first_cv, per_settings, graph_origin_kind="merge"), 0
            return None, 0

        if executor._merge_precedes_convert(first_job):
            report = analyze_merge_sources(source_paths)
            if not report.mergeable:
                executor._set_step_status(first_job, "merge", "error")
                executor._set_step_detail(
                    first_job,
                    "merge",
                    f"{format_list_summary('Quellen', [path.name for path in source_paths])} | Inkompatible Merge-Eingänge",
                )
                executor._set_job_status(first_orig_idx, "Fehler: Merge-Eingänge inkompatibel")
                executor.job_progress.emit(first_orig_idx, 0)
                executor.log_message.emit(f"❌ Merge-Gruppe {gid} ist nicht direkt mergebar")
                for reason in report.reasons:
                    executor.log_message.emit(f"   • {reason}")
                return None, 1

        executor._set_step_status(first_job, "merge", "running")
        executor._set_job_status(first_orig_idx, "Zusammenführen …")
        executor.job_progress.emit(first_orig_idx, 0)
        merged_path.parent.mkdir(parents=True, exist_ok=True)
        concat_ok = executor._concat_func(
            source_paths,
            merged_path,
            cancel_flag=executor._cancel,
            log_callback=executor.log_message.emit,
            progress_callback=lambda pct: executor.job_progress.emit(first_orig_idx, pct),
            overwrite=per_settings.video.overwrite,
            no_bframes=first_job.merge_no_bframes,
            keyframe_interval=per_settings.video.keyframe_interval,
            encoder=per_settings.video.encoder,
            preset=first_job.merge_preset or per_settings.video.preset,
            crf=per_settings.video.crf,
            target_resolution=first_job.merge_output_resolution,
            metadata_job=first_cv,
        )
        if not concat_ok:
            executor._set_step_status(first_job, "merge", "error")
            executor._set_step_detail(
                first_job,
                "merge",
                f"{format_list_summary('Quellen', [path.name for path in source_paths])} | Fehler beim Zusammenführen nach {merged_path.name}",
            )
            executor._set_job_status(first_orig_idx, "Fehler: Merge fehlgeschlagen")
            executor.job_progress.emit(first_orig_idx, 0)
            executor.log_message.emit(f"❌ Merge fehlgeschlagen für Gruppe {gid}")
            return None, 1

        executor._set_step_status(first_job, "merge", "done")
        executor._set_step_detail(
            first_job,
            "merge",
            f"{format_list_summary('Quellen', [path.name for path in source_paths])} | {format_media_artifact(merged_path)} | {format_encoder_summary(per_settings.video.encoder)}",
        )
        executor._set_job_status(first_orig_idx, "Zusammenführen OK")
        self._delete_step.execute(executor, generated_outputs)
        first_cv.output_path = merged_path
        return PreparedOutput(first_orig_idx, first_job, first_cv, per_settings, graph_origin_kind="merge"), 0

    @staticmethod
    def _apply_merge_output_metadata(first_job: WorkflowJob, first_cv: ConvertJob) -> None:
        match = MatchData(**first_job.merge_match_data) if first_job.merge_match_data else None
        segment = SegmentData(**first_job.merge_segment_data) if first_job.merge_segment_data else None
        if match is not None and segment is not None:
            first_cv.youtube_title = build_video_title(match, segment)
            first_cv.youtube_playlist = build_playlist_title(match)
            first_cv.youtube_description = build_video_description(match, segment)
            first_cv.youtube_tags = build_video_tags(match, segment)
            return

        if first_job.merge_output_title:
            first_cv.youtube_title = first_job.merge_output_title
        if first_job.merge_output_playlist:
            first_cv.youtube_playlist = first_job.merge_output_playlist
        if first_job.merge_output_description:
            first_cv.youtube_description = first_job.merge_output_description

    @staticmethod
    def _expected_merged_path(first_job: WorkflowJob, cv_job: ConvertJob):
        base = cv_job.output_path or cv_job.source_path
        target_extension = ExecutorSupport.resolve_container_extension(first_job.merge_output_format, base)
        if first_job.merge_output_title or cv_job.youtube_title:
            stem = build_output_filename_from_title(
                first_job.merge_output_title or cv_job.youtube_title,
                fallback=base.stem,
            )
            if stem != base.stem:
                return ExecutorSupport.derived_output_path(cv_job, base, stem=stem, extension=target_extension)
        return ExecutorSupport.derived_output_path(cv_job, base, suffix="_merged", extension=target_extension)
