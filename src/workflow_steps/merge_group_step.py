from __future__ import annotations

from typing import Any

from ..converter import ConvertJob
from ..workflow import WorkflowJob
from .delete_sources_step import DeleteSourcesStep
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
        merged_path = self._expected_merged_path(first_cv)

        if merged_path.exists() and not per_settings.video.overwrite:
            executor.log_message.emit(
                f"↩ Merge-Gruppe {gid}: vorhandenes Ergebnis gefunden – nutze {merged_path.name}"
            )
            executor._set_step_status(first_job, "merge", "reused-target")
            executor._set_job_status(first_orig_idx, "Zusammenführen OK")
            executor.job_progress.emit(first_orig_idx, 100)
            first_cv.output_path = merged_path
            return PreparedOutput(first_orig_idx, first_job, first_cv, per_settings), 0

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
                first_cv.output_path = source_paths[0]
                return PreparedOutput(first_orig_idx, first_job, first_cv, per_settings), 0
            return None, 0

        merged_path = source_paths[0].with_stem(source_paths[0].stem + "_merged")
        executor._set_step_status(first_job, "merge", "running")
        executor._set_job_status(first_orig_idx, "Zusammenführen …")
        executor.job_progress.emit(first_orig_idx, 0)
        concat_ok = executor._concat_func(
            source_paths,
            merged_path,
            cancel_flag=executor._cancel,
            log_callback=executor.log_message.emit,
            progress_callback=lambda pct: executor.job_progress.emit(first_orig_idx, pct),
            overwrite=per_settings.video.overwrite,
        )
        if not concat_ok:
            executor._set_step_status(first_job, "merge", "error")
            executor.job_progress.emit(first_orig_idx, 0)
            executor.log_message.emit(f"❌ Merge fehlgeschlagen für Gruppe {gid}")
            return None, 1

        executor._set_step_status(first_job, "merge", "done")
        self._delete_step.execute(executor, generated_outputs)
        first_cv.output_path = merged_path
        return PreparedOutput(first_orig_idx, first_job, first_cv, per_settings), 0

    @staticmethod
    def _expected_merged_path(cv_job: ConvertJob):
        base = cv_job.output_path or cv_job.source_path
        return base.with_stem(base.stem + "_merged")
