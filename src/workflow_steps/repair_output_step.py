from __future__ import annotations

from pathlib import Path
from typing import Any

from ..media.converter import run_repair_output
from ..media.ffmpeg_runner import validate_media_output
from ..media.step_reporting import format_media_artifact
from .models import PreparedOutput


class RepairOutputStep:
    name = "repair"

    def execute(self, executor: Any, prepared: PreparedOutput) -> int:
        repair_enabled = prepared.repair_enabled_override
        if repair_enabled is None:
            repair_enabled = False
        if not repair_enabled:
            return 0

        current_output = prepared.cv_job.output_path
        if current_output is None or not current_output.exists():
            executor._set_step_status(prepared.job, "repair", "error")
            executor._set_job_status(prepared.orig_idx, "Reparatur ohne gueltiges Eingangsartefakt")
            return 1

        repaired_path = self._repaired_path(current_output)
        if repaired_path.exists() and not prepared.per_settings.video.overwrite:
            if validate_media_output(repaired_path, require_video=True, decode_probe=True, log_callback=executor.log_message.emit):
                prepared.cv_job.output_path = repaired_path
                executor._set_step_status(prepared.job, "repair", "reused-target")
                executor._set_step_detail(prepared.job, "repair", format_media_artifact(repaired_path))
                executor._set_job_status(prepared.orig_idx, f"Reparatur OK (vorhanden): {repaired_path.name}")
                executor.job_progress.emit(prepared.orig_idx, 100)
                return 0
            executor.log_message.emit(
                f"⚠ Vorhandene Reparaturdatei ist defekt – erstelle neu {repaired_path.name}"
            )
            try:
                repaired_path.unlink()
            except OSError:
                executor._set_step_status(prepared.job, "repair", "error")
                executor._set_job_status(prepared.orig_idx, "Defekte Reparaturdatei ist nicht loeschbar")
                return 1

        executor._set_step_status(prepared.job, "repair", "running")
        executor._set_job_status(prepared.orig_idx, "Repariere Ausgabe …")
        executor.job_progress.emit(prepared.orig_idx, 0)
        ok = run_repair_output(
            prepared.cv_job,
            prepared.per_settings,
            cancel_flag=executor._cancel,
            log_callback=executor.log_message.emit,
            progress_callback=lambda pct: executor.job_progress.emit(prepared.orig_idx, pct),
        )
        if not ok:
            executor._set_step_status(prepared.job, "repair", "error")
            executor._set_step_detail(prepared.job, "repair", f"Reparatur fehlgeschlagen für {current_output.name}")
            executor._set_job_status(prepared.orig_idx, "Reparatur fehlgeschlagen")
            return 1
        executor._set_step_status(prepared.job, "repair", "done")
        executor._set_step_detail(prepared.job, "repair", format_media_artifact(prepared.cv_job.output_path))
        executor.job_progress.emit(prepared.orig_idx, 100)
        return 0

    @staticmethod
    def _repaired_path(current_output: Path) -> Path:
        return current_output.with_stem(current_output.stem + "_repaired").with_suffix(".mp4")