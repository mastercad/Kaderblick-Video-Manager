from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import PreparedOutput


class StopOutputStep:
    name = "stop"

    def execute(self, executor: Any, prepared: PreparedOutput) -> int:
        current_output = prepared.cv_job.output_path or prepared.cv_job.source_path
        output_name = Path(current_output).name if current_output else "aktuelles Artefakt"
        branch = self._latest_validation_result(prepared)

        if branch == "irreparable":
            message = f"Workflow-Zweig beendet: Datei irreparabel ({output_name})"
        elif branch == "repairable":
            message = f"Workflow-Zweig beendet nach reparierbarem Befund ({output_name})"
        else:
            message = f"Workflow-Zweig beendet: {output_name}"

        prepared.mark_finished = False
        prepared.terminal_status_text = message
        executor.log_message.emit(message)
        executor._set_step_status(prepared.job, self.name, "done")
        executor._set_step_detail(prepared.job, self.name, message)
        executor._set_job_status(prepared.orig_idx, message)
        executor.job_progress.emit(prepared.orig_idx, 100, self.name)
        return 0

    @staticmethod
    def _latest_validation_result(prepared: PreparedOutput) -> str:
        results = getattr(prepared, "validation_results", {}) or {}
        if not results:
            return ""
        return next(reversed(results.values()))