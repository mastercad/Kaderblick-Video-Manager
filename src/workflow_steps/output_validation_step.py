from __future__ import annotations

from pathlib import Path
from typing import Any

from ..media.ffmpeg_runner import inspect_media_compatibility


class OutputValidationStep:
    def __init__(self, node_type: str, *, deep_scan: bool) -> None:
        self.name = node_type
        self._deep_scan = deep_scan
        self._status_running = "Deep-Scan läuft …" if deep_scan else "Kompatibilität prüfen …"

    def execute(self, executor: Any, prepared: Any) -> int:
        current_output = prepared.cv_job.output_path or prepared.cv_job.source_path
        if current_output is None or not Path(current_output).exists():
            executor._set_step_status(prepared.job, self.name, "irreparable")
            executor._set_job_status(prepared.orig_idx, "Validierung ohne gültiges Eingangsartefakt")
            return 1

        executor._set_step_status(prepared.job, self.name, "running")
        executor._set_job_status(prepared.orig_idx, self._status_running)
        executor.job_progress.emit(prepared.orig_idx, 0, self.name)
        result = inspect_media_compatibility(
            Path(current_output),
            require_video=True,
            deep_scan=self._deep_scan,
            log_callback=executor.log_message.emit,
        )
        node_id = executor._graph_node_id_for_type(prepared.job, self.name)
        if node_id:
            prepared.validation_results[node_id] = result.status
        executor._set_step_status(prepared.job, self.name, result.status)
        detail = result.summary
        if result.details:
            detail += " | " + "; ".join(result.details[:4])
        executor._set_step_detail(prepared.job, self.name, detail)
        executor._set_job_status(prepared.orig_idx, result.summary)
        executor.job_progress.emit(prepared.orig_idx, 100, self.name)

        if result.status == "ok":
            return 0
        if executor._validation_branch_has_targets(prepared, self.name, result.status):
            return 0
        return 1