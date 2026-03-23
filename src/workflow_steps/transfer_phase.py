from __future__ import annotations

from pathlib import Path
from typing import Any

from ..media.converter import ConvertJob
from ..workflow import WorkflowJob
from .models import ConvertItem, TransferPhaseResult


class TransferPhase:
    name = "transfer-phase"

    def execute(self, executor: Any, active: list[tuple[int, WorkflowJob]]) -> TransferPhaseResult:
        convert_items: list[ConvertItem] = []
        transfer_fail = 0

        executor.phase_changed.emit("Phase 1 – Downloads …")
        workflow_label = "1 aktiver Workflow" if len(active) == 1 else f"{len(active)} aktive Workflows"
        executor.log_message.emit(
            f"\n{'═' * 60}"
            f"\n  📥 Phase 1: Transfer  ({workflow_label})"
            f"\n{'═' * 60}"
        )

        for active_pos, (orig_idx, job) in enumerate(active):
            if executor._cancel.is_set():
                break
            executor._set_step_status(job, "transfer", "running")
            executor._set_job_status(orig_idx, "Transfer …")
            try:
                files = executor._transfer_step.execute(executor, orig_idx, job)
            except Exception as exc:
                executor._set_step_status(job, "transfer", f"error: {exc}")
                executor._set_job_status(orig_idx, f"Fehler: {exc}")
                job.error_msg = str(exc)
                executor.log_message.emit(f"❌ {job.name}: {exc}")
                transfer_fail += 1
                continue

            if executor._cancel.is_set():
                break

            executor._set_job_status(orig_idx, "Transfer OK")
            executor._set_step_status(job, "transfer", "done")
            executor.overall_progress.emit(active_pos + 1, len(active))
            convert_items.extend(self._build_convert_items(executor, orig_idx, job, files))

        return TransferPhaseResult(
            convert_items=convert_items,
            transfer_fail=transfer_fail,
            cancelled=executor._cancel.is_set(),
        )

    @staticmethod
    def _build_convert_items(executor: Any, orig_idx: int, job: Any, files: list[str]) -> list[ConvertItem]:
        items: list[ConvertItem] = []
        for file_path in files:
            if job.convert_enabled:
                convert_job = executor._build_convert_job(job, file_path)
            else:
                convert_job = ConvertJob(
                    source_path=Path(file_path),
                    job_type="convert",
                    youtube_title=executor._resolve_youtube_title(job, file_path),
                    youtube_playlist=job.default_youtube_playlist,
                )
                convert_job.status = "Fertig"
                convert_job.output_path = Path(file_path)
            items.append(ConvertItem(orig_idx=orig_idx, job=job, cv_job=convert_job))
        return items
