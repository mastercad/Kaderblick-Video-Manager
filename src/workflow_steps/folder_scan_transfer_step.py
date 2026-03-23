from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ..workflow import WorkflowJob
from .transfer_io import emit_item_progress, transfer_files


class FolderScanTransferStep:
    name = "folder-scan-transfer"

    def execute(
        self,
        executor: Any,
        orig_idx: int,
        job: WorkflowJob,
        on_file_ready: Callable[[str], None] | None = None,
    ) -> list[str]:
        src_dir = Path(job.source_folder)
        dst_dir = Path(job.copy_destination) if job.copy_destination else None
        pattern = job.file_pattern or "*.mp4"
        executor.job_progress.emit(orig_idx, 0)
        executor.source_progress.emit(orig_idx, 0)

        if not src_dir.exists():
            fallback = self._existing_targets(job)
            if fallback:
                executor.log_message.emit(
                    f"  ↩ Quellordner fehlt – nutze {len(fallback)} vorhandene Datei(en) aus dem Zielverzeichnis"
                )
                executor._set_step_status(job, "transfer", "reused-target")
                executor.job_progress.emit(orig_idx, 100)
                executor.source_progress.emit(orig_idx, 100)
                return fallback
            raise FileNotFoundError(f"Quellordner nicht gefunden: {src_dir}")

        files = sorted(src_dir.glob(pattern))
        if not files:
            fallback = self._existing_targets(job)
            if fallback:
                executor.log_message.emit(
                    f"  ↩ Keine Quelldateien gefunden – nutze {len(fallback)} vorhandene Datei(en) aus dem Zielverzeichnis"
                )
                executor._set_step_status(job, "transfer", "reused-target")
                executor.job_progress.emit(orig_idx, 100)
                return fallback
            executor.log_message.emit(f"  ⚠ Keine Dateien mit Muster '{pattern}' in {src_dir}")
            executor.job_progress.emit(orig_idx, 100)
            executor.source_progress.emit(orig_idx, 100)
            return []

        if not dst_dir:
            total_files = len(files)
            for file_idx, path in enumerate(files, start=1):
                executor._set_job_status(orig_idx, f"Transfer {file_idx}/{total_files}: {path.name} …")
                emit_item_progress(executor, orig_idx, file_idx, total_files)
            executor.log_message.emit(f"\n📁 {job.name}: {len(files)} Datei(en) gefunden")
            executor.job_progress.emit(orig_idx, 100)
            executor.source_progress.emit(orig_idx, 100)
            ready = [str(path) for path in files]
            if on_file_ready is not None:
                for path in ready:
                    on_file_ready(path)
            executor._set_step_detail(
                job,
                "transfer",
                f"Gefunden: {len(files)} Datei(en) in {src_dir.name} | Muster: {pattern}",
            )
            return ready

        dst_dir.mkdir(parents=True, exist_ok=True)
        verb = "verschieben" if job.move_files else "kopieren"
        executor.log_message.emit(f"\n📁 {job.name}: {len(files)} Datei(en) {verb} …")
        executor._set_step_detail(
            job,
            "transfer",
            f"Quelle: {src_dir.name} | {len(files)} Datei(en) werden {verb}",
        )
        return transfer_files(executor, orig_idx, job, files, dst_dir, on_file_ready=on_file_ready)

    @staticmethod
    def _existing_targets(job: WorkflowJob) -> list[str]:
        if not job.copy_destination:
            return []
        dst_dir = Path(job.copy_destination)
        if not dst_dir.exists():
            return []
        pattern = job.file_pattern or "*.mp4"
        return [str(path) for path in sorted(dst_dir.glob(pattern)) if path.is_file()]
