from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ..workflow import WorkflowJob
from .executor_support import ExecutorSupport
from .transfer_io import emit_item_progress, transfer_files


class DirectFilesTransferStep:
    name = "files-transfer"

    def execute(
        self,
        executor: Any,
        orig_idx: int,
        job: WorkflowJob,
        on_file_ready: Callable[[str], None] | None = None,
    ) -> list[str]:
        paths: list[Path] = []
        dst_dir = ExecutorSupport.resolve_copy_destination(executor._settings, job)
        total_entries = len(job.files)
        executor.source_progress.emit(orig_idx, 0)
        for entry_idx, entry in enumerate(job.files, start=1):
            source_path = Path(entry.source_path)
            executor._set_job_status(orig_idx, f"Transfer {entry_idx}/{total_entries}: {source_path.name} …")
            if source_path.exists():
                paths.append(source_path)
                emit_item_progress(executor, orig_idx, entry_idx, total_entries)
                continue

            fallback = None
            if dst_dir is not None and ExecutorSupport.allow_reuse_existing(executor):
                candidate = dst_dir / source_path.name
                if candidate.exists():
                    fallback = candidate
            if fallback:
                executor.log_message.emit(
                    f"  ↩ Quelle fehlt, nutze vorhandenes Ziel: {fallback}"
                )
                paths.append(fallback)
            elif self._should_keep_missing_entry_for_resume(job, source_path):
                executor.log_message.emit(
                    f"  ↩ Quelle fehlt: {source_path} | Fortsetzen über vorhandenes Merge-Ergebnis wird versucht"
                )
                paths.append(source_path)
            else:
                executor.log_message.emit(f"  ⚠ Datei nicht gefunden: {source_path}")
            emit_item_progress(executor, orig_idx, entry_idx, total_entries)

        executor.log_message.emit(f"\n🗃 {job.name}: {len(paths)} Datei(en) bereit")
        executor._set_step_detail(
            job,
            "transfer",
            f"Bereit: {len(paths)} Datei(en) | {', '.join(path.name for path in paths[:4])}{' …' if len(paths) > 4 else ''}",
        )

        if not dst_dir:
            executor.source_progress.emit(orig_idx, 100)
            ready = [str(path) for path in paths]
            if on_file_ready is not None:
                for path in ready:
                    on_file_ready(path)
            return ready

        dst_dir.mkdir(parents=True, exist_ok=True)
        verb = "verschieben" if job.move_files else "kopieren"
        executor.log_message.emit(f"📁 {job.name}: {len(paths)} Datei(en) {verb} …")
        return transfer_files(executor, orig_idx, job, paths, dst_dir, on_file_ready=on_file_ready)

    @staticmethod
    def _should_keep_missing_entry_for_resume(job: WorkflowJob, source_path: Path) -> bool:
        if not (job.resume_status or job.step_statuses):
            return False
        for entry in job.files:
            if entry.source_path != str(source_path):
                continue
            if entry.merge_group_id:
                return True
            if job.convert_enabled:
                return True
            if job.title_card_enabled or job.create_youtube_version or job.upload_youtube:
                return True
        return False
