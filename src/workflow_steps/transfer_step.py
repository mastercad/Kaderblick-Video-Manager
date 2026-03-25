from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Callable

from .direct_files_transfer_step import DirectFilesTransferStep
from .executor_support import ExecutorSupport
from .folder_scan_transfer_step import FolderScanTransferStep
from .pi_download_transfer_step import PiDownloadTransferStep
from ..workflow import WorkflowJob, graph_has_multiple_sources, graph_source_nodes


class TransferStep:
    name = "transfer"

    def __init__(self):
        self._files_step = DirectFilesTransferStep()
        self._folder_scan_step = FolderScanTransferStep()
        self._pi_download_step = PiDownloadTransferStep()

    def execute(
        self,
        executor: Any,
        orig_idx: int,
        job: WorkflowJob,
        on_file_ready: Callable[[str], None] | None = None,
    ) -> list[str]:
        if graph_has_multiple_sources(job):
            return self._execute_graph_sources(executor, orig_idx, job, on_file_ready)
        if job.source_mode == "files":
            return self._files_step.execute(executor, orig_idx, job, on_file_ready=on_file_ready)
        if job.source_mode == "pi_download":
            return self._pi_download_step.execute(executor, orig_idx, job, on_file_ready=on_file_ready)
        if job.source_mode == "folder_scan":
            return self._folder_scan_step.execute(executor, orig_idx, job, on_file_ready=on_file_ready)
        raise ValueError(f"Unbekannter Quellmodus: {job.source_mode!r}")

    def resume_inputs(
        self,
        executor: Any,
        orig_idx: int,
        job: WorkflowJob,
        on_file_ready: Callable[[str], None] | None = None,
    ) -> list[str]:
        if graph_has_multiple_sources(job):
            return self._resume_graph_sources(executor, orig_idx, job, on_file_ready)
        if job.source_mode == "files":
            ready = self._resume_direct_files(executor, job)
        elif job.source_mode == "pi_download":
            ready = self._resume_pi_download(executor, job)
        elif job.source_mode == "folder_scan":
            ready = self._resume_folder_scan(executor, job)
        else:
            raise ValueError(f"Unbekannter Quellmodus: {job.source_mode!r}")
        if on_file_ready is not None:
            for path in ready:
                on_file_ready(path)
        if not ready:
            executor.source_progress.emit(orig_idx, 100)
        executor.source_progress.emit(orig_idx, 100)
        return ready

    def _execute_graph_sources(
        self,
        executor: Any,
        orig_idx: int,
        job: WorkflowJob,
        on_file_ready: Callable[[str], None] | None = None,
    ) -> list[str]:
        ready_paths: list[str] = []

        def _wrap(source_node_id: str, downstream: Callable[[str], None] | None):
            def _inner(file_path: str) -> None:
                ExecutorSupport.register_runtime_file_entry(job, source_node_id, file_path)
                ready_paths.append(file_path)
                if downstream is not None:
                    downstream(file_path)
            return _inner

        for source_node_id, source_node_type in graph_source_nodes(job):
            branch_job = copy.deepcopy(job)
            if source_node_type == "source_files":
                branch_job.source_mode = "files"
                branch_job.files = ExecutorSupport.files_for_source(job, source_node_id)
                self._files_step.execute(executor, orig_idx, branch_job, on_file_ready=_wrap(source_node_id, on_file_ready))
            elif source_node_type == "source_folder_scan":
                branch_job.source_mode = "folder_scan"
                self._folder_scan_step.execute(executor, orig_idx, branch_job, on_file_ready=_wrap(source_node_id, on_file_ready))
            elif source_node_type == "source_pi_download":
                branch_job.source_mode = "pi_download"
                branch_job.files = ExecutorSupport.files_for_source(job, source_node_id)
                self._pi_download_step.execute(executor, orig_idx, branch_job, on_file_ready=_wrap(source_node_id, on_file_ready))
        return ready_paths

    def _resume_graph_sources(
        self,
        executor: Any,
        orig_idx: int,
        job: WorkflowJob,
        on_file_ready: Callable[[str], None] | None = None,
    ) -> list[str]:
        ready_paths: list[str] = []

        def _wrap(source_node_id: str, downstream: Callable[[str], None] | None):
            def _inner(file_path: str) -> None:
                ExecutorSupport.register_runtime_file_entry(job, source_node_id, file_path)
                ready_paths.append(file_path)
                if downstream is not None:
                    downstream(file_path)
            return _inner

        for source_node_id, source_node_type in graph_source_nodes(job):
            branch_job = copy.deepcopy(job)
            if source_node_type == "source_files":
                branch_job.source_mode = "files"
                branch_job.files = ExecutorSupport.files_for_source(job, source_node_id)
                for path in self._resume_direct_files(executor, branch_job):
                    _wrap(source_node_id, on_file_ready)(path)
            elif source_node_type == "source_folder_scan":
                branch_job.source_mode = "folder_scan"
                for path in self._resume_folder_scan(executor, branch_job):
                    _wrap(source_node_id, on_file_ready)(path)
            elif source_node_type == "source_pi_download":
                branch_job.source_mode = "pi_download"
                branch_job.files = ExecutorSupport.files_for_source(job, source_node_id)
                for path in self._resume_pi_download(executor, branch_job):
                    _wrap(source_node_id, on_file_ready)(path)
        executor.source_progress.emit(orig_idx, 100)
        return ready_paths

    def _resume_direct_files(self, executor: Any, job: WorkflowJob) -> list[str]:
        ready: list[str] = []
        dst_dir = ExecutorSupport.resolve_copy_destination(executor._settings, job)
        for entry in job.files:
            source_path = Path(entry.source_path)
            if dst_dir is not None:
                candidate = dst_dir / source_path.name
                if candidate.exists() or self._files_step._should_keep_missing_entry_for_resume(job, candidate):
                    ready.append(str(candidate))
                    continue
            if source_path.exists() or self._files_step._should_keep_missing_entry_for_resume(job, source_path):
                ready.append(str(source_path))
        return ready

    def _resume_folder_scan(self, executor: Any, job: WorkflowJob) -> list[str]:
        dst_dir = ExecutorSupport.resolve_copy_destination(executor._settings, job)
        pattern = job.file_pattern or "*.mp4"
        if dst_dir is not None and dst_dir.exists():
            existing = [str(path) for path in sorted(dst_dir.glob(pattern)) if path.is_file()]
            if existing:
                return existing
        src_dir = Path(job.source_folder)
        if src_dir.exists():
            return [str(path) for path in sorted(src_dir.glob(pattern)) if path.is_file()]
        if dst_dir is not None:
            candidates: list[str] = []
            seen: set[str] = set()
            for entry in job.files:
                candidate = dst_dir / Path(entry.source_path).name
                key = str(candidate)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(key)
            return candidates
        return [str(entry.source_path) for entry in job.files if str(entry.source_path).strip()]

    def _resume_pi_download(self, executor: Any, job: WorkflowJob) -> list[str]:
        existing = self._pi_download_step._existing_targets(executor, job)
        if existing:
            return existing
        dest_root = ExecutorSupport.resolve_download_destination(executor._settings, job)
        if dest_root is None:
            return []
        candidates: list[str] = []
        seen: set[str] = set()
        for entry in job.files:
            candidate = dest_root / f"{Path(entry.source_path).stem}.mjpg"
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(key)
        return candidates
