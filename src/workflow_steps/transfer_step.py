from __future__ import annotations

import copy
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
