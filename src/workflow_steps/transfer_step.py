from __future__ import annotations

from typing import Any

from .direct_files_transfer_step import DirectFilesTransferStep
from .folder_scan_transfer_step import FolderScanTransferStep
from .pi_download_transfer_step import PiDownloadTransferStep
from ..workflow import WorkflowJob


class TransferStep:
    name = "transfer"

    def __init__(self):
        self._files_step = DirectFilesTransferStep()
        self._folder_scan_step = FolderScanTransferStep()
        self._pi_download_step = PiDownloadTransferStep()

    def execute(self, executor: Any, orig_idx: int, job: WorkflowJob) -> list[str]:
        if job.source_mode == "files":
            return self._files_step.execute(executor, orig_idx, job)
        if job.source_mode == "pi_download":
            return self._pi_download_step.execute(executor, orig_idx, job)
        if job.source_mode == "folder_scan":
            return self._folder_scan_step.execute(executor, orig_idx, job)
        raise ValueError(f"Unbekannter Quellmodus: {job.source_mode!r}")
