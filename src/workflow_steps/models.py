from __future__ import annotations

from dataclasses import dataclass

from ..converter import ConvertJob
from ..settings import AppSettings
from ..workflow import WorkflowJob


@dataclass
class PreparedOutput:
    orig_idx: int
    job: WorkflowJob
    cv_job: ConvertJob
    per_settings: AppSettings
    status_prefix: str = ""
    mark_finished: bool = True


@dataclass
class ConvertItem:
    orig_idx: int
    job: WorkflowJob
    cv_job: ConvertJob


@dataclass
class TransferPhaseResult:
    convert_items: list[ConvertItem]
    transfer_fail: int
    cancelled: bool = False


@dataclass
class ProcessingResult:
    ok: int
    skip: int
    fail: int
