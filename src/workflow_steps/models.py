from __future__ import annotations

from dataclasses import dataclass, field

from ..media.converter import ConvertJob
from ..settings import AppSettings
from ..workflow import WorkflowJob


@dataclass
class PreparedOutput:
    orig_idx: int
    job: WorkflowJob
    cv_job: ConvertJob
    per_settings: AppSettings
    resume_from_step: str = ""
    status_prefix: str = ""
    mark_finished: bool = True
    title_card_enabled_override: bool | None = None
    repair_enabled_override: bool | None = None
    youtube_version_enabled_override: bool | None = None
    youtube_upload_enabled_override: bool | None = None
    kaderblick_enabled_override: bool | None = None
    graph_origin_node_id: str = ""
    graph_origin_kind: str = "source"
    validation_results: dict[str, str] = field(default_factory=dict)
    terminal_status_text: str = ""


@dataclass
class ConvertItem:
    orig_idx: int
    job: WorkflowJob
    cv_job: ConvertJob
    resume_from_step: str = ""


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
