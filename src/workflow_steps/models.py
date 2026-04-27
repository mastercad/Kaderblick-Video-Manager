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
    graph_cursor_node_id: str = ""
    graph_origin_kind: str = "source"
    validation_results: dict[str, str] = field(default_factory=dict)
    terminal_status_text: str = ""
    delivery_branches: list["PreparedOutput"] = field(default_factory=list)


@dataclass
class ConvertItem:
    orig_idx: int
    job: WorkflowJob
    cv_job: ConvertJob
    resume_from_step: str = ""
    graph_origin_node_id: str = ""
    merge_group_id: str = ""
