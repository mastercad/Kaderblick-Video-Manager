"""Public facade for the main application package."""

from PySide6.QtCore import QThread
from PySide6.QtWidgets import QMessageBox

from ..job_workflow import JobWorkflowDialog
from ..runtime.workflow_executor import WorkflowExecutor
from ..settings import AppSettings
from ..workflow import Workflow, WorkflowJob, FileEntry, WORKFLOW_DIR
from .helpers import (
    _compute_job_overall_progress,
    _format_resume_tooltip,
    _job_has_source_config,
    _normalize_cancelled_resume_state,
    _planned_job_steps,
    _repair_restored_workflow,
    _workflow_step_progress,
)
from .window import ConverterApp
from .theme import apply_application_theme


__all__ = [
    "ConverterApp",
    "apply_application_theme",
    "AppSettings",
    "QThread",
    "QMessageBox",
    "WorkflowExecutor",
    "JobWorkflowDialog",
    "Workflow",
    "WorkflowJob",
    "FileEntry",
    "WORKFLOW_DIR",
    "_compute_job_overall_progress",
    "_format_resume_tooltip",
    "_job_has_source_config",
    "_normalize_cancelled_resume_state",
    "_planned_job_steps",
    "_repair_restored_workflow",
    "_workflow_step_progress",
]