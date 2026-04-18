"""Dateipersistenz fuer Workflows."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from ..runtime_paths import data_dir, workflows_dir

if TYPE_CHECKING:
    from .model import Workflow


WORKFLOW_DIR = workflows_dir()
LAST_WORKFLOW_FILE = data_dir() / "last_workflow.json"


def save_workflow(workflow: "Workflow", path: Path, *, include_runtime: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(workflow.to_dict(include_runtime=include_runtime), indent=2, ensure_ascii=False))


def load_workflow(path: Path, *, include_runtime: bool = False) -> "Workflow":
    from .model import Workflow

    return Workflow.from_dict(json.loads(path.read_text()), include_runtime=include_runtime)


def load_last_workflow() -> "Workflow | None":
    if LAST_WORKFLOW_FILE.exists():
        try:
            return load_workflow(LAST_WORKFLOW_FILE, include_runtime=True)
        except Exception:
            pass
    return None