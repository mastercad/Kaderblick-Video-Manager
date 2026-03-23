"""Dateipersistenz fuer Workflows."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from ..settings import _BASE_DIR, _DATA_DIR

if TYPE_CHECKING:
    from .model import Workflow


WORKFLOW_DIR = _BASE_DIR / "workflows"
LAST_WORKFLOW_FILE = _DATA_DIR / "last_workflow.json"


def save_workflow(workflow: "Workflow", path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(workflow.to_dict(), indent=2, ensure_ascii=False))


def load_workflow(path: Path) -> "Workflow":
    from .model import Workflow

    return Workflow.from_dict(json.loads(path.read_text()))


def load_last_workflow() -> "Workflow | None":
    if LAST_WORKFLOW_FILE.exists():
        try:
            return load_workflow(LAST_WORKFLOW_FILE)
        except Exception:
            pass
    return None