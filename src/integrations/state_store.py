"""Gemeinsamer JSON-Zustand fuer Integrationen und UI-Historie."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..settings import _DATA_DIR


INTEGRATION_STATE_FILE = _DATA_DIR / "integration_state.json"


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def load_state(path: Path = INTEGRATION_STATE_FILE) -> dict[str, Any]:
    return _read_json(path) or {}


def save_state(data: dict[str, Any], path: Path = INTEGRATION_STATE_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def load_section(
    section: str,
    *,
    path: Path = INTEGRATION_STATE_FILE,
) -> dict[str, Any]:
    state = load_state(path)
    section_data = state.get(section)
    if isinstance(section_data, dict):
        return section_data
    return {}


def save_section(section: str, data: dict[str, Any], *, path: Path = INTEGRATION_STATE_FILE) -> None:
    state = load_state(path)
    state[section] = data
    save_state(state, path)