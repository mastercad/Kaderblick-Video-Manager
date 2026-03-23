"""Hilfsfunktionen fuer Workflow-Namen."""

from __future__ import annotations

import re


_NAME_SUFFIX_RE = re.compile(r"^(?P<base>.*?)(?: \((?P<num>\d+)\))?$")


def normalize_workflow_name(name: str) -> str:
    return (name or "").strip()


def increment_workflow_name(name: str, existing_names: list[str] | set[str]) -> str:
    normalized = normalize_workflow_name(name) or "Workflow"
    existing = {
        normalize_workflow_name(entry)
        for entry in existing_names
        if normalize_workflow_name(entry)
    }
    if normalized not in existing:
        return normalized

    match = _NAME_SUFFIX_RE.match(normalized)
    assert match is not None
    base = normalize_workflow_name(match.group("base") or normalized) or "Workflow"
    current_num = int(match.group("num") or "1")
    candidate_num = max(2, current_num + 1 if match.group("num") else 2)

    while True:
        candidate = f"{base} ({candidate_num})"
        if candidate not in existing:
            return candidate
        candidate_num += 1