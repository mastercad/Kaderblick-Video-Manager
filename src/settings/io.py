"""Hilfsfunktionen fuer das Laden und Schreiben von Einstellungen."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .model import DeviceSettings


SECRET_FIELDS: dict[str, set[str]] = {
    "kaderblick": {"jwt_token", "jwt_refresh_token", "bearer_token"},
}


def normalize_kaderblick_auth_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    return "bearer" if mode == "bearer" else "jwt"


def read_settings_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def apply_settings_payload(
    settings: Any,
    data: dict[str, Any],
    *,
    preserve_existing_secrets: bool = False,
) -> None:
    for section_name in ("video", "audio", "youtube", "kaderblick"):
        section = getattr(settings, section_name)
        secret_fields = SECRET_FIELDS.get(section_name, set())
        for key, value in data.get(section_name, {}).items():
            if not hasattr(section, key):
                continue
            if section_name == "kaderblick" and key == "auth_mode":
                value = normalize_kaderblick_auth_mode(value)
            if preserve_existing_secrets and key in secret_fields and value == "" and getattr(section, key, ""):
                continue
            setattr(section, key, value)

    cam_data = data.get("cameras", {})
    for key, value in cam_data.items():
        if key == "devices":
            settings.cameras.devices = [
                DeviceSettings(**{field_name: field_value for field_name, field_value in device.items() if hasattr(DeviceSettings(), field_name)})
                for device in (value or [])
            ]
        elif hasattr(settings.cameras, key):
            setattr(settings.cameras, key, value)

    settings.last_directory = data.get("last_directory", settings.last_directory)
    settings.workflow_output_root = data.get("workflow_output_root", getattr(settings, "workflow_output_root", ""))
    settings.default_match_date = data.get("default_match_date", getattr(settings, "default_match_date", ""))
    settings.default_match_competition = data.get("default_match_competition", getattr(settings, "default_match_competition", ""))
    settings.default_match_home_team = data.get("default_match_home_team", getattr(settings, "default_match_home_team", ""))
    settings.default_match_away_team = data.get("default_match_away_team", getattr(settings, "default_match_away_team", ""))
    settings.default_match_location = data.get("default_match_location", getattr(settings, "default_match_location", ""))
    settings.default_kaderblick_game_id = data.get("default_kaderblick_game_id", getattr(settings, "default_kaderblick_game_id", ""))
    settings.restore_last_workflow = data.get("restore_last_workflow", settings.restore_last_workflow)


def merge_blank_secrets(payload: dict[str, Any], existing: dict[str, Any] | None) -> None:
    if not existing:
        return
    for section_name, secret_fields in SECRET_FIELDS.items():
        current_section = payload.get(section_name, {})
        existing_section = existing.get(section_name, {})
        if not isinstance(current_section, dict) or not isinstance(existing_section, dict):
            continue
        for field_name in secret_fields:
            if current_section.get(field_name, "") == "" and existing_section.get(field_name, ""):
                current_section[field_name] = existing_section[field_name]


def write_settings_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")