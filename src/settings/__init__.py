"""Einstellungen, Profile und persistente Konfiguration."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
import re
from typing import Any

from .io import (
    apply_settings_payload,
    merge_blank_secrets,
    read_settings_payload,
    write_settings_payload,
)
from .model import (
    AudioSettings,
    CameraSettings,
    DeviceSettings,
    KaderblickSettings,
    VideoSettings,
    YouTubeSettings,
)
from .profiles import PROFILES


_BASE_DIR = Path(__file__).resolve().parent.parent.parent
_CONFIG_DIR = _BASE_DIR / "config"
_DATA_DIR = _BASE_DIR / "data"

SETTINGS_FILE = _CONFIG_DIR / "settings.json"
CLIENT_SECRET_FILE = _CONFIG_DIR / "client_secret.json"
TOKEN_FILE = _DATA_DIR / "youtube_token.json"

_INVALID_OUTPUT_SEGMENT_RE = re.compile(r'[\\/:*?"<>|]+')
_WORKFLOW_STAGE_SEGMENTS = {"raw", "processed"}


def _sanitize_output_segment(name: str) -> str:
    sanitized = _INVALID_OUTPUT_SEGMENT_RE.sub("_", (name or "").strip())
    sanitized = sanitized.strip().strip(".")
    return sanitized or "Workflow"


def _default_output_leaf(workflow_name: str, device_name: str = "", current_date: date | None = None) -> str:
    device_segment = (device_name or "").strip()
    if device_segment:
        return _sanitize_output_segment(device_segment)
    day = (current_date or date.today()).isoformat()
    return _sanitize_output_segment(f"{workflow_name or 'Workflow'} {day}")


def _normalize_stage_root(path_value: str) -> Path | None:
    raw = (path_value or "").strip()
    if not raw:
        return None
    root = Path(raw)
    if root.name.lower() in _WORKFLOW_STAGE_SEGMENTS:
        return root.parent
    return root


@dataclass
class AppSettings:
    video: VideoSettings = field(default_factory=VideoSettings)
    audio: AudioSettings = field(default_factory=AudioSettings)
    youtube: YouTubeSettings = field(default_factory=YouTubeSettings)
    kaderblick: KaderblickSettings = field(default_factory=KaderblickSettings)
    cameras: CameraSettings = field(default_factory=CameraSettings)
    workflow_output_root: str = ""
    default_match_date: str = ""
    default_match_competition: str = ""
    default_match_home_team: str = ""
    default_match_away_team: str = ""
    last_directory: str = ""
    restore_last_workflow: bool = True

    def _output_dir_for_root(self, root: str, workflow_name: str, device_name: str = "") -> str:
        root = (root or "").strip()
        if not root:
            return ""
        return str(Path(root) / _default_output_leaf(workflow_name, device_name))

    def workflow_output_dir_for(self, workflow_name: str, device_name: str = "") -> str:
        return self._output_dir_for_root(self.workflow_output_root, workflow_name, device_name)

    def workflow_raw_dir_for(self, workflow_name: str, device_name: str = "") -> str:
        base_dir = self.workflow_output_dir_for(workflow_name, device_name)
        return self.stage_dir_for(base_dir, "raw")

    def workflow_processed_dir_for(self, workflow_name: str, device_name: str = "") -> str:
        base_dir = self.workflow_output_dir_for(workflow_name, device_name)
        return self.stage_dir_for(base_dir, "processed")

    @staticmethod
    def stage_root_for(path_value: str) -> str:
        root = _normalize_stage_root(path_value)
        return str(root) if root is not None else ""

    @staticmethod
    def stage_dir_for(path_value: str, stage: str) -> str:
        stage_name = (stage or "").strip().lower()
        if stage_name not in _WORKFLOW_STAGE_SEGMENTS:
            raise ValueError(f"Unbekannter Workflow-Stage-Ordner: {stage}")
        root = _normalize_stage_root(path_value)
        if root is None:
            return ""
        return str(root / stage_name)

    def default_match_values(self) -> dict[str, str]:
        return {
            "date_iso": (self.default_match_date or "").strip(),
            "competition": (self.default_match_competition or "").strip(),
            "home_team": (self.default_match_home_team or "").strip(),
            "away_team": (self.default_match_away_team or "").strip(),
        }

    def save(self, preserve_existing_secrets: bool = True):
        payload = asdict(self)
        if preserve_existing_secrets:
            existing = self._load_existing_payload_for_merge()
            merge_blank_secrets(payload, existing)
        write_settings_payload(SETTINGS_FILE, payload)

    @classmethod
    def load(cls) -> "AppSettings":
        settings = cls()
        config_data = cls._read_settings_payload(SETTINGS_FILE)
        if config_data is not None:
            cls._apply_payload(settings, config_data)
        
        return settings

    @staticmethod
    def _read_settings_payload(path: Path) -> dict[str, Any] | None:
        return read_settings_payload(path)

    @classmethod
    def _apply_payload(
        cls,
        settings: "AppSettings",
        data: dict[str, Any],
        *,
        preserve_existing_secrets: bool = False,
    ) -> None:
        apply_settings_payload(settings, data, preserve_existing_secrets=preserve_existing_secrets)

    @classmethod
    def _merge_blank_secrets(cls, payload: dict[str, Any], existing: dict[str, Any] | None) -> None:
        merge_blank_secrets(payload, existing)

    @classmethod
    def _load_existing_payload_for_merge(cls) -> dict[str, Any] | None:
        return cls._read_settings_payload(SETTINGS_FILE)

__all__ = [
    "_BASE_DIR",
    "_CONFIG_DIR",
    "_DATA_DIR",
    "SETTINGS_FILE",
    "CLIENT_SECRET_FILE",
    "TOKEN_FILE",
    "PROFILES",
    "VideoSettings",
    "AudioSettings",
    "YouTubeSettings",
    "KaderblickSettings",
    "DeviceSettings",
    "CameraSettings",
    "AppSettings",
]