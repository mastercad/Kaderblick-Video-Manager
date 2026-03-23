"""Einstellungen, Profile und persistente Konfiguration."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
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


@dataclass
class AppSettings:
    video: VideoSettings = field(default_factory=VideoSettings)
    audio: AudioSettings = field(default_factory=AudioSettings)
    youtube: YouTubeSettings = field(default_factory=YouTubeSettings)
    kaderblick: KaderblickSettings = field(default_factory=KaderblickSettings)
    cameras: CameraSettings = field(default_factory=CameraSettings)
    last_directory: str = ""
    restore_last_workflow: bool = True

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