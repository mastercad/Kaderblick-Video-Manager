"""Datenklassen fuer App-Einstellungen."""

from __future__ import annotations

from dataclasses import dataclass, field

from .profiles import PROFILES


@dataclass
class VideoSettings:
    fps: int = 25
    output_format: str = "mp4"
    crf: int = 18
    lossless: bool = False
    preset: str = "medium"
    encoder: str = "auto"
    profile: str = "Benutzerdefiniert"
    overwrite: bool = False
    audio_sync: bool = False
    merge_halves: bool = False
    merge_title_duration: int = 3
    merge_title_bg: str = "#000000"
    merge_title_fg: str = "#FFFFFF"
    no_bframes: bool = True
    keyframe_interval: int = 1

    def apply_profile(self, profile_name: str) -> None:
        if profile_name not in PROFILES:
            return
        values = PROFILES[profile_name]
        for key, value in values.items():
            if hasattr(self, key):
                setattr(self, key, value)
        self.profile = profile_name


@dataclass
class AudioSettings:
    include_audio: bool = True
    amplify_audio: bool = True
    amplify_db: float = 6.0
    audio_suffix: str = ""
    audio_bitrate: str = "192k"


@dataclass
class YouTubeSettings:
    create_youtube: bool = False
    youtube_crf: int = 23
    youtube_maxrate: str = "8M"
    youtube_bufsize: str = "16M"
    youtube_audio_bitrate: str = "128k"
    upload_to_youtube: bool = False


@dataclass
class KaderblickSettings:
    base_url: str = "https://api.kaderblick.de"
    auth_mode: str = "jwt"
    jwt_token: str = ""
    jwt_refresh_token: str = ""
    bearer_token: str = ""


@dataclass
class DeviceSettings:
    name: str = ""
    ip: str = ""
    port: int = 22
    username: str = ""
    password: str = ""
    ssh_key: str = ""


@dataclass
class CameraSettings:
    source: str = "/home/kaderblick/camera_api/recordings"
    destination: str = ""
    delete_after_download: bool = False
    auto_convert: bool = True
    devices: list = field(default_factory=list)