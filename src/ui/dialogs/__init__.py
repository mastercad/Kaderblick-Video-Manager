"""Dialog package facade."""

from .camera import CameraSettingsDialog
from .general import GeneralSettingsDialog
from .job_edit import JobEditDialog
from .kaderblick import KaderblickSettingsDialog
from .shutdown import ShutdownCountdownDialog
from .video import AudioSettingsDialog, VideoSettingsDialog
from .youtube import YouTubeSettingsDialog


__all__ = [
    "VideoSettingsDialog",
    "AudioSettingsDialog",
    "YouTubeSettingsDialog",
    "JobEditDialog",
    "GeneralSettingsDialog",
    "KaderblickSettingsDialog",
    "CameraSettingsDialog",
    "ShutdownCountdownDialog",
]