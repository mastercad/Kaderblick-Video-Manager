"""Settings dialog actions for the main window."""

from __future__ import annotations


def _open_camera_settings(self):
    from ..ui.dialogs import CameraSettingsDialog

    dialog = CameraSettingsDialog(self, self.settings)
    if dialog.exec():
        self.settings.save()


def _open_video_settings(self):
    from ..ui.dialogs import VideoSettingsDialog

    dialog = VideoSettingsDialog(self, self.settings)
    dialog.exec()


def _open_audio_settings(self):
    from ..ui.dialogs import AudioSettingsDialog

    AudioSettingsDialog(self, self.settings).exec()


def _open_youtube_settings(self):
    from ..ui.dialogs import YouTubeSettingsDialog

    YouTubeSettingsDialog(self, self.settings).exec()


def _open_kaderblick_settings(self):
    from ..ui.dialogs import KaderblickSettingsDialog

    KaderblickSettingsDialog(self, self.settings).exec()


def _open_general_settings(self):
    from ..ui.dialogs import GeneralSettingsDialog

    GeneralSettingsDialog(self, self.settings).exec()