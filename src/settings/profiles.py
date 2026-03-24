"""Vordefinierte Einstellungskombinationen."""

VIDEO_LABEL_PROFILE = "Profil:"
VIDEO_LABEL_PRESET = "Preset:"
VIDEO_LABEL_RESOLUTION = "Auflösung:"
VIDEO_LABEL_CONTAINER = "Container:"
VIDEO_LABEL_FPS = "Framerate (FPS):"
VIDEO_TEXT_NO_BFRAMES = "B-Frames vermeiden"

VIDEO_TOOLTIP_PRESET = (
    "Steuert den Kompromiss aus Kodiergeschwindigkeit und Effizienz. "
    "Langsamere Presets brauchen mehr Zeit, erzeugen bei gleicher Qualität aber meist kleinere Dateien."
)

VIDEO_TOOLTIP_RESOLUTION = (
    "Legt die Zielauflösung fest. Originalauflösung übernimmt Breite und Höhe aus der Quelle unverändert."
)

VIDEO_TOOLTIP_CONTAINER = (
    "Legt nur den Ausgabecontainer fest. Auflösung, Qualität und Encoder werden separat gesteuert."
)

VIDEO_TOOLTIP_NO_BFRAMES = (
    "Vermeidet B-Frames für stabileren Random-Access, robustere KI-Verarbeitung und besser vorhersagbares Seeking. "
    "Nachteil: leicht größere Dateien oder etwas geringere Effizienz."
)

VIDEO_TOOLTIP_AUDIO_SYNC = (
    "Korrigiert Drift bei MJPEG-Aufnahmen mit Frame-Drops, indem die effektive Framerate an die Audio-Dauer angepasst wird."
)

VIDEO_TOOLTIP_OVERWRITE = (
    "Wenn aktiviert, werden bestehende Ausgabedateien neu erzeugt statt wiederverwendet."
)

VIDEO_FORMAT_OPTIONS = [
    ("mp4", "MP4"),
    ("avi", "AVI"),
]

VIDEO_PRESET_OPTIONS = [
    "ultrafast",
    "superfast",
    "veryfast",
    "faster",
    "fast",
    "medium",
    "slow",
    "slower",
    "veryslow",
    "placebo",
]

VIDEO_RESOLUTION_OPTIONS = [
    ("source", "Originalauflösung"),
    ("2160p", "4K UHD (3840 x 2160)"),
    ("1440p", "QHD (2560 x 1440)"),
    ("1080p", "Full HD (1920 x 1080)"),
    ("720p", "HD (1280 x 720)"),
    ("576p", "SD PAL (1024 x 576)"),
    ("480p", "SD NTSC (854 x 480)"),
    ("360p", "nHD (640 x 360)"),
]

_VIDEO_RESOLUTION_DIMENSIONS = {
    "2160p": (3840, 2160),
    "1440p": (2560, 1440),
    "1080p": (1920, 1080),
    "720p": (1280, 720),
    "576p": (1024, 576),
    "480p": (854, 480),
    "360p": (640, 360),
}

PROFILES: dict[str, dict] = {
    "Schnell": {
        "encoder": "auto",
        "lossless": False,
        "preset": "veryfast",
        "crf": 23,
        "output_format": "mp4",
        "output_resolution": "source",
        "no_bframes": False,
        "keyframe_interval": 2,
    },
    "Ausgewogen": {
        "encoder": "auto",
        "lossless": False,
        "preset": "medium",
        "crf": 20,
        "output_format": "mp4",
        "output_resolution": "source",
        "no_bframes": True,
        "keyframe_interval": 1,
    },
    "Qualität": {
        "encoder": "auto",
        "lossless": False,
        "preset": "slow",
        "crf": 18,
        "output_format": "mp4",
        "output_resolution": "source",
        "no_bframes": True,
        "keyframe_interval": 1,
    },
    "KI Auswertung": {
        "encoder": "auto",
        "lossless": False,
        "preset": "slow",
        "crf": 12,
        "output_format": "mp4",
        "output_resolution": "source",
        "no_bframes": True,
        "keyframe_interval": 1,
    },
    "YouTube": {
        "encoder": "auto",
        "lossless": False,
        "preset": "medium",
        "crf": 20,
        "output_format": "mp4",
        "output_resolution": "source",
        "no_bframes": True,
        "keyframe_interval": 1,
    },
    "YouTube HQ": {
        "encoder": "auto",
        "lossless": False,
        "preset": "slow",
        "crf": 18,
        "output_format": "mp4",
        "output_resolution": "source",
        "no_bframes": True,
        "keyframe_interval": 1,
    },
    "Benutzerdefiniert": {},
}


def resolution_dimensions(value: str | None) -> tuple[int, int] | None:
    key = str(value or "").strip().lower()
    if key in {"", "source", "original", "originalauflösung", "originalaufloesung"}:
        return None
    return _VIDEO_RESOLUTION_DIMENSIONS.get(key)


def matching_profile_name(values: dict[str, object], keys: tuple[str, ...]) -> str:
    for profile_name, profile_values in PROFILES.items():
        if not profile_values:
            continue
        if all(values.get(key) == profile_values.get(key) for key in keys if key in profile_values):
            return profile_name
    return "Benutzerdefiniert"