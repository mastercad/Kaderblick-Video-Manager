"""Vordefinierte Einstellungskombinationen."""

PROFILES: dict[str, dict] = {
    "KI Auswertung": {
        "encoder": "auto",
        "lossless": False,
        "preset": "slow",
        "crf": 12,
        "output_format": "mp4",
        "no_bframes": True,
        "keyframe_interval": 1,
    },
    "YouTube": {
        "encoder": "auto",
        "lossless": False,
        "preset": "medium",
        "crf": 20,
        "output_format": "mp4",
        "no_bframes": True,
        "keyframe_interval": 1,
    },
    "Benutzerdefiniert": {},
}