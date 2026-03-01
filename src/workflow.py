"""Workflow-Datenmodell: Zwei-Etappen-Auftrags-Baukasten.

Etappe 1 – Quellen:
  Definiert von welchen Quellen Dateien heruntergeladen oder verschoben
  werden (Pi-Kameras, lokale Quellen / Datenträger).

Etappe 2 – Verarbeitung (pro Quelle):
  Noch bevor die Dateien existieren, wird pro Quelle festgelegt:
    • Encoding-Einstellungen
    • Soll Audio+Video gemerged werden?
    • Soll Audio verstärkt werden?
    • Soll eine YouTube-Version erstellt werden?
    • YouTube-Titel + Playlist
    • Soll auf YouTube hochgeladen werden?
    • Wie soll die Ausgabedatei heißen?

Globale Optionen:
  • Rechner nach Abschluss herunterfahren
"""

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

from .settings import _DATA_DIR

WORKFLOW_DIR = _DATA_DIR / "workflows"
LAST_WORKFLOW_FILE = _DATA_DIR / "last_workflow.json"


# ═════════════════════════════════════════════════════════════════
#  Workflow-Quelle (Etappe 1 + Etappe 2 zusammen)
#
#  Jede Quelle beschreibt:
#    - Woher die Dateien kommen (Kamera, SSD, Ordner)
#    - Was damit passieren soll (Verarbeitung, YouTube)
# ═════════════════════════════════════════════════════════════════

@dataclass
class WorkflowSource:
    """Eine Quelle im Workflow mit zugehöriger Verarbeitungskonfiguration."""

    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    enabled: bool = True

    # ── Etappe 1: Quelle + Transfer ─────────────────────────
    source_type: str = "pi_camera"    # "pi_camera" | "local"
    name: str = ""                    # Anzeigename (z.B. "Kamera 1")

    #  Pi-Kamera
    device_name: str = ""             # Verweist auf DeviceSettings.name

    #  Lokale Quelle / Datenträger
    source_path: str = ""             # Quellordner oder Videodatei
    audio_path: str = ""              # Explizite Audio-Datei (Einzel-Datei-Modus)
    file_extensions: str = "*.mp4"    # Glob-Pattern für Dateien (Ordner-Modus)
    move_to_destination: bool = False  # Dateien in Zielordner verschieben

    #  Gemeinsam
    destination_path: str = ""        # Zielordner
    delete_source: bool = False       # Quelldateien nach Transfer löschen

    # ── Etappe 2: Verarbeitung (wird VOR Download konfiguriert) ─
    merge_audio_video: bool = True    # Audio + Video zusammenführen
    amplify_audio: bool = False       # Audioverstärkung (compand+loudnorm)
    audio_sync: bool = False          # Frame-Drop-Korrektur
    output_filename: str = ""         # "{name}" → wird beim Konvertieren ersetzt

    #  Encoding
    encoder: str = "auto"             # auto | h264_nvenc | libx264
    crf: int = 18
    preset: str = "medium"
    fps: int = 25
    output_format: str = "mp4"

    #  YouTube
    create_youtube: bool = False      # YouTube-optimierte Version erzeugen
    upload_youtube: bool = False       # Auf YouTube hochladen
    youtube_title: str = ""           # "{ name }" als Platzhalter möglich
    youtube_playlist: str = ""

    # ── Laufzeit (nicht persistiert) ─────────────────────────
    status: str = "Wartend"           # Wartend | Herunterladen | Verarbeiten | Fertig | Fehler
    progress_pct: int = 0
    error_msg: str = ""


# Felder die nicht in die JSON-Datei geschrieben werden
_RUNTIME_FIELDS = {"status", "progress_pct", "error_msg"}


# ═════════════════════════════════════════════════════════════════
#  Workflow (Sammlung aller Quellen + globale Optionen)
# ═════════════════════════════════════════════════════════════════

@dataclass
class Workflow:
    """Kompletter Workflow mit allen Quellen und globalen Optionen."""

    name: str = ""
    sources: list[WorkflowSource] = field(default_factory=list)
    shutdown_after: bool = False
    upload_youtube_global: bool = False   # Globaler YouTube-Upload-Schalter
    created_at: str = field(
        default_factory=lambda: datetime.now().isoformat(timespec="seconds"))

    # ── Serialisierung ───────────────────────────────────────

    def to_dict(self) -> dict:
        """Konvertiert den Workflow in ein JSON-fähiges dict."""
        data = {
            "name": self.name,
            "shutdown_after": self.shutdown_after,
            "upload_youtube_global": self.upload_youtube_global,
            "created_at": self.created_at,
            "sources": [],
        }
        for src in self.sources:
            d = asdict(src)
            for key in _RUNTIME_FIELDS:
                d.pop(key, None)
            data["sources"].append(d)
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "Workflow":
        """Erzeugt einen Workflow aus einem dict."""
        valid_fields = set(WorkflowSource.__dataclass_fields__)
        sources = []
        for raw in data.get("sources", []):
            filtered = {k: v for k, v in raw.items()
                        if k in valid_fields and k not in _RUNTIME_FIELDS}
            sources.append(WorkflowSource(**filtered))
        return cls(
            name=data.get("name", ""),
            sources=sources,
            shutdown_after=data.get("shutdown_after", False),
            upload_youtube_global=data.get("upload_youtube_global", False),
            created_at=data.get("created_at", ""),
        )

    # ── Dateipersistenz ──────────────────────────────────────

    def save(self, path: Path) -> None:
        """Speichert den Workflow als JSON-Datei."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_dict(), indent=2, ensure_ascii=False))

    @classmethod
    def load(cls, path: Path) -> "Workflow":
        """Lädt einen Workflow aus einer JSON-Datei."""
        return cls.from_dict(json.loads(path.read_text()))

    def save_as_last(self) -> None:
        """Speichert den Workflow als zuletzt verwendeten Workflow."""
        self.save(LAST_WORKFLOW_FILE)

    @classmethod
    def load_last(cls) -> "Workflow | None":
        """Lädt den zuletzt verwendeten Workflow, falls vorhanden."""
        if LAST_WORKFLOW_FILE.exists():
            try:
                return cls.load(LAST_WORKFLOW_FILE)
            except Exception:
                pass
        return None
