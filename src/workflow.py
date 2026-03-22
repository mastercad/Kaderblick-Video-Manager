"""Workflow-Datenmodell.

Ein Workflow ist eine geordnete Liste von Aufträgen (WorkflowJob).
Jeder Auftrag definiert:
  - Woher die Dateien kommen  (direkte Auswahl | Pi-Download | Ordner)
  - Wie sie verarbeitet werden (Encoding, Audio – alles im Auftrag)
  - Was mit ihnen passiert    (YouTube-Upload, Umbenennungen)
  - Per-Datei-Metadaten       (Ausgabename, YT-Titel, Playlist)

Rückwärtskompatibilität:
  Altes Format (sources / WorkflowSource) wird beim Laden automatisch
  in das neue Format migriert.
"""

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

from .settings import _DATA_DIR

WORKFLOW_DIR = _DATA_DIR / "workflows"
LAST_WORKFLOW_FILE = _DATA_DIR / "last_workflow.json"


# ─────────────────────────────────────────────────────────────────
#  FileEntry – Metadaten für eine einzelne Quelldatei
# ─────────────────────────────────────────────────────────────────

@dataclass
class FileEntry:
    """Metadaten für eine einzelne Quelldatei innerhalb eines Auftrags."""

    source_path: str = ""
    output_filename: str = ""      # leer = automatisch aus Quelldateiname ableiten
    youtube_title: str = ""        # leer = Dateiname als Titel verwenden
    youtube_description: str = ""  # leer = automatisch aus Metadaten generiert
    youtube_playlist: str = ""
    # Kaderblick-spezifische Felder (pro Datei)
    kaderblick_game_id: str = ""      # leer = Job-Standard verwenden
    kaderblick_game_start: int = 0    # Startzeit in Sekunden (Standard 0)
    kaderblick_video_type_id: int = 0 # 0 = Job-Standard verwenden
    kaderblick_camera_id: int = 0    # 0 = Job-Standard verwenden
    # Merge-Gruppe: nicht-leer = Datei gehört zu einer Zusammenführungs-Gruppe.
    # Die erste Datei der Gruppe hält den gemeinsamen YT-Titel/Playlist.
    merge_group_id: str = ""
    # Individuelle Titelkarte für diese Quelldatei (leer = Auftrag-Titel verwenden)
    title_card_subtitle: str = ""   # z.B. "1. Halbzeit", "Kamera 1"


# ─────────────────────────────────────────────────────────────────
#  WorkflowJob – ein einzelner ausführbarer Auftrag
# ─────────────────────────────────────────────────────────────────

# Laufzeitfelder werden nicht in die JSON-Datei geschrieben.
_RUNTIME_FIELDS = {"status", "progress_pct", "overall_progress_pct", "current_step_key", "error_msg"}


@dataclass
class WorkflowJob:
    """Ein einzelner Auftrag im Workflow.

    Quellmodi
    ---------
    "files"        – direkt ausgewählte Dateien (Liste von FileEntry-Objekten)
    "pi_download"  – Aufnahmen von einer Raspberry-Pi-Kamera herunterladen
    "folder_scan"  – Dateien in einem lokalen Ordner scannen / kopieren
    """

    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    enabled: bool = True
    name: str = ""

    # ── Quellmodus ────────────────────────────────────────────
    source_mode: str = "files"     # "files" | "pi_download" | "folder_scan"

    # files-Modus: direkt ausgewählte Dateien
    files: list = field(default_factory=list)   # list[FileEntry]

    # pi_download-Modus
    device_name: str = ""
    download_destination: str = ""
    delete_after_download: bool = False

    # folder_scan-Modus
    source_folder: str = ""
    file_pattern: str = "*.mp4"
    copy_destination: str = ""    # leer = Dateien direkt am Quellort verarbeiten
    move_files: bool = False      # True = verschieben, False = kopieren / an Ort lassen
    output_prefix: str = ""       # optionaler Präfix für umbenannte Ausgabedateien

    # ── Verarbeitung ──────────────────────────────────────────
    convert_enabled: bool = True
    encoder: str = "auto"
    crf: int = 18
    preset: str = "medium"
    fps: int = 25
    output_format: str = "mp4"
    overwrite: bool = False     # vorhandene Ausgabedateien überschreiben

    # ── Audio ─────────────────────────────────────────────────
    merge_audio: bool = False      # separate A+V-Dateien zusammenführen
    amplify_audio: bool = False    # Lautstärke anheben
    amplify_db: float = 6.0        # Verstärkung in dB
    audio_sync: bool = False       # Frame-Drop-Korrektur

    # ── YouTube ───────────────────────────────────────────────
    create_youtube_version: bool = False   # separate YT-optimierte Version erzeugen
    upload_youtube: bool = False           # auf YouTube hochladen
    default_youtube_title: str = ""        # Standard-Titel (Vorlage)
    default_youtube_playlist: str = ""     # Standard-Playlist
    default_youtube_competition: str = ""  # Wettbewerb für Metadaten-Dialog
    # ── Kaderblick ─────────────────────────────────────────────
    upload_kaderblick: bool = False        # Video auf Kaderblick eintragen
    default_kaderblick_game_id: str = ""   # Spiel-ID (wird pro Datei geerbt)
    default_kaderblick_video_type_id: int = 0  # Video-Typ-ID (Standard 0 = nicht gesetzt)
    default_kaderblick_camera_id: int = 0      # Kamera-ID für Kaderblick
    # ── Titelkarte ────────────────────────────────────────────
    title_card_enabled: bool = False       # Titelkarte vor jedem Video einblenden
    title_card_logo_path: str = ""         # Pfad zum Logo-Bild (leer = kein Logo)
    title_card_duration: float = 3.0       # Anzeigedauer in Sekunden
    title_card_bg_color: str = "#000000"   # Hintergrundfarbe
    title_card_fg_color: str = "#FFFFFF"   # Textfarbe
    title_card_home_team: str = ""         # Heimmannschaft
    title_card_away_team: str = ""         # Auswärtsmannschaft
    title_card_date: str = ""              # Datum (YYYY-MM-DD oder Anzeigetext)
    # ── Resume-Info (persistiert) ───────────────────────────
    resume_status: str = ""                # letzter sichtbarer Status beim vorherigen Lauf
    step_statuses: dict = field(default_factory=dict)  # z.B. {"transfer": "done"}
    # ── Laufzeit (nicht persistiert) ──────────────────────────
    status: str = "Wartend"
    progress_pct: int = 0
    overall_progress_pct: int = 0
    current_step_key: str = ""
    error_msg: str = ""

    # ── Serialisierung ────────────────────────────────────────

    def to_dict(self) -> dict:
        d = asdict(self)
        for key in _RUNTIME_FIELDS:
            d.pop(key, None)
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "WorkflowJob":
        valid = set(cls.__dataclass_fields__)
        filtered = {k: v for k, v in data.items()
                    if k in valid and k not in _RUNTIME_FIELDS}
        raw_files = filtered.pop("files", [])
        files = [
            FileEntry(**{k: v for k, v in f.items()
                         if k in FileEntry.__dataclass_fields__})
            for f in raw_files
        ]
        return cls(files=files, **filtered)


# ─────────────────────────────────────────────────────────────────
#  Workflow – Sammlung aller Aufträge
# ─────────────────────────────────────────────────────────────────

@dataclass
class Workflow:
    """Kompletter Workflow: geordnete Liste von Aufträgen + globale Optionen."""

    name: str = ""
    jobs: list = field(default_factory=list)   # list[WorkflowJob]
    shutdown_after: bool = False
    created_at: str = field(
        default_factory=lambda: datetime.now().isoformat(timespec="seconds"))

    # ── Serialisierung ────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "shutdown_after": self.shutdown_after,
            "created_at": self.created_at,
            "jobs": [j.to_dict() for j in self.jobs],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Workflow":
        raw_jobs = data.get("jobs", [])

        # Migrationsfall: altes Format mit "sources" / WorkflowSource
        if not raw_jobs and "sources" in data:
            raw_jobs = [_migrate_source_to_job(s) for s in data["sources"]]

        jobs = [WorkflowJob.from_dict(j) for j in raw_jobs]
        return cls(
            name=data.get("name", ""),
            jobs=jobs,
            shutdown_after=data.get("shutdown_after", False),
            created_at=data.get("created_at", ""),
        )

    # ── Dateipersistenz ───────────────────────────────────────

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_dict(), indent=2, ensure_ascii=False))

    @classmethod
    def load(cls, path: Path) -> "Workflow":
        return cls.from_dict(json.loads(path.read_text()))

    def save_as_last(self) -> None:
        self.save(LAST_WORKFLOW_FILE)

    @classmethod
    def load_last(cls) -> "Workflow | None":
        if LAST_WORKFLOW_FILE.exists():
            try:
                return cls.load(LAST_WORKFLOW_FILE)
            except Exception:
                pass
        return None


# ─────────────────────────────────────────────────────────────────
#  Migration: altes WorkflowSource-Format → WorkflowJob
# ─────────────────────────────────────────────────────────────────

def _migrate_source_to_job(source: dict) -> dict:
    """Konvertiert ein altes WorkflowSource-dict in das neue WorkflowJob-Format."""
    src_type = source.get("source_type", "local")
    src_path = source.get("source_path", "")

    # Quellmodus bestimmen
    if src_type == "pi_camera":
        mode = "pi_download"
    elif src_path and Path(src_path).is_file():
        mode = "files"
    else:
        mode = "folder_scan"

    # Per-Datei-Eintrag für Einzeldatei-Modus
    files = []
    if mode == "files":
        files = [{
            "source_path": src_path,
            "output_filename": source.get("output_filename", ""),
            "youtube_title": source.get("youtube_title", ""),
            "youtube_playlist": source.get("youtube_playlist", ""),
        }]

    return {
        "id": source.get("id", uuid.uuid4().hex[:8]),
        "enabled": source.get("enabled", True),
        "name": source.get("name", ""),
        "source_mode": mode,
        "files": files,
        "device_name": source.get("device_name", ""),
        "download_destination": source.get("destination_path", ""),
        "delete_after_download": source.get("delete_source", False),
        "source_folder": src_path if mode == "folder_scan" else "",
        "file_pattern": source.get("file_extensions", "*.mp4"),
        "copy_destination": (source.get("destination_path", "")
                             if source.get("move_to_destination") else ""),
        "move_files": source.get("move_to_destination", False),
        "convert_enabled": True,
        "encoder": source.get("encoder", "auto"),
        "crf": source.get("crf", 18),
        "preset": source.get("preset", "medium"),
        "fps": source.get("fps", 25),
        "output_format": source.get("output_format", "mp4"),
        "merge_audio": source.get("merge_audio_video", False),
        "amplify_audio": source.get("amplify_audio", False),
        "amplify_db": source.get("amplify_db", 6.0),
        "audio_sync": source.get("audio_sync", False),
        "create_youtube_version": source.get("create_youtube", False),
        "upload_youtube": source.get("upload_youtube", False),
        "default_youtube_title": source.get("youtube_title", ""),
        "default_youtube_playlist": source.get("youtube_playlist", ""),
    }
