"""Workflow-Executor: Wandelt ein Workflow-Modell in ausführbare Jobs um.

Überbrückt das WorkflowSource-Modell zur bestehenden Pipeline:
  WorkflowSource  →  ConvertJob (download) + ConvertJob (convert)

Etappe 1 – Transfer:
  • pi_camera  →  DownloadWorker (SSH/rsync)
  • local      →  Dateien direkt scannen oder in Zielordner verschieben

Etappe 2 – Verarbeitung:
  • ConvertJob wird mit den quellenspezifischen Einstellungen bestückt
  • Jede Quelle hat eigene Encoding/Audio/YouTube-Einstellungen
"""

import shutil
import threading
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QObject, Signal, Slot

from .settings import AppSettings, DeviceSettings, CameraSettings, AudioSettings
from .converter import ConvertJob, run_convert
from .downloader import download_device
from .youtube import get_youtube_service, upload_to_youtube
from .workflow import Workflow, WorkflowSource


class WorkflowExecutor(QObject):
    """Führt einen Workflow zweiphasig aus.

    Phase 1: Alle Downloads/Verschiebungen (sequenziell pro Quelle,
             weil Kamera-Akkus geschont werden sollen).
    Phase 2: Konvertierung aller heruntergeladenen Dateien
             mit quellenspezifischen Einstellungen.

    Signale
    -------
    log_message(str)
    source_status(int, str)          # (source_index, status)
    source_progress(int, int)        # (source_index, percent)
    overall_progress(int, int)       # (done_sources, total_sources)
    file_progress(str, str, float, float, float)  # download: (device, file, transferred, total, speed)
    convert_progress(int, int)       # (convert_idx, percent)
    phase_changed(int)               # 1=Download, 2=Konvertierung
    finished(int, int, int)          # (ok, skip, fail)
    """

    log_message      = Signal(str)
    source_status    = Signal(int, str)
    source_progress  = Signal(int, int)
    overall_progress = Signal(int, int)
    file_progress    = Signal(str, str, float, float, float)
    convert_progress = Signal(int, int)
    phase_changed    = Signal(int)
    finished         = Signal(int, int, int)

    def __init__(self, workflow: Workflow, settings: AppSettings):
        super().__init__()
        self._workflow = workflow
        self._settings = settings
        self._cancel = threading.Event()

        # Zur Laufzeit gefüllte Job-Liste (für Phase 2)
        self._convert_jobs: list[tuple[WorkflowSource, ConvertJob]] = []

    def cancel(self):
        self._cancel.set()

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    @Slot()
    def run(self):
        active = [s for s in self._workflow.sources if s.enabled]
        if not active:
            self.finished.emit(0, 0, 0)
            return

        # ═══════════════════════════════════════════════════════
        #  Phase 1: Downloads / Verschiebungen
        # ═══════════════════════════════════════════════════════
        self.phase_changed.emit(1)
        self.log_message.emit(
            f"\n{'═'*60}"
            f"\n  📥 Phase 1: Downloads & Transfers ({len(active)} Quellen)"
            f"\n{'═'*60}")

        for src_idx, src in enumerate(active):
            if self._cancel.is_set():
                break

            src.status = "Herunterladen"
            self.source_status.emit(src_idx, src.status)

            try:
                files = self._transfer_source(src_idx, src)
            except Exception as e:
                src.status = "Fehler"
                src.error_msg = str(e)
                self.source_status.emit(src_idx, src.status)
                self.log_message.emit(f"❌ {src.name}: {e}")
                continue

            if self._cancel.is_set():
                break

            src.status = "Heruntergeladen"
            self.source_status.emit(src_idx, src.status)
            self.overall_progress.emit(src_idx + 1, len(active))

            # ConvertJobs für Phase 2 erzeugen
            for fpath in files:
                job = self._create_convert_job(src, fpath)
                self._convert_jobs.append((src, job))

        if self._cancel.is_set():
            self.log_message.emit("Phase 1 abgebrochen.")
            self.finished.emit(0, 0, 0)
            return

        # ═══════════════════════════════════════════════════════
        #  Phase 2: Konvertierung
        # ═══════════════════════════════════════════════════════
        if not self._convert_jobs:
            self.log_message.emit("\nKeine Dateien zum Konvertieren.")
            self.finished.emit(0, 0, 0)
            return

        self.phase_changed.emit(2)
        total = len(self._convert_jobs)
        self.log_message.emit(
            f"\n{'═'*60}"
            f"\n  🔄 Phase 2: Konvertierung ({total} Datei(en))"
            f"\n{'═'*60}")

        yt_service = None
        needs_yt = any(s.upload_youtube for s in active)
        if needs_yt:
            self.log_message.emit("YouTube-Anmeldung …")
            yt_service = get_youtube_service(
                log_callback=self.log_message.emit)
            if not yt_service:
                self.log_message.emit(
                    "⚠ YouTube-Upload deaktiviert (Anmeldung fehlgeschlagen)")

        ok = skip = fail = 0
        for conv_idx, (src, job) in enumerate(self._convert_jobs):
            if self._cancel.is_set():
                self.log_message.emit("Konvertierung abgebrochen.")
                break

            self.log_message.emit(
                f"\n═══ [{conv_idx+1}/{total}] {src.name}: "
                f"{job.source_path.name} ═══")

            # Settings temporär pro Quelle anpassen
            per_source_settings = self._make_source_settings(src)

            job.status = "Läuft"
            job.progress_pct = 0

            def _progress_cb(pct, _idx=conv_idx, _job=job):
                _job.progress_pct = pct
                self.convert_progress.emit(_idx, pct)

            success = run_convert(
                job, per_source_settings,
                cancel_flag=self._cancel,
                log_callback=self.log_message.emit,
                progress_callback=_progress_cb)

            if self._cancel.is_set():
                break

            if success and job.status == "Fertig":
                ok += 1
                # YouTube-Upload
                if src.upload_youtube and yt_service:
                    upload_to_youtube(
                        job, per_source_settings, yt_service,
                        log_callback=self.log_message.emit)
            elif job.status == "Übersprungen":
                skip += 1
            else:
                fail += 1

            self.overall_progress.emit(conv_idx + 1, total)

        self.log_message.emit(
            f"\n✅ Workflow abgeschlossen: {ok} OK, {skip} übersprungen, "
            f"{fail} Fehler")
        self.finished.emit(ok, skip, fail)

    # ── Transfer (Phase 1) ────────────────────────────────────

    def _transfer_source(self, src_idx: int,
                         src: WorkflowSource) -> list[str]:
        """Überträgt Dateien einer Quelle und gibt Pfade zurück."""

        if src.source_type == "pi_camera":
            return self._download_pi(src_idx, src)
        elif src.source_type == "local":
            return self._handle_local(src)
        else:
            raise ValueError(f"Unbekannter Quelltyp: {src.source_type}")

    def _download_pi(self, src_idx: int,
                     src: WorkflowSource) -> list[str]:
        """Lädt Aufnahmen von einer Pi-Kamera herunter."""
        # Gerät finden
        dev = None
        for d in self._settings.cameras.devices:
            if d.name == src.device_name:
                dev = d
                break
        if not dev:
            raise RuntimeError(
                f"Gerät '{src.device_name}' nicht in Einstellungen")

        self.log_message.emit(
            f"\n⬇ {src.name}: Download von {dev.ip} …")

        # Speed-Tracking Variablen
        import time
        speed_state = {
            "speed_bps": 0.0,
            "last_time": 0.0,
            "last_transferred": 0,
            "current_file": "",
        }

        def _dl_progress(device_name, filename, transferred, total):
            now = time.monotonic()
            # Neue Datei?
            if filename != speed_state["current_file"]:
                speed_state["current_file"] = filename
                speed_state["speed_bps"] = 0.0
                speed_state["last_time"] = now
                speed_state["last_transferred"] = transferred

            # Throttle
            if transferred < total and (now - speed_state["last_time"]) < 0.25:
                return

            dt = now - speed_state["last_time"]
            if dt >= 0.5 and speed_state["last_time"] > 0:
                speed = (transferred - speed_state["last_transferred"]) / dt
                speed_state["speed_bps"] = (
                    speed if speed_state["speed_bps"] == 0
                    else 0.3 * speed + 0.7 * speed_state["speed_bps"])
                speed_state["last_transferred"] = transferred
                speed_state["last_time"] = now
            elif speed_state["last_time"] == 0:
                speed_state["last_transferred"] = transferred
                speed_state["last_time"] = now

            self.file_progress.emit(
                device_name, filename,
                float(transferred), float(total),
                speed_state["speed_bps"])

        results = download_device(
            device=dev,
            config=self._settings.cameras,
            log_cb=self.log_message.emit,
            progress_cb=_dl_progress,
            cancel_flag=self._cancel,
            destination_override=src.destination_path,
            delete_after_download=src.delete_source,
        )
        # results: list of (local_dir, base, mjpg_path)
        return [r[2] for r in results]

    def _handle_local(self, src: WorkflowSource) -> list[str]:
        """Verarbeitet lokale Quellen: Ordner oder Einzeldatei."""
        src_path = Path(src.source_path)

        if not src_path.exists():
            raise FileNotFoundError(
                f"Quelle nicht gefunden: {src_path}")

        # ── Einzeldatei-Modus (audio_path gesetzt oder source_path ist Datei) ──
        if src_path.is_file():
            self.log_message.emit(
                f"\n📄 {src.name}: Einzeldatei")
            if src.move_to_destination:
                dst_dir = Path(src.destination_path)
                dst_dir.mkdir(parents=True, exist_ok=True)
                dst = dst_dir / src_path.name
                if not dst.exists():
                    self.log_message.emit(f"  → {src_path.name}")
                    shutil.move(str(src_path), str(dst))
                    # Audio-Datei auch verschieben
                    if src.audio_path:
                        audio_src = Path(src.audio_path)
                        if audio_src.exists():
                            audio_dst = dst_dir / audio_src.name
                            shutil.move(str(audio_src), str(audio_dst))
                            src.audio_path = str(audio_dst)
                    return [str(dst)]
                else:
                    self.log_message.emit(
                        f"  ⚠ Übersprungen (existiert): {src_path.name}")
                    return [str(dst)]
            return [str(src_path)]

        # ── Ordner-Modus ───────────────────────────────────────────────
        pattern = src.file_extensions or "*.mp4"
        files = sorted(src_path.glob(pattern))
        if not files:
            self.log_message.emit(
                f"  ⚠ Keine Dateien mit '{pattern}' in {src_path}")
            return []

        # Dateien in Zielordner verschieben
        if src.move_to_destination:
            dst_dir = Path(src.destination_path)
            dst_dir.mkdir(parents=True, exist_ok=True)

            self.log_message.emit(
                f"\n📁 {src.name}: {len(files)} Datei(en) verschieben …")

            result = []
            for f in files:
                if self._cancel.is_set():
                    break
                dst = dst_dir / f.name
                if dst.exists():
                    self.log_message.emit(
                        f"  ⚠ Übersprungen (existiert): {f.name}")
                    result.append(str(dst))
                    continue

                self.log_message.emit(f"  → {f.name}")
                try:
                    shutil.move(str(f), str(dst))
                    result.append(str(dst))
                except Exception as e:
                    self.log_message.emit(f"  ❌ Fehler: {e}")

            self.log_message.emit(
                f"  ✓ {len(result)} Datei(en) verschoben")
            return result

        # Dateien direkt im Quellordner verarbeiten (kein Verschieben)
        self.log_message.emit(
            f"\n📁 {src.name}: {len(files)} Datei(en) gefunden")
        return [str(f) for f in files]

    # ── ConvertJob erstellen ──────────────────────────────────

    def _create_convert_job(self, src: WorkflowSource,
                            file_path: str) -> ConvertJob:
        """Erstellt einen ConvertJob mit quellenspezifischen Einstellungen."""
        job = ConvertJob(
            source_path=Path(file_path),
            job_type="convert",
            youtube_title=src.youtube_title,
            youtube_playlist=src.youtube_playlist,
        )
        # Explizite Audio-Datei (Einzeldatei-Modus)
        if src.audio_path:
            job.audio_override = Path(src.audio_path)
        # Output-Pfad
        if src.output_filename:
            out_dir = Path(file_path).parent
            job.output_path = out_dir / f"{src.output_filename}.{src.output_format}"
        return job

    def _make_source_settings(self, src: WorkflowSource) -> AppSettings:
        """Erzeugt eine AppSettings-Kopie mit quellenspezifischen Werten.

        Die globalen Settings werden als Basis genommen und die pro-Quelle
        Werte darüber geschrieben. So bleiben Einstellungen wie Audio-Bitrate
        etc. erhalten, während Encoder/CRF/Preset pro Quelle
        unterschiedlich sein können.
        """
        from dataclasses import replace
        from .settings import (
            AppSettings, VideoSettings, AudioSettings, YouTubeSettings,
        )

        # Kopie der globalen Settings
        s = AppSettings(
            video=replace(self._settings.video),
            audio=replace(self._settings.audio),
            youtube=replace(self._settings.youtube),
            cameras=self._settings.cameras,
            last_directory=self._settings.last_directory,
        )

        # Pro-Quelle-Werte überschreiben
        s.video.encoder = src.encoder
        s.video.crf = src.crf
        s.video.preset = src.preset
        s.video.fps = src.fps
        s.video.output_format = src.output_format
        s.video.audio_sync = src.audio_sync

        # Audio
        s.audio.include_audio = src.merge_audio_video
        s.audio.amplify_audio = src.amplify_audio
        s.audio.amplify_db = src.amplify_db

        # YouTube
        s.youtube.create_youtube = src.create_youtube
        s.youtube.upload_to_youtube = src.upload_youtube

        return s
