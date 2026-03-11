"""Workflow-Executor: Führt einen Workflow aus.

Wandelt WorkflowJob-Objekte in ausführbare Schritte um:
  1. Transfer-Phase  (Download / Kopieren / Verschieben)
  2. Verarbeitungs-Phase  (Konvertierung, YouTube-Upload)

Signale
-------
log_message(str)
job_status(int, str)         – (original_job_idx, status_text)  → aktualisiert Tabellenzeile
job_progress(int, int)       – (original_job_idx, percent 0-100) → Fortschrittsbalken pro Zeile
overall_progress(int, int)   – (done, total)
file_progress(str, str, float, float, float)  – Download-Fortschritt
phase_changed(int)           – 1=Transfer  2=Konvertierung
finished(int, int, int)      – (ok, skipped, failed)
"""

import shutil
import threading
from pathlib import Path

from PySide6.QtCore import QObject, Signal, Slot

from .settings import AppSettings
from .converter import ConvertJob, run_convert
from .downloader import download_device
from .youtube import get_youtube_service, upload_to_youtube
from .workflow import Workflow, WorkflowJob, FileEntry


class WorkflowExecutor(QObject):
    """Führt einen Workflow zweiphasig aus."""

    log_message      = Signal(str)
    job_status       = Signal(int, str)    # (original_job_idx, status_text)
    job_progress     = Signal(int, int)    # (original_job_idx, 0-100)
    overall_progress = Signal(int, int)    # (done, total)
    file_progress    = Signal(str, str, float, float, float)
    phase_changed    = Signal(int)
    finished         = Signal(int, int, int)

    # Rückwärtskompatibilität
    source_status    = Signal(int, str)
    source_progress  = Signal(int, int)
    convert_progress = Signal(int, int)

    def __init__(self, workflow: Workflow, settings: AppSettings):
        super().__init__()
        self._workflow = workflow
        self._settings = settings
        self._cancel   = threading.Event()
        # (original_job_idx, job, cv_job)
        self._convert_items: list[tuple[int, WorkflowJob, ConvertJob]] = []

    def cancel(self) -> None:
        self._cancel.set()

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    @Slot()
    def run(self) -> None:
        # Nur aktivierte Jobs; merke originale Indizes in workflow.jobs
        active = [(idx, job)
                  for idx, job in enumerate(self._workflow.jobs)
                  if job.enabled]
        if not active:
            self.finished.emit(0, 0, 0)
            return

        # ── Phase 1: Transfer ─────────────────────────────────
        self.phase_changed.emit(1)
        self.log_message.emit(
            f"\n{'═'*60}"
            f"\n  📥 Phase 1: Transfer  ({len(active)} Auftrag/Aufträge)"
            f"\n{'═'*60}")

        for active_pos, (orig_idx, job) in enumerate(active):
            if self._cancel.is_set():
                break
            self._set_job_status(orig_idx, "Transfer …")
            try:
                files = self._run_transfer(orig_idx, job)
            except Exception as exc:
                self._set_job_status(orig_idx, f"Fehler: {exc}")
                job.error_msg = str(exc)
                self.log_message.emit(f"❌ {job.name}: {exc}")
                continue

            if self._cancel.is_set():
                break

            self._set_job_status(orig_idx, "Transfer OK")
            self.overall_progress.emit(active_pos + 1, len(active))

            if job.convert_enabled:
                for fpath in files:
                    cv_job = self._build_convert_job(job, fpath)
                    self._convert_items.append((orig_idx, job, cv_job))
            else:
                # Keine Konvertierung – Originaldatei vormerken (YT-Upload möglich)
                for fpath in files:
                    cv_job = ConvertJob(
                        source_path=Path(fpath),
                        job_type="convert",
                        youtube_title=self._resolve_youtube_title(job, fpath),
                        youtube_playlist=job.default_youtube_playlist,
                    )
                    cv_job.status   = "Fertig"
                    cv_job.output_path = Path(fpath)
                    self._convert_items.append((orig_idx, job, cv_job))

        if self._cancel.is_set():
            self.log_message.emit("Phase 1 abgebrochen.")
            self.finished.emit(0, 0, 0)
            return

        # ── Phase 2: Konvertierung ────────────────────────────
        to_convert    = [(i, j, cv) for i, j, cv in self._convert_items if j.convert_enabled]
        upload_only   = [(i, j, cv) for i, j, cv in self._convert_items
                         if not j.convert_enabled and j.upload_youtube]

        if not to_convert and not upload_only:
            self.log_message.emit("\nKeine weiteren Verarbeitungsschritte.")
            self.finished.emit(0, 0, 0)
            return

        if to_convert:
            self.phase_changed.emit(2)
            self.log_message.emit(
                f"\n{'═'*60}"
                f"\n  🔄 Phase 2: Konvertierung  ({len(to_convert)} Datei(en))"
                f"\n{'═'*60}")

        # YouTube-Service
        needs_youtube = any(j.upload_youtube for _, j, _ in self._convert_items)
        yt_service = None
        if needs_youtube:
            self.log_message.emit("YouTube-Anmeldung …")
            yt_service = get_youtube_service(log_callback=self.log_message.emit)
            if not yt_service:
                self.log_message.emit(
                    "⚠ YouTube-Upload deaktiviert (Anmeldung fehlgeschlagen)")

        # Per-Job Gesamtfortschritt: (orig_idx → (done, total))
        totals: dict[int, list[int]] = {}
        for orig_idx, _, _ in to_convert:
            if orig_idx not in totals:
                totals[orig_idx] = [0, 0]
            totals[orig_idx][1] += 1

        ok = skip = fail = 0

        for conv_pos, (orig_idx, job, cv_job) in enumerate(to_convert):
            if self._cancel.is_set():
                self.log_message.emit("Konvertierung abgebrochen.")
                break

            self.log_message.emit(
                f"\n═══ [{conv_pos+1}/{len(to_convert)}] {job.name}: "
                f"{cv_job.source_path.name} ═══")

            per_settings = self._build_job_settings(job)
            cv_job.status       = "Läuft"
            cv_job.progress_pct = 0
            self._set_job_status(orig_idx, "Konvertiere …")

            done_count = totals[orig_idx][0]
            total_count = totals[orig_idx][1]

            def _progress(pct, _oi=orig_idx, _done=done_count, _tot=total_count, _cv=cv_job):
                _cv.progress_pct = pct
                # Gesamtfortschritt für diesen Job: (abgeschlossene + laufende) / gesamt
                composite = int((_done + pct / 100.0) / _tot * 100) if _tot else pct
                self.job_progress.emit(_oi, composite)
                self.convert_progress.emit(_done, pct)   # Kompatibilität

            success = run_convert(
                cv_job, per_settings,
                cancel_flag=self._cancel,
                log_callback=self.log_message.emit,
                progress_callback=_progress)

            if self._cancel.is_set():
                break

            if success and cv_job.status == "Fertig":
                ok += 1
                totals[orig_idx][0] += 1
                remaining = totals[orig_idx][1] - totals[orig_idx][0]
                if remaining == 0:
                    self._set_job_status(orig_idx, "Fertig")
                    self.job_progress.emit(orig_idx, 100)
                else:
                    done = totals[orig_idx][0]
                    self._set_job_status(
                        orig_idx, f"Fertig {done}/{totals[orig_idx][1]}")
                if job.upload_youtube and yt_service:
                    self._upload_to_youtube(cv_job, per_settings, yt_service)
            elif cv_job.status == "Übersprungen":
                skip += 1
                totals[orig_idx][0] += 1
            else:
                fail += 1
                self._set_job_status(orig_idx, f"Fehler: {cv_job.error_msg[:60]}")
                self.job_progress.emit(orig_idx, 0)

            self.overall_progress.emit(conv_pos + 1, len(to_convert))

        # Nur-Upload (convert_enabled=False)
        for orig_idx, job, cv_job in upload_only:
            if self._cancel.is_set():
                break
            if yt_service:
                self._set_job_status(orig_idx, "YouTube-Upload …")
                self._upload_to_youtube(
                    cv_job, self._build_job_settings(job), yt_service)
                self._set_job_status(orig_idx, "Fertig")
                self.job_progress.emit(orig_idx, 100)

        self.log_message.emit(
            f"\n✅ Fertig: {ok} OK, {skip} übersprungen, {fail} Fehler")
        self.finished.emit(ok, skip, fail)

    # ── Transfer ─────────────────────────────────────────────

    def _run_transfer(self, orig_idx: int, job: WorkflowJob) -> list[str]:
        if job.source_mode == "files":
            return self._handle_direct_files(job)
        if job.source_mode == "pi_download":
            return self._download_from_pi(orig_idx, job)
        if job.source_mode == "folder_scan":
            return self._scan_folder(job)
        raise ValueError(f"Unbekannter Quellmodus: {job.source_mode!r}")

    def _handle_direct_files(self, job: WorkflowJob) -> list[str]:
        paths = []
        for entry in job.files:
            p = Path(entry.source_path)
            if p.exists():
                paths.append(str(p))
            else:
                self.log_message.emit(f"  ⚠ Datei nicht gefunden: {p}")
        self.log_message.emit(f"\n🗃 {job.name}: {len(paths)} Datei(en) bereit")
        return paths

    def _download_from_pi(self, orig_idx: int, job: WorkflowJob) -> list[str]:
        dev = next((d for d in self._settings.cameras.devices
                    if d.name == job.device_name), None)
        if not dev:
            raise RuntimeError(
                f"Gerät '{job.device_name}' nicht in den Einstellungen")

        self.log_message.emit(f"\n⬇ {job.name}: Download von {dev.ip} …")

        import time
        state: dict = {"speed": 0.0, "last_t": 0.0, "last_b": 0, "file": ""}

        def _on_progress(device_name, filename, transferred, total):
            now = time.monotonic()
            if filename != state["file"]:
                state.update(speed=0.0, last_t=now, last_b=transferred, file=filename)
            if transferred < total and (now - state["last_t"]) < 0.25:
                return
            dt = now - state["last_t"]
            if dt >= 0.5 and state["last_t"] > 0:
                raw = (transferred - state["last_b"]) / dt
                state["speed"] = raw if state["speed"] == 0 else 0.3*raw + 0.7*state["speed"]
                state["last_b"] = transferred
                state["last_t"] = now
            elif state["last_t"] == 0:
                state.update(last_b=transferred, last_t=now)
            if total > 0:
                pct = int(transferred / total * 100)
                self.job_progress.emit(orig_idx, pct)
            self.file_progress.emit(device_name, filename,
                                    float(transferred), float(total),
                                    state["speed"])

        results = download_device(
            device=dev,
            config=self._settings.cameras,
            log_cb=self.log_message.emit,
            progress_cb=_on_progress,
            cancel_flag=self._cancel,
            destination_override=job.download_destination,
            delete_after_download=job.delete_after_download,
        )
        return [r[2] for r in results]

    def _scan_folder(self, job: WorkflowJob) -> list[str]:
        src_dir = Path(job.source_folder)
        if not src_dir.exists():
            raise FileNotFoundError(f"Quellordner nicht gefunden: {src_dir}")

        pattern = job.file_pattern or "*.mp4"
        files   = sorted(src_dir.glob(pattern))
        if not files:
            self.log_message.emit(
                f"  ⚠ Keine Dateien mit Muster '{pattern}' in {src_dir}")
            return []

        dst_dir = Path(job.copy_destination) if job.copy_destination else None
        if dst_dir:
            dst_dir.mkdir(parents=True, exist_ok=True)
            verb = "verschieben" if job.move_files else "kopieren"
            self.log_message.emit(
                f"\n📁 {job.name}: {len(files)} Datei(en) {verb} …")
            result = []
            for f in files:
                if self._cancel.is_set():
                    break
                dst = dst_dir / f.name
                if dst.exists():
                    self.log_message.emit(f"  ⚠ Übersprungen (existiert): {f.name}")
                    result.append(str(dst))
                    continue
                self.log_message.emit(f"  → {f.name}")
                try:
                    if job.move_files:
                        shutil.move(str(f), str(dst))
                    else:
                        shutil.copy2(str(f), str(dst))
                    result.append(str(dst))
                except Exception as exc:
                    self.log_message.emit(f"  ❌ {f.name}: {exc}")
            return result

        self.log_message.emit(f"\n📁 {job.name}: {len(files)} Datei(en) gefunden")
        return [str(f) for f in files]

    # ── ConvertJob erstellen ──────────────────────────────────

    def _build_convert_job(self, job: WorkflowJob, file_path: str) -> ConvertJob:
        entry = self._find_file_entry(job, file_path)
        yt_title    = (entry.youtube_title    if entry and entry.youtube_title
                       else job.default_youtube_title or Path(file_path).stem)
        yt_playlist = (entry.youtube_playlist if entry and entry.youtube_playlist
                       else job.default_youtube_playlist)
        cv_job = ConvertJob(
            source_path=Path(file_path),
            job_type="convert",
            youtube_title=yt_title,
            youtube_playlist=yt_playlist,
        )
        if entry and entry.output_filename:
            out_dir = Path(file_path).parent
            cv_job.output_path = out_dir / f"{entry.output_filename}.{job.output_format}"
        return cv_job

    @staticmethod
    def _find_file_entry(job: WorkflowJob, file_path: str) -> FileEntry | None:
        for entry in job.files:
            if entry.source_path == file_path:
                return entry
        return None

    @staticmethod
    def _resolve_youtube_title(job: WorkflowJob, file_path: str) -> str:
        entry = WorkflowExecutor._find_file_entry(job, file_path)
        if entry and entry.youtube_title:
            return entry.youtube_title
        return job.default_youtube_title or Path(file_path).stem

    # ── Settings pro Job ─────────────────────────────────────

    def _build_job_settings(self, job: WorkflowJob) -> AppSettings:
        from dataclasses import replace
        s = AppSettings(
            video=replace(self._settings.video),
            audio=replace(self._settings.audio),
            youtube=replace(self._settings.youtube),
            cameras=self._settings.cameras,
            last_directory=self._settings.last_directory,
        )
        s.video.encoder       = job.encoder
        s.video.crf           = job.crf
        s.video.preset        = job.preset
        s.video.fps           = job.fps
        s.video.output_format = job.output_format
        s.video.audio_sync    = job.audio_sync
        s.audio.include_audio = job.merge_audio
        s.audio.amplify_audio = job.amplify_audio
        s.audio.amplify_db    = job.amplify_db
        s.youtube.create_youtube     = job.create_youtube_version
        s.youtube.upload_to_youtube  = job.upload_youtube
        return s

    # ── YouTube-Upload ────────────────────────────────────────

    def _upload_to_youtube(self, cv_job: ConvertJob,
                           settings: AppSettings, yt_service) -> None:
        upload_to_youtube(cv_job, settings, yt_service,
                          log_callback=self.log_message.emit)

    # ── Status-Helfer ─────────────────────────────────────────

    def _set_job_status(self, orig_idx: int, status: str) -> None:
        """Emit job_status and keep backward-compat alias in sync."""
        self.job_status.emit(orig_idx, status)
        self.source_status.emit(orig_idx, status)
