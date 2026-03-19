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
phase_changed(str)           – z. B. „Phase 1 – Downloads …"
finished(int, int, int)      – (ok, skipped, failed)
"""

import shutil
import tempfile
import threading
from pathlib import Path

from PySide6.QtCore import QObject, Signal, Slot

from .settings import AppSettings
from .converter import ConvertJob, run_convert, run_youtube_convert, run_concat
from .downloader import download_device
from .youtube import get_youtube_service, upload_to_youtube, get_video_id_for_output
from .kaderblick import post_to_kaderblick as kaderblick_post
from .merge import generate_title_card
from .workflow import Workflow, WorkflowJob, FileEntry


class WorkflowExecutor(QObject):
    """Führt einen Workflow zweiphasig aus."""

    log_message      = Signal(str)
    job_status       = Signal(int, str)    # (original_job_idx, status_text)
    job_progress     = Signal(int, int)    # (original_job_idx, 0-100)
    overall_progress = Signal(int, int)    # (done, total)
    file_progress    = Signal(str, str, float, float, float)
    phase_changed    = Signal(str)
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
        self._transfer_fail: int = 0   # phase-1 failures

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
        self.phase_changed.emit("Phase 1 – Downloads …")
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
                self._transfer_fail += 1
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
            self.phase_changed.emit("Phase 2 – Konvertierung …")
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

        # Kaderblick-Sortindex vorberechnen:
        # Gruppiere alle Kaderblick-Dateien nach (effektiver) Spiel-ID,
        # sortiere alphabetisch nach Quelldateiname → liefert 1-basierten Index.
        kb_sort_index: dict[tuple[str, str], int] = {}
        all_items = to_convert + [(i, j, cv) for i, j, cv in upload_only]
        from collections import defaultdict as _defaultdict
        kb_by_game: dict[str, list[str]] = _defaultdict(list)
        for _, j, cv in all_items:
            if not (j.upload_kaderblick and j.upload_youtube):
                continue
            entry = self._find_file_entry(j, str(cv.source_path))
            gid = (
                (entry.kaderblick_game_id if entry and entry.kaderblick_game_id else "")
                or j.default_kaderblick_game_id
            )
            if gid:
                kb_by_game[gid].append(cv.source_path.name)
        for gid, names in kb_by_game.items():
            for pos, name in enumerate(sorted(set(names)), start=1):
                kb_sort_index[(gid, name)] = pos

        # Per-Job Gesamtfortschritt: (orig_idx → (done, total))
        totals: dict[int, list[int]] = {}
        for orig_idx, _, _ in to_convert:
            if orig_idx not in totals:
                totals[orig_idx] = [0, 0]
            totals[orig_idx][1] += 1

        # Merge-Gruppen vorab bestimmen: ALLE Mitglieder einer Merge-Gruppe
        # (auch das erste) bekommen ihren Upload übersprungen – der Upload
        # geschieht erst nach dem Concat auf das fertige Merged-Ergebnis.
        _merge_seen_groups: set[str] = set()
        _merge_skip_conv_idx: set[int] = set()   # alle Merge-Mitglieder (inkl. erstes)
        for _ci, (_, _j, _cv) in enumerate(to_convert):
            _entry = self._find_file_entry(_j, str(_cv.source_path))
            _gid = (getattr(_entry, 'merge_group_id', '') or "") if _entry else ""
            if _gid:
                _merge_skip_conv_idx.add(_ci)   # immer eintragen (auch erstes Mitglied)
                _merge_seen_groups.add(_gid)

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

                # ── Optionale Titelkarte vor dem Video ──────────────────
                # Bei Merge-Gruppen erst nach dem Concat einfügen, nicht pro Datei.
                is_in_merge_group = conv_pos in _merge_skip_conv_idx
                if (job.title_card_enabled and not is_in_merge_group
                        and cv_job.output_path and cv_job.output_path.exists()):
                    cv_job.output_path = self._prepend_title_card(
                        cv_job, job, per_settings)

                if remaining == 0:
                    self._set_job_status(orig_idx, "Fertig")
                    self.job_progress.emit(orig_idx, 100)
                else:
                    done = totals[orig_idx][0]
                    self._set_job_status(
                        orig_idx, f"Fertig {done}/{totals[orig_idx][1]}")
                # Nicht-erste Merge-Mitglieder: Upload nach dem Concat erledigt
                is_merge_follower = is_in_merge_group
                if job.upload_youtube and yt_service and not is_merge_follower:
                    done_u  = totals[orig_idx][0]
                    total_u = totals[orig_idx][1]
                    status_txt = (f"YouTube-Upload {done_u}/{total_u} …"
                                  if total_u > 1 else "YouTube-Upload …")
                    self._set_job_status(orig_idx, status_txt)
                    self.phase_changed.emit("YouTube-Upload …")
                    yt_ok = self._upload_to_youtube(cv_job, per_settings, yt_service, orig_idx)
                    if not yt_ok:
                        fail += 1
                        self._set_job_status(orig_idx, f"Fehler: YouTube-Upload fehlgeschlagen")
                    elif job.upload_kaderblick:
                        self._set_job_status(orig_idx, "Kaderblick …")
                        entry = self._find_file_entry(job, str(cv_job.source_path))
                        gid = (
                            (entry.kaderblick_game_id
                             if entry and entry.kaderblick_game_id else "")
                            or job.default_kaderblick_game_id
                        )
                        sort_idx = kb_sort_index.get((gid, cv_job.source_path.name), 1)
                        kb_ok = self._post_to_kaderblick(cv_job, job, sort_idx, per_settings)
                        if not kb_ok:
                            fail += 1
                        elif remaining == 0:
                            self._set_job_status(orig_idx, "Fertig")
            elif cv_job.status == "Übersprungen":
                skip += 1
                totals[orig_idx][0] += 1
            else:
                fail += 1
                self._set_job_status(orig_idx, f"Fehler: {cv_job.error_msg[:60]}")
                self.job_progress.emit(orig_idx, 0)

            self.overall_progress.emit(conv_pos + 1, len(to_convert))

        # ── Merge-Gruppen zusammenführen ────────────────────
        # Nach der Konvertierung: Dateien gleicher merge_group_id per ffmpeg concat verbinden.
        # Die erste Datei der Gruppe bekommt den Ausgabepfad der Merged-Datei,
        # alle Folgedateien werden beim Upload übersprungen.
        _merged_out: dict[str, Path] = {}     # merge_group_id → merged output path
        _skip_upload: set[int] = set()        # Index in to_convert → überspringen

        for conv_idx, (orig_idx, job, cv_job) in enumerate(to_convert):
            if not cv_job.output_path or not cv_job.output_path.exists():
                continue
            entry = self._find_file_entry(job, str(cv_job.source_path))
            gid = (getattr(entry, 'merge_group_id', '') or "") if entry else ""
            if not gid:
                continue

            if gid not in _merged_out:
                # Erste Datei der Gruppe – bestimmt Ausgabepfad
                merged_path = cv_job.output_path.with_stem(
                    cv_job.output_path.stem + "_merged")
                _merged_out[gid] = merged_path
                _merged_first_idx = conv_idx
            else:
                # Folgedatei – wird nach dem Merge gelöscht / übersprungen (Upload)
                _skip_upload.add(conv_idx)

        # Eigentliche Concat-Ausführung pro Gruppe
        from collections import defaultdict as _ddict2
        group_items: dict[str, list] = _ddict2(list)
        for conv_idx, (orig_idx, job, cv_job) in enumerate(to_convert):
            entry = self._find_file_entry(job, str(cv_job.source_path))
            gid = (getattr(entry, 'merge_group_id', '') or "") if entry else ""
            if gid:
                group_items[gid].append((conv_idx, orig_idx, job, cv_job))

        if group_items:
            self.phase_changed.emit("Zusammenführen …")
        for gid, group in group_items.items():
            source_paths: list[Path] = []
            for _, _, _, cv in group:
                if cv.output_path and cv.output_path.exists():
                    source_paths.append(cv.output_path)

            if len(source_paths) < 2:
                continue  # Nur eine Datei – kein Merge nötig
            merged_path = _merged_out[gid]
            first_orig_idx = group[0][1]
            first_job      = group[0][2]
            first_cv       = group[0][3]
            per_settings   = self._build_job_settings(first_job)
            self._set_job_status(first_orig_idx, "Zusammenführen …")

            concat_ok = run_concat(
                source_paths,
                merged_path,
                cancel_flag=self._cancel,
                log_callback=self.log_message.emit,
            )
            if concat_ok:
                # Einzeldateien (Originale) nach dem Merge löschen
                for src in source_paths:
                    try:
                        src.unlink()
                    except OSError:
                        pass

                # Ersten cv_job auf Merged-Pfad umbiegen
                first_cv.output_path = merged_path

                # Optionale Titelkarte vor das Merge-Ergebnis setzen
                if first_job.title_card_enabled and merged_path.exists():
                    self._set_job_status(first_orig_idx, "Titelkarte …")
                    first_cv.output_path = self._prepend_title_card(
                        first_cv, first_job, per_settings)

                # Optionale YouTube-Version aus dem Merge-Ergebnis erstellen
                if first_job.create_youtube_version and merged_path.exists():
                    self._set_job_status(first_orig_idx, "YT-Version erstellen …")
                    ok_yt = run_youtube_convert(
                        first_cv, per_settings,
                        cancel_flag=self._cancel,
                        log_callback=self.log_message.emit,
                    )
                    if not ok_yt:
                        self.log_message.emit(
                            "⚠ YouTube-Version konnte nicht erstellt werden "
                            "– Original wird hochgeladen.")

                # Upload des zusammengeführten Videos
                if first_job.upload_youtube and yt_service:
                    self._set_job_status(first_orig_idx, "YouTube-Upload …")
                    self.phase_changed.emit("YouTube-Upload …")
                    yt_ok = self._upload_to_youtube(
                        first_cv, per_settings, yt_service, first_orig_idx)
                    if not yt_ok:
                        fail += 1
                        self._set_job_status(
                            first_orig_idx, "Fehler: YouTube-Upload fehlgeschlagen")
                    elif first_job.upload_kaderblick:
                        self._set_job_status(first_orig_idx, "Kaderblick …")
                        kb_entry = self._find_file_entry(
                            first_job, str(first_cv.source_path))
                        kb_gid = (
                            (kb_entry.kaderblick_game_id
                             if kb_entry and kb_entry.kaderblick_game_id else "")
                            or first_job.default_kaderblick_game_id
                        )
                        kb_sort = kb_sort_index.get(
                            (kb_gid, first_cv.source_path.name), 1)
                        kb_ok = self._post_to_kaderblick(
                            first_cv, first_job, kb_sort, per_settings)
                        if not kb_ok:
                            fail += 1
                        else:
                            self._set_job_status(first_orig_idx, "Fertig")
                    else:
                        self._set_job_status(first_orig_idx, "Fertig")
                    self.job_progress.emit(first_orig_idx, 100)
            else:
                fail += 1
                self.log_message.emit(f"❌ Merge fehlgeschlagen für Gruppe {gid}")

        # Nur-Upload (convert_enabled=False)
        if upload_only:
            self.phase_changed.emit("YouTube-Upload …")
            # Per-Job Zähler aufbauen (analog zu totals bei Konvertierung)
            upload_totals: dict[int, list[int]] = {}
            for oi, _, _ in upload_only:
                if oi not in upload_totals:
                    upload_totals[oi] = [0, 0]
                upload_totals[oi][1] += 1

            total_uploads = len(upload_only)
            self.log_message.emit(
                f"\n{'═'*60}"
                f"\n  ☁  YouTube-Upload  ({total_uploads} Datei(en))"
                f"\n{'═'*60}")

            for upload_pos, (orig_idx, job, cv_job) in enumerate(upload_only):
                if self._cancel.is_set():
                    break
                per_settings = self._build_job_settings(job)
                done_u  = upload_totals[orig_idx][0]
                total_u = upload_totals[orig_idx][1]

                self.log_message.emit(
                    f"\n═══ [{upload_pos+1}/{total_uploads}] {job.name}: "
                    f"{cv_job.source_path.name} ═══")

                # Ggf. zuerst YouTube-optimierte Version erstellen
                if (job.create_youtube_version
                        and cv_job.output_path
                        and cv_job.output_path.exists()):
                    self._set_job_status(orig_idx, "YT-Version erstellen …")
                    ok_yt = run_youtube_convert(
                        cv_job, per_settings,
                        cancel_flag=self._cancel,
                        log_callback=self.log_message.emit,
                    )
                    if not ok_yt:
                        self.log_message.emit(
                            "⚠ YouTube-Version konnte nicht erstellt werden "
                            "– Original wird hochgeladen.")
                if self._cancel.is_set():
                    break

                if yt_service:
                    status_txt = (
                        f"YouTube-Upload {done_u+1}/{total_u} …"
                        if total_u > 1 else "YouTube-Upload …")
                    self._set_job_status(orig_idx, status_txt)
                    yt_ok = self._upload_to_youtube(cv_job, per_settings, yt_service, orig_idx)
                    if not yt_ok:
                        fail += 1
                        self._set_job_status(orig_idx, "Fehler: YouTube-Upload fehlgeschlagen")
                    elif job.upload_kaderblick:
                        entry = self._find_file_entry(job, str(cv_job.source_path))
                        gid = (
                            (entry.kaderblick_game_id
                             if entry and entry.kaderblick_game_id else "")
                            or job.default_kaderblick_game_id
                        )
                        sort_idx = kb_sort_index.get((gid, cv_job.source_path.name), 1)
                        kb_ok = self._post_to_kaderblick(cv_job, job, sort_idx, per_settings)
                        if not kb_ok:
                            fail += 1

                upload_totals[orig_idx][0] += 1
                done_u = upload_totals[orig_idx][0]

                # Anteiliger Fortschrittsbalken für diesen Job
                pct = int(done_u / total_u * 100) if total_u else 100
                self.job_progress.emit(orig_idx, pct)

                # Job-Status: "Fertig" erst wenn alle Dateien dieses Jobs erledigt
                if done_u >= total_u:
                    self._set_job_status(orig_idx, "Fertig")
                else:
                    self._set_job_status(orig_idx, f"Fertig {done_u}/{total_u}")

                # Gesamtfortschritt in Dateien (nicht Jobs)
                self.overall_progress.emit(upload_pos + 1, total_uploads)

        total_fail = fail + self._transfer_fail
        icon = "✅" if total_fail == 0 else "❌"
        self.log_message.emit(
            f"\n{icon} Fertig: {ok} OK, {skip} übersprungen, {total_fail} Fehler")
        self.finished.emit(ok, skip, total_fail)

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

        # Wenn der Benutzer im Job-Editor Kamera-Dateien vorab ausgewählt hat,
        # nur diese herunterladen (Basis-Namen ohne Extension)
        selective: set | None = None
        if job.files:
            selective = {Path(e.source_path).stem for e in job.files}

        results = download_device(
            device=dev,
            config=self._settings.cameras,
            log_cb=self.log_message.emit,
            progress_cb=_on_progress,
            cancel_flag=self._cancel,
            destination_override=job.download_destination,
            delete_after_download=job.delete_after_download,
            selective_bases=selective,
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
        from .youtube_title_editor import build_video_tags, MatchData, SegmentData
        entry = self._find_file_entry(job, file_path)
        yt_title    = (entry.youtube_title    if entry and entry.youtube_title
                       else job.default_youtube_title or Path(file_path).stem)
        yt_playlist = (entry.youtube_playlist if entry and entry.youtube_playlist
                       else job.default_youtube_playlist)
        yt_desc     = (entry.youtube_description if entry and entry.youtube_description
                       else "")
        # Tags aus Titel ableiten (einfacher Ansatz: Wörter aus Titel)
        yt_tags: list[str] = []
        if yt_title and " | " in yt_title:
            parts = [p.strip() for p in yt_title.split(" | ")]
            yt_tags = [t for t in parts if t and len(t) < 50]
        yt_tags = list(dict.fromkeys(["Fußball", "Sport"] + yt_tags))
        cv_job = ConvertJob(
            source_path=Path(file_path),
            job_type="convert",
            youtube_title=yt_title,
            youtube_description=yt_desc,
            youtube_playlist=yt_playlist,
            youtube_tags=yt_tags,
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
        s.kaderblick = self._settings.kaderblick   # global, nicht per-job überschrieben
        return s

    # ── YouTube-Upload ────────────────────────────────────────

    def _upload_to_youtube(self, cv_job: ConvertJob,
                           settings: AppSettings,
                           yt_service, orig_idx: int) -> bool:
        return upload_to_youtube(cv_job, settings, yt_service,
                                 log_callback=self.log_message.emit,
                                 cancel_flag=self._cancel,
                                 progress_callback=lambda pct:
                                     self.job_progress.emit(orig_idx, pct))

    # ── Titelkarte vor Video einfügen ────────────────────────

    def _prepend_title_card(self, cv_job: ConvertJob, job: WorkflowJob,
                            per_settings: AppSettings) -> Path:
        """Generiert eine Titelkarte und stellt sie dem konvertierten Video voran.

        Returns:
            Pfad zur fertigen Datei (mit vorangestellter Titelkarte).
            Bei Fehler wird der ursprüngliche Pfad zurückgegeben.
        """
        from .ffmpeg_runner import get_resolution
        video_path = cv_job.output_path
        if not video_path:
            raise ValueError("cv_job.output_path ist None")

        entry = self._find_file_entry(job, str(cv_job.source_path))

        title = ""
        if job.title_card_home_team and job.title_card_away_team:
            title = f"{job.title_card_home_team} vs {job.title_card_away_team}"
        elif job.title_card_home_team or job.title_card_away_team:
            title = job.title_card_home_team or job.title_card_away_team

        subtitle = (
            (entry.title_card_subtitle if entry and entry.title_card_subtitle else "")
            or video_path.stem
        )

        res = get_resolution(video_path)
        w, h = res if res else (1920, 1080)
        fps  = per_settings.video.fps or 25

        tmpdir   = Path(tempfile.mkdtemp(prefix="intro_"))
        card_path = tmpdir / "intro.mp4"

        self.log_message.emit(f"  Erstelle Titelkarte: \"{subtitle}\"")
        ok = generate_title_card(
            card_path,
            subtitle=subtitle,
            duration=job.title_card_duration,
            width=w, height=h, fps=fps,
            title=title,
            logo_path=job.title_card_logo_path,
            bg_color=job.title_card_bg_color,
            fg_color=job.title_card_fg_color,
            cancel_flag=self._cancel,
            log_callback=self.log_message.emit,
            work_dir=tmpdir,
        )
        if not ok or self._cancel.is_set():
            self.log_message.emit("  ⚠ Titelkarte konnte nicht erstellt werden")
            try:
                import shutil as _sh
                _sh.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass
            return video_path

        # Concat: [intro, video] → mit_intro.mp4
        with_intro_path = video_path.with_stem(video_path.stem + "_tmp_intro")
        concat_ok = run_concat(
            [card_path, video_path],
            with_intro_path,
            cancel_flag=self._cancel,
            log_callback=self.log_message.emit,
        )

        # Aufräumen
        try:
            import shutil as _sh
            _sh.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass

        if not concat_ok or self._cancel.is_set():
            self.log_message.emit("  ⚠ Zusammenführen mit Titelkarte fehlgeschlagen")
            if with_intro_path.exists():
                with_intro_path.unlink(missing_ok=True)
            return video_path

        # Original ersetzen
        try:
            video_path.unlink()
        except OSError:
            pass
        with_intro_path.rename(video_path)
        return video_path

    # ── Kaderblick-Upload ───────────────────────────────────────────

    def _post_to_kaderblick(self, cv_job: ConvertJob, job: WorkflowJob,
                            sort_index: int, per_settings: AppSettings) -> bool:
        """Trägt ein Video auf Kaderblick ein (nach erfolgreichem YouTube-Upload).
        Gibt True zurück wenn erfolgreich oder übersprungen (Duplikat), False bei Fehler.
        """
        output = cv_job.output_path
        if not output:
            return True   # nichts zu tun – kein Fehler
        video_id = get_video_id_for_output(output)
        if not video_id:
            self.log_message.emit(
                "  ⚠ Kaderblick: YouTube-Video-ID nicht in Registry – übersprungen")
            return True   # nicht eingetragen, aber kein harter Fehler

        youtube_url = f"https://www.youtube.com/watch?v={video_id}"
        entry = self._find_file_entry(job, str(cv_job.source_path))
        game_id = (
            (entry.kaderblick_game_id if entry and entry.kaderblick_game_id else None)
            or job.default_kaderblick_game_id
        )
        game_start = entry.kaderblick_game_start if entry else 0
        video_type_id = (
            (entry.kaderblick_video_type_id
             if entry and entry.kaderblick_video_type_id else 0)
            or job.default_kaderblick_video_type_id
        )
        camera_id = (
            (entry.kaderblick_camera_id
             if entry and entry.kaderblick_camera_id else 0)
            or job.default_kaderblick_camera_id
        )
        return kaderblick_post(
            settings=per_settings,
            game_id=game_id,
            video_name=cv_job.youtube_title or cv_job.source_path.stem,
            youtube_video_id=video_id,
            youtube_url=youtube_url,
            file_path=cv_job.source_path,
            output_file_path=output,
            game_start_seconds=game_start,
            video_type_id=video_type_id,
            camera_id=camera_id,
            sort_index=sort_index,
            log_callback=self.log_message.emit,
        )

    # ── Status-Helfer ─────────────────────────────────────────

    def _set_job_status(self, orig_idx: int, status: str) -> None:
        """Emit job_status and keep backward-compat alias in sync."""
        self.job_status.emit(orig_idx, status)
        self.source_status.emit(orig_idx, status)
