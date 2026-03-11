"""Haupt-GUI des Video Managers (QMainWindow)."""

import argparse
import time
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, QThread, Slot
from PySide6.QtGui import QFont, QIcon, QKeySequence
from PySide6.QtWidgets import (
    QMainWindow, QToolBar, QTableWidget, QTableWidgetItem, QHeaderView,
    QTextEdit, QProgressBar, QLabel, QCheckBox,
    QFileDialog, QMessageBox, QSplitter, QAbstractItemView,
)

from .settings import AppSettings, SESSION_FILE
from .delegates import ProgressDelegate
from .dialogs import (
    VideoSettingsDialog, AudioSettingsDialog,
    YouTubeSettingsDialog,
    CameraSettingsDialog,
)
from .workflow import Workflow, WorkflowJob, FileEntry, WORKFLOW_DIR
from .workflow_executor import WorkflowExecutor
from .job_editor import JobEditorDialog


# ── Kompakt-Beschreibungen für die Übersichtstabelle ──────────────────────────

def _summarize_source(job: WorkflowJob) -> str:
    mode_icons = {"files": "🗃", "folder_scan": "📁", "pi_download": "📷"}
    icon = mode_icons.get(job.source_mode, "?")
    if job.source_mode == "files":
        n = len(job.files)
        return f"{icon} {n} Datei{'en' if n != 1 else ''}"
    if job.source_mode == "folder_scan":
        folder = Path(job.source_folder).name if job.source_folder else "–"
        return f"{icon} {folder}"
    if job.source_mode == "pi_download":
        return f"{icon} {job.device_name or '–'}"
    return "?"


def _summarize_pipeline(job: WorkflowJob) -> str:
    parts = []
    if job.source_mode == "pi_download":
        parts.append("Download")
    if job.convert_enabled:
        parts.append("Konvert.")
    if job.create_youtube_version:
        parts.append("YT-Version")
    if job.upload_youtube:
        parts.append("YT-Upload")
    return " → ".join(parts) if parts else "—"


class ConverterApp(QMainWindow):
    def __init__(self, cli_args: argparse.Namespace | None = None):
        super().__init__()
        self.setWindowTitle("Video Manager")
        self.resize(960, 640)
        self.setMinimumSize(720, 460)

        # Icon setzen (Fenster + Taskleiste)
        _icon_path = Path(__file__).resolve().parent.parent / "assets" / "icon.svg"
        if _icon_path.exists():
            self.setWindowIcon(QIcon(str(_icon_path)))

        self.settings = AppSettings.load()

        # --cameras-config: YAML importieren und Kameradaten überschreiben
        if cli_args and cli_args.cameras_config:
            self._apply_cameras_config(cli_args.cameras_config)

        self._workflow: Workflow = Workflow(jobs=[])
        self._wf_executor: Optional[WorkflowExecutor] = None
        self._wf_thread: Optional[QThread] = None
        self._wf_start_time: float = 0.0

        self._build_menu()
        self._build_toolbar()
        self._build_central()
        self._build_statusbar()

        # Session wiederherstellen (CLI-Flags überschreiben Einstellung)
        restore = self.settings.restore_session
        if cli_args and cli_args.restore_session:
            restore = True
        elif cli_args and cli_args.no_restore_session:
            restore = False
        if restore:
            self._restore_session()

        # --add: Dateien in die Jobliste laden
        if cli_args and cli_args.add:
            self._apply_add_files(cli_args.add)

        # --workflow: Workflow laden und automatisch starten
        self._cli_workflow: str | None = (
            cli_args.workflow if cli_args else None)

    def showEvent(self, event):  # noqa: N802
        """Startet CLI-Workflow beim ersten show (UI muss bereit sein)."""
        super().showEvent(event)
        if self._cli_workflow:
            wf_path = self._cli_workflow
            self._cli_workflow = None  # nur einmal ausführen
            from PySide6.QtCore import QTimer
            QTimer.singleShot(0, lambda: self._apply_workflow(wf_path))

    # ──────────────────────────────────────────────────────────
    #  CLI-Helfer
    # ──────────────────────────────────────────────────────────
    def _apply_cameras_config(self, yaml_path: str):
        """Importiert Kamera-Daten aus YAML und überschreibt gespeicherte."""
        from .downloader import import_from_yaml
        p = Path(yaml_path)
        if not p.exists():
            print(f"[WARN] --cameras-config: Datei nicht gefunden: {p}")
            return
        try:
            cam = import_from_yaml(str(p))
            self.settings.cameras = cam
            self.settings.save()
        except Exception as exc:
            print(f"[WARN] --cameras-config: Import fehlgeschlagen: {exc}")

    def _apply_add_files(self, paths: list[str]):
        """Fügt CLI-übergebene Dateien als Aufträge ein."""
        added = 0
        for raw in paths:
            p = Path(raw)
            if p.is_file():
                job = WorkflowJob(
                    source_mode="files",
                    files=[FileEntry(source_path=str(p))],
                )
                self._workflow.jobs.append(job)
                added += 1
            elif p.is_dir():
                for f in sorted(p.rglob("*")):
                    if f.is_file():
                        job = WorkflowJob(
                            source_mode="files",
                            files=[FileEntry(source_path=str(f))],
                        )
                        self._workflow.jobs.append(job)
                        added += 1
            else:
                print(f"[WARN] --add: Nicht gefunden: {p}")
        if added:
            self._refresh_table()
            self._update_count()
            self._append_log(
                f"CLI: {added} Datei(en) aus --add hinzugefügt")

    def _apply_workflow(self, wf_path: str):
        """Lädt und startet einen Workflow aus CLI-Argument."""
        p = Path(wf_path)
        if not p.exists():
            self._append_log(
                f"[FEHLER] --workflow: Datei nicht gefunden: {p}")
            return
        try:
            wf = Workflow.load(p)
        except Exception as exc:
            self._append_log(
                f"[FEHLER] --workflow: Laden fehlgeschlagen: {exc}")
            return
        self._workflow = wf
        self._refresh_table()
        self._update_count()
        self._append_log(f"CLI: Workflow geladen aus {p.name}")
        self._start_workflow()

    # ══════════════════════════════════════════════════════════
    #  Menü
    # ══════════════════════════════════════════════════════════
    def _build_menu(self):
        mb = self.menuBar()

        file_menu = mb.addMenu("&Datei")
        a = file_menu.addAction("Workflow laden …")
        a.setShortcut(QKeySequence("Ctrl+I"))
        a.triggered.connect(self._load_workflow)
        a = file_menu.addAction("Workflow speichern …")
        a.setShortcut(QKeySequence("Ctrl+E"))
        a.triggered.connect(self._save_workflow)
        file_menu.addSeparator()
        file_menu.addAction("Alle Aufträge entfernen", self._clear_jobs)
        file_menu.addSeparator()
        file_menu.addAction("Beenden", self.close)

        settings_menu = mb.addMenu("&Einstellungen")
        settings_menu.addAction("Video …", self._open_video_settings)
        settings_menu.addAction("Audio …", self._open_audio_settings)
        settings_menu.addAction("YouTube …", self._open_youtube_settings)
        settings_menu.addAction("Kameras …", self._open_camera_settings)
        settings_menu.addSeparator()
        settings_menu.addAction("Allgemein …", self._open_general_settings)

    # ══════════════════════════════════════════════════════════
    #  Toolbar
    # ══════════════════════════════════════════════════════════
    def _build_toolbar(self):
        tb = QToolBar("Aktionen")
        tb.setMovable(False)
        self.addToolBar(tb)

        tb.addAction("＋ Auftrag", self._add_job)
        tb.addAction("＋ Alle Kameras", self._add_all_cameras)
        tb.addSeparator()
        tb.addAction("Bearbeiten", self._edit_job)
        tb.addAction("Duplizieren", self._duplicate_job)
        tb.addAction("Entfernen", self._remove_selected)
        tb.addSeparator()

        self.act_start = tb.addAction("▶  Starten", self._start_workflow)
        self.act_cancel = tb.addAction("■  Abbrechen", self._cancel_workflow)
        self.act_cancel.setEnabled(False)
        tb.addSeparator()

        tb.addAction("Laden", self._load_workflow)
        tb.addAction("Speichern", self._save_workflow)
        tb.addSeparator()

        self._shutdown_cb = QCheckBox("Rechner herunterfahren")
        tb.addWidget(self._shutdown_cb)

    # ══════════════════════════════════════════════════════════
    #  Zentrales Widget
    # ══════════════════════════════════════════════════════════
    def _build_central(self):
        splitter = QSplitter(Qt.Vertical)

        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(
            ["#", "Name", "Quelle", "Pipeline", "Status"])
        hdr = self.table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(1, QHeaderView.Stretch)
        hdr.setSectionResizeMode(2, QHeaderView.Interactive)
        hdr.setSectionResizeMode(3, QHeaderView.Interactive)
        hdr.setSectionResizeMode(4, QHeaderView.Interactive)
        hdr.resizeSection(2, 180)
        hdr.resizeSection(3, 160)
        hdr.resizeSection(4, 200)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        self.table.doubleClicked.connect(self._edit_job)

        self._progress_delegate = ProgressDelegate(self.table)
        self.table.setItemDelegateForColumn(4, self._progress_delegate)

        splitter.addWidget(self.table)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Monospace", 9))
        splitter.addWidget(self.log_text)

        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)
        self.setCentralWidget(splitter)

    # ══════════════════════════════════════════════════════════
    #  Statusbar
    # ══════════════════════════════════════════════════════════
    def _build_statusbar(self):
        self.status_label = QLabel("Bereit")
        self.statusBar().addWidget(self.status_label, 2)

        self.progress = QProgressBar()
        self.progress.setTextVisible(True)
        self.progress.setMaximumHeight(18)
        self.progress.setValue(0)
        self.progress.setFormat("Gesamt: %v/%m")
        self.statusBar().addPermanentWidget(self.progress, 1)

    # ══════════════════════════════════════════════════════════
    #  Tabelle
    # ══════════════════════════════════════════════════════════
    def _refresh_table(self):
        jobs = self._workflow.jobs
        self.table.setRowCount(len(jobs))
        for i, job in enumerate(jobs):
            self.table.setItem(i, 0, QTableWidgetItem(str(i + 1)))
            self.table.setItem(i, 1, QTableWidgetItem(job.name or "–"))
            self.table.setItem(i, 2, QTableWidgetItem(_summarize_source(job)))
            self.table.setItem(i, 3, QTableWidgetItem(_summarize_pipeline(job)))
            # Status-Spalte: nur anlegen wenn noch kein Status (Ausführung läuft)
            if self.table.item(i, 4) is None:
                item = QTableWidgetItem("Wartend")
                item.setData(Qt.UserRole, 0)
                self.table.setItem(i, 4, item)

    def _set_row_status(self, row: int, status: str):
        """Schreibt Status-Text in Spalte 4 und setzt Farbe."""
        item = self.table.item(row, 4)
        if item is None:
            item = QTableWidgetItem()
            self.table.setItem(row, 4, item)
        item.setText(status)
        if status == "Fertig":
            item.setForeground(Qt.darkGreen)
        elif "Fehler" in status:
            item.setForeground(Qt.red)
        elif status == "Übersprungen":
            item.setForeground(Qt.gray)
        elif status in ("Läuft", "Herunterladen", "Transfer …",
                        "Konvertiere …", "YouTube-Upload …"):
            item.setForeground(Qt.blue)
        else:
            item.setForeground(Qt.black)
        self.table.viewport().update()

    def _set_row_progress(self, row: int, pct: int):
        """Setzt Fortschrittsbalken-Daten in Spalte 4."""
        item = self.table.item(row, 4)
        if item is None:
            item = QTableWidgetItem()
            self.table.setItem(row, 4, item)
        item.setData(Qt.UserRole, pct)
        self.table.viewport().update()

    def _reset_status_column(self):
        """Setzt alle Status-Zellen auf »Wartend«."""
        for i in range(self.table.rowCount()):
            item = self.table.item(i, 4)
            if item is None:
                item = QTableWidgetItem()
                self.table.setItem(i, 4, item)
            item.setText("Wartend")
            item.setData(Qt.UserRole, 0)
            item.setForeground(Qt.black)
        self.table.viewport().update()

    # ══════════════════════════════════════════════════════════
    #  Log
    # ══════════════════════════════════════════════════════════
    @Slot(str)
    def _append_log(self, msg: str):
        self.log_text.append(msg)

    # ══════════════════════════════════════════════════════════
    #  Aufträge verwalten
    # ══════════════════════════════════════════════════════════
    def _add_job(self):
        """Öffnet den Auftrags-Editor für einen neuen Job."""
        dlg = JobEditorDialog(self, self.settings, job=None)
        if dlg.exec():
            self._workflow.jobs.append(dlg.result_job)
            self._refresh_table()
            self._update_count()

    def _add_all_cameras(self):
        """Fügt je einen Pi-Download-Auftrag pro konfigurierter Kamera hinzu."""
        cam = self.settings.cameras
        if not cam.devices:
            QMessageBox.warning(
                self, "Keine Kameras",
                "Bitte zuerst unter Einstellungen → Kameras Geräte anlegen.")
            return
        existing_names = {
            j.device_name for j in self._workflow.jobs
            if j.source_mode == "pi_download"
        }
        added = 0
        for dev in cam.devices:
            if dev.name not in existing_names:
                job = WorkflowJob(
                    name=dev.name,
                    source_mode="pi_download",
                    device_name=dev.name,
                    convert_enabled=True,
                )
                self._workflow.jobs.append(job)
                added += 1
        if added:
            self._refresh_table()
            self._update_count()
            self._append_log(f"{added} Kamera-Auftrag/Aufträge hinzugefügt.")
        else:
            QMessageBox.information(
                self, "Hinweis",
                "Für alle Kameras sind bereits Aufträge vorhanden.")

    def _remove_selected(self):
        rows = sorted(
            {idx.row() for idx in self.table.selectedIndexes()},
            reverse=True)
        for r in rows:
            if 0 <= r < len(self._workflow.jobs):
                del self._workflow.jobs[r]
        self._refresh_table()
        self._update_count()

    def _clear_jobs(self):
        if self._workflow.jobs:
            if QMessageBox.question(
                    self, "Bestätigung", "Alle Aufträge entfernen?",
                    QMessageBox.Yes | QMessageBox.No
            ) == QMessageBox.Yes:
                self._workflow.jobs.clear()
                self._refresh_table()
                self.status_label.setText("Bereit")

    def _edit_job(self):
        rows = sorted(
            {idx.row() for idx in self.table.selectedIndexes()})
        if not rows:
            return
        idx = rows[0]
        if 0 <= idx < len(self._workflow.jobs):
            job = self._workflow.jobs[idx]
            dlg = JobEditorDialog(self, self.settings, job=job)
            if dlg.exec():
                self._refresh_table()

    def _duplicate_job(self):
        rows = sorted(
            {idx.row() for idx in self.table.selectedIndexes()})
        if not rows:
            return
        import copy
        for r in reversed(rows):
            if 0 <= r < len(self._workflow.jobs):
                copy_job = copy.deepcopy(self._workflow.jobs[r])
                copy_job.name = (copy_job.name or "") + " (Kopie)"
                self._workflow.jobs.insert(r + 1, copy_job)
        self._refresh_table()
        self._update_count()

    def _update_count(self):
        n = len(self._workflow.jobs)
        self.status_label.setText(
            f"{n} Auftrag/Aufträge" if n else "Bereit")

    # ══════════════════════════════════════════════════════════
    #  Einstellungs-Dialoge
    # ══════════════════════════════════════════════════════════
    def _open_camera_settings(self):
        dlg = CameraSettingsDialog(self, self.settings)
        if dlg.exec():
            self.settings.save()

    def _open_video_settings(self):
        dlg = VideoSettingsDialog(self, self.settings)
        dlg.exec()

    def _open_audio_settings(self):
        AudioSettingsDialog(self, self.settings).exec()

    def _open_youtube_settings(self):
        YouTubeSettingsDialog(self, self.settings).exec()

    def _open_general_settings(self):
        from .dialogs import GeneralSettingsDialog
        GeneralSettingsDialog(self, self.settings).exec()

    # ══════════════════════════════════════════════════════════
    #  Workflow laden / speichern
    # ══════════════════════════════════════════════════════════
    def _load_workflow(self):
        WORKFLOW_DIR.mkdir(parents=True, exist_ok=True)
        path, _ = QFileDialog.getOpenFileName(
            self, "Workflow laden",
            str(WORKFLOW_DIR),
            "JSON-Dateien (*.json);;Alle Dateien (*)")
        if path:
            try:
                wf = Workflow.load(Path(path))
                self._workflow = wf
                self._refresh_table()
                self._update_count()
                self._append_log(f"Workflow geladen: {path}")
            except Exception as exc:
                QMessageBox.critical(self, "Fehler beim Laden", str(exc))

    def _save_workflow(self):
        WORKFLOW_DIR.mkdir(parents=True, exist_ok=True)
        path, _ = QFileDialog.getSaveFileName(
            self, "Workflow speichern",
            str(WORKFLOW_DIR / "workflow.json"),
            "JSON-Dateien (*.json);;Alle Dateien (*)")
        if path:
            try:
                self._workflow.save(Path(path))
                self._append_log(f"Workflow gespeichert: {path}")
            except Exception as exc:
                QMessageBox.critical(self, "Fehler beim Speichern", str(exc))

    # ══════════════════════════════════════════════════════════
    #  Session speichern / wiederherstellen
    # ══════════════════════════════════════════════════════════
    def _save_session(self):
        """Speichert den aktuellen Workflow als session.json."""
        try:
            self._workflow.save(SESSION_FILE)
        except Exception:
            pass   # Stillschweigend ignorieren

    def _restore_session(self):
        """Lädt den letzten Workflow aus session.json."""
        if SESSION_FILE.exists():
            try:
                self._workflow = Workflow.load(SESSION_FILE)
                self._refresh_table()
                self._update_count()
                self._append_log(
                    f"Session wiederhergestellt: "
                    f"{len(self._workflow.jobs)} Auftrag/Aufträge")
            except Exception:
                pass

    # ══════════════════════════════════════════════════════════
    #  ▶ Workflow starten
    # ══════════════════════════════════════════════════════════
    def _start_workflow(self):
        enabled = [j for j in self._workflow.jobs if j.enabled]
        if not enabled:
            QMessageBox.information(
                self, "Hinweis", "Keine aktiven Aufträge vorhanden.")
            return

        if self._wf_thread and self._wf_thread.isRunning():
            return

        # Status-Spalte zurücksetzen
        self._reset_status_column()

        self._set_busy(True)
        self._append_log(
            f"\n{'═'*60}"
            f"\n  ▶ Workflow gestartet  ({len(enabled)} aktive Aufträge)"
            f"\n{'═'*60}")
        self.progress.setMaximum(len(enabled))
        self.progress.setValue(0)

        self._workflow.shutdown_after = self._shutdown_cb.isChecked()

        self._wf_thread = QThread(self)
        self._wf_executor = WorkflowExecutor(self._workflow, self.settings)
        self._wf_executor.moveToThread(self._wf_thread)

        self._wf_thread.started.connect(self._wf_executor.run)
        self._wf_executor.log_message.connect(self._append_log)
        self._wf_executor.job_status.connect(self._on_job_status)
        self._wf_executor.job_progress.connect(self._on_job_progress)
        self._wf_executor.file_progress.connect(self._on_dl_progress)
        self._wf_executor.overall_progress.connect(self._on_overall_progress)
        self._wf_executor.phase_changed.connect(self._on_phase_changed)
        self._wf_executor.finished.connect(self._on_workflow_done)

        self._wf_start_time = time.monotonic()
        self._wf_thread.start()

    def _cancel_workflow(self):
        if self._wf_executor:
            self._wf_executor.cancel()
        self._append_log("Abbruch angefordert …")

    # ── Executor-Slots ────────────────────────────────────────

    @Slot(int, str)
    def _on_job_status(self, orig_idx: int, status: str):
        if 0 <= orig_idx < self.table.rowCount():
            self._set_row_status(orig_idx, status)

    @Slot(int, int)
    def _on_job_progress(self, orig_idx: int, pct: int):
        if 0 <= orig_idx < self.table.rowCount():
            self._set_row_progress(orig_idx, pct)

    @Slot(str, str, float, float, float)
    def _on_dl_progress(self, device: str, filename: str,
                        transferred: float, total: float, speed_bps: float):
        if total > 0:
            pct = int(transferred / total * 100)
            info = f"⬇ {device}: {filename}  {pct}%"
            if speed_bps > 0:
                speed_mb = speed_bps / 1048576
                remaining = total - transferred
                eta_s = remaining / speed_bps
                if eta_s >= 3600:
                    eta_str = f"{int(eta_s // 3600)}h {int((eta_s % 3600) // 60)}min"
                elif eta_s >= 60:
                    eta_str = f"{int(eta_s // 60)}min {int(eta_s % 60)}s"
                else:
                    eta_str = f"{int(eta_s)}s"
                info += f"  –  {speed_mb:.1f} MB/s  ETA {eta_str}"
            self.status_label.setText(info)
        else:
            self.status_label.setText(f"⬇ {device}: {filename}")

    @Slot(int)
    def _on_phase_changed(self, phase: int):
        if phase == 1:
            self.status_label.setText("Phase 1 – Downloads …")
        elif phase == 2:
            self.status_label.setText("Phase 2 – Konvertierung …")

    @Slot(int, int)
    def _on_overall_progress(self, done: int, total: int):
        self.progress.setMaximum(total)
        self.progress.setValue(done)
        elapsed = time.monotonic() - self._wf_start_time
        self.status_label.setText(
            f"Schritt {done}/{total}  ({self._format_duration(elapsed)})")

    @Slot(int, int, int)
    def _on_workflow_done(self, ok: int, skip: int, fail: int):
        if self._wf_thread:
            self._wf_thread.quit()
            self._wf_thread.wait()
            self._wf_thread = None
            self._wf_executor = None

        elapsed = time.monotonic() - self._wf_start_time
        msg = (f"Fertig: {ok} OK, {skip} übersprungen, {fail} Fehler"
               f"  ({self._format_duration(elapsed)})")
        self._append_log(f"\n{msg}")
        self.status_label.setText(msg)
        self._set_busy(False)

        if self._workflow.shutdown_after and fail == 0:
            self._append_log("\n⏻ Rechner wird in 1 Minute heruntergefahren …")
            import subprocess
            subprocess.Popen(["shutdown", "+1"])
        elif self._workflow.shutdown_after and fail > 0:
            self._append_log("\n⚠ Herunterfahren übersprungen wegen Fehlern.")

    # ══════════════════════════════════════════════════════════
    #  Hilfsmethoden
    # ══════════════════════════════════════════════════════════
    @staticmethod
    def _format_duration(seconds: float) -> str:
        s = int(seconds)
        if s >= 3600:
            h = s // 3600
            m = (s % 3600) // 60
            return f"{h}h {m:02d}min"
        elif s >= 60:
            m = s // 60
            sec = s % 60
            return f"{m}min {sec:02d}s"
        else:
            return f"{s}s"

    def _set_busy(self, busy: bool):
        self.act_start.setEnabled(not busy)
        self.act_cancel.setEnabled(busy)

    # ══════════════════════════════════════════════════════════
    #  Beenden
    # ══════════════════════════════════════════════════════════
    def closeEvent(self, event):
        running = self._wf_thread and self._wf_thread.isRunning()
        if running:
            if QMessageBox.question(
                    self, "Verarbeitung läuft",
                    "Workflow läuft noch. Wirklich beenden?",
                    QMessageBox.Yes | QMessageBox.No
            ) != QMessageBox.Yes:
                event.ignore()
                return
            if self._wf_executor:
                self._wf_executor.cancel()
            if self._wf_thread:
                self._wf_thread.quit()
                if not self._wf_thread.wait(10_000):
                    self._wf_thread.terminate()
                    self._wf_thread.wait(2000)
        # Session immer speichern (für Restore beim nächsten Start)
        self._save_session()
        event.accept()
