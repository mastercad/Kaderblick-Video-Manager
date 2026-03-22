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
    KaderblickSettingsDialog,
    CameraSettingsDialog,
)
from .workflow import Workflow, WorkflowJob, FileEntry, WORKFLOW_DIR
from .workflow_executor import WorkflowExecutor
from .job_editor import JobEditorDialog


_ROLE_STEP_PROGRESS = int(Qt.ItemDataRole.UserRole)
_ROLE_JOB_PROGRESS = _ROLE_STEP_PROGRESS + 1


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
    if any(f.merge_group_id for f in job.files):
        parts.append("Kombinieren")
    if job.title_card_enabled:
        parts.append("Titelkarte")
    if job.create_youtube_version:
        parts.append("YT-Version")
    if job.upload_youtube:
        parts.append("YT-Upload")
    if job.upload_kaderblick:
        parts.append("KB")
    return " → ".join(parts) if parts else "—"


def _format_resume_tooltip(job: WorkflowJob) -> str:
    if not job.step_statuses:
        return job.resume_status or ""
    labels = {
        "transfer": "Transfer",
        "convert": "Konvertierung",
        "merge": "Zusammenführen",
        "titlecard": "Titelkarte",
        "yt_version": "YT-Version",
        "youtube_upload": "YouTube-Upload",
        "kaderblick": "Kaderblick",
    }
    lines = []
    for key in ("transfer", "convert", "merge", "titlecard", "yt_version", "youtube_upload", "kaderblick"):
        value = job.step_statuses.get(key)
        if value:
            lines.append(f"{labels.get(key, key)}: {value}")
    if job.resume_status:
        lines.insert(0, f"Letzter Status: {job.resume_status}")
    return "\n".join(lines)


def _planned_job_steps(job: WorkflowJob) -> list[str]:
    has_merge = any(file.merge_group_id for file in job.files)
    has_output_stack = job.convert_enabled or has_merge or job.upload_youtube

    steps = ["transfer"]
    if job.convert_enabled:
        steps.append("convert")
    if has_merge:
        steps.append("merge")
    if has_output_stack and job.title_card_enabled:
        steps.append("titlecard")
    if has_output_stack and job.create_youtube_version:
        steps.append("yt_version")
    if has_output_stack and job.upload_youtube:
        steps.append("youtube_upload")
    if has_output_stack and job.upload_youtube and job.upload_kaderblick:
        steps.append("kaderblick")
    return steps


def _is_finished_step(status: str) -> bool:
    return status in {"done", "reused-target", "skipped"}


def _infer_step_key(job: WorkflowJob, status: str) -> str:
    if job.current_step_key:
        return job.current_step_key

    prefixes = (
        ("Transfer", "transfer"),
        ("Konvertiere", "convert"),
        ("Zusammenführen", "merge"),
        ("Titelkarte", "titlecard"),
        ("YT-Version", "yt_version"),
        ("YouTube-Upload", "youtube_upload"),
        ("Kaderblick", "kaderblick"),
    )
    for prefix, step_key in prefixes:
        if status.startswith(prefix):
            return step_key

    for step_key in reversed(_planned_job_steps(job)):
        if step_key in job.step_statuses:
            return step_key
    return "transfer"


def _compute_job_overall_progress(job: WorkflowJob, status: str, step_pct: int) -> int:
    planned_steps = _planned_job_steps(job)
    if not planned_steps:
        return 100 if status == "Fertig" else 0
    if status == "Fertig":
        return 100

    current_step = _infer_step_key(job, status)
    if current_step not in planned_steps:
        current_step = planned_steps[0]

    completed = sum(1 for step_key in planned_steps if _is_finished_step(job.step_statuses.get(step_key, "")))
    step_index = planned_steps.index(current_step)
    pct = max(0, min(step_pct, 100))

    if _is_finished_step(job.step_statuses.get(current_step, "")) and pct >= 100:
        completed = max(completed, step_index + 1)
        return int(completed / len(planned_steps) * 100)

    completed_before_current = min(completed, step_index)
    return int((completed_before_current + pct / 100.0) / len(planned_steps) * 100)


def _job_has_source_config(job: WorkflowJob) -> bool:
    if job.source_mode == "files":
        return bool(job.files)
    if job.source_mode == "folder_scan":
        return bool(job.source_folder.strip())
    if job.source_mode == "pi_download":
        return bool(job.device_name.strip())
    return False


def _jobs_look_compatible(restored: WorkflowJob, fallback: WorkflowJob) -> bool:
    if restored.source_mode != fallback.source_mode:
        return False
    if restored.id and restored.id == fallback.id:
        return True
    if restored.name and fallback.name and restored.name == fallback.name:
        return True
    return restored.name in {"", "Job 1"} or fallback.name in {"", "Job 1"}


def _overlay_resume_state(target: WorkflowJob, source: WorkflowJob) -> WorkflowJob:
    target.enabled = source.enabled
    if source.name:
        target.name = source.name
    target.resume_status = source.resume_status
    target.step_statuses = dict(source.step_statuses) if isinstance(source.step_statuses, dict) else {}
    target.progress_pct = source.progress_pct
    target.overall_progress_pct = source.overall_progress_pct
    target.current_step_key = source.current_step_key
    return target


def _repair_restored_workflow(restored: Workflow, fallback: Workflow | None) -> tuple[Workflow, int, int]:
    if not restored.jobs:
        return restored, 0, 0

    repaired = 0
    dropped_resume_state = 0
    repaired_jobs: list[WorkflowJob] = []

    for idx, job in enumerate(restored.jobs):
        if _job_has_source_config(job):
            repaired_jobs.append(job)
            continue

        fallback_job = None
        if fallback and idx < len(fallback.jobs):
            candidate = fallback.jobs[idx]
            if _job_has_source_config(candidate) and _jobs_look_compatible(job, candidate):
                fallback_job = WorkflowJob.from_dict(candidate.to_dict())

        if fallback_job is not None:
            repaired_jobs.append(_overlay_resume_state(fallback_job, job))
            repaired += 1
            continue

        if job.resume_status or job.step_statuses:
            job.resume_status = ""
            job.step_statuses = {}
            job.progress_pct = 0
            job.overall_progress_pct = 0
            job.current_step_key = ""
            dropped_resume_state += 1
        repaired_jobs.append(job)

    restored.jobs = repaired_jobs
    return restored, repaired, dropped_resume_state


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
        settings_menu.addAction("Kaderblick …", self._open_kaderblick_settings)
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

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(
            ["#", "Name", "Quelle", "Pipeline", "Status", "Job"])
        hdr = self.table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(1, QHeaderView.Stretch)
        hdr.setSectionResizeMode(2, QHeaderView.Interactive)
        hdr.setSectionResizeMode(3, QHeaderView.Interactive)
        hdr.setSectionResizeMode(4, QHeaderView.Interactive)
        hdr.setSectionResizeMode(5, QHeaderView.Interactive)
        hdr.resizeSection(2, 180)
        hdr.resizeSection(3, 160)
        hdr.resizeSection(4, 260)
        hdr.resizeSection(5, 120)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        self.table.doubleClicked.connect(self._edit_job)

        self._step_progress_delegate = ProgressDelegate(self.table, progress_role=_ROLE_STEP_PROGRESS)
        self._job_progress_delegate = ProgressDelegate(self.table, progress_role=_ROLE_JOB_PROGRESS)
        self.table.setItemDelegateForColumn(4, self._step_progress_delegate)
        self.table.setItemDelegateForColumn(5, self._job_progress_delegate)

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
            status_item = QTableWidgetItem(job.resume_status or "Wartend")
            status_item.setData(_ROLE_STEP_PROGRESS, job.progress_pct)
            tooltip = _format_resume_tooltip(job)
            status_item.setToolTip(tooltip)
            self.table.setItem(i, 4, status_item)

            job_item = QTableWidgetItem(f"{job.overall_progress_pct}%")
            job_item.setData(_ROLE_JOB_PROGRESS, job.overall_progress_pct)
            self.table.setItem(i, 5, job_item)

    def _set_row_status(self, row: int, status: str):
        """Schreibt Status-Text in Spalte 4 und setzt Farbe."""
        item = self.table.item(row, 4)
        if item is None:
            item = QTableWidgetItem()
            self.table.setItem(row, 4, item)
        item.setText(status)
        active_prefixes = (
            "Läuft",
            "Herunterladen",
            "Transfer",
            "Konvertiere",
            "Zusammenführen",
            "Titelkarte",
            "YT-Version",
            "YouTube-Upload",
            "Kaderblick",
        )
        if status == "Fertig":
            item.setForeground(Qt.darkGreen)
        elif "Fehler" in status:
            item.setForeground(Qt.red)
        elif status == "Übersprungen":
            item.setForeground(Qt.gray)
        elif status.startswith(active_prefixes):
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
        item.setData(_ROLE_STEP_PROGRESS, pct)
        self.table.viewport().update()

    def _set_row_job_progress(self, row: int, pct: int):
        item = self.table.item(row, 5)
        if item is None:
            item = QTableWidgetItem()
            self.table.setItem(row, 5, item)
        item.setText(f"{pct}%")
        item.setData(_ROLE_JOB_PROGRESS, pct)
        self.table.viewport().update()

    def _reset_status_column(self):
        """Setzt Status- und Job-Fortschritt zurück."""
        for i in range(self.table.rowCount()):
            status_item = self.table.item(i, 4)
            if status_item is None:
                status_item = QTableWidgetItem()
                self.table.setItem(i, 4, status_item)
            status_item.setText("Wartend")
            status_item.setData(_ROLE_STEP_PROGRESS, 0)
            status_item.setForeground(Qt.black)

            job_item = self.table.item(i, 5)
            if job_item is None:
                job_item = QTableWidgetItem()
                self.table.setItem(i, 5, job_item)
            job_item.setText("0%")
            job_item.setData(_ROLE_JOB_PROGRESS, 0)
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
            self._workflow.save_as_last()

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
            self._workflow.save_as_last()
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
        self._workflow.save_as_last()

    def _clear_jobs(self):
        if self._workflow.jobs:
            if QMessageBox.question(
                    self, "Bestätigung", "Alle Aufträge entfernen?",
                    QMessageBox.Yes | QMessageBox.No
            ) == QMessageBox.Yes:
                self._workflow.jobs.clear()
                self._refresh_table()
                self.status_label.setText("Bereit")
                self._workflow.save_as_last()

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
                self._workflow.save_as_last()

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
        self._workflow.save_as_last()

    def _update_count(self):
        n = len(self._workflow.jobs)
        self.status_label.setStyleSheet("")
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

    def _open_kaderblick_settings(self):
        KaderblickSettingsDialog(self, self.settings).exec()

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
                restored = Workflow.load(SESSION_FILE)
                fallback = Workflow.load_last()
                restored, repaired, dropped = _repair_restored_workflow(restored, fallback)
                self._workflow = restored
                self._refresh_table()
                self._update_count()
                msg = (
                    f"Session wiederhergestellt: "
                    f"{len(self._workflow.jobs)} Auftrag/Aufträge"
                )
                if repaired:
                    msg += f" | {repaired} unvollständige Aufträge aus letzter Jobliste repariert"
                if dropped:
                    msg += f" | {dropped} Resume-Status ohne gültige Konfiguration verworfen"
                self._append_log(msg)
            except Exception:
                pass

    def _has_resumeable_jobs(self) -> bool:
        return any(
            job.enabled and _job_has_source_config(job) and (job.resume_status or job.step_statuses)
            for job in self._workflow.jobs
        )

    def _ask_resume_behavior(self) -> QMessageBox.StandardButton:
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Icon.Question)
        box.setWindowTitle("Workflow fortsetzen?")
        box.setText(
            "Es gibt gespeicherte Fortschrittsdaten eines vorherigen Laufs.\n"
            "Soll der Workflow fortgesetzt oder neu gestartet werden?"
        )
        continue_button = box.addButton("Fortsetzen", QMessageBox.ButtonRole.AcceptRole)
        restart_button = box.addButton("Neu starten", QMessageBox.ButtonRole.DestructiveRole)
        cancel_button = box.addButton("Abbrechen", QMessageBox.ButtonRole.RejectRole)
        box.setDefaultButton(continue_button)
        box.exec()

        clicked = box.clickedButton()
        if clicked is continue_button:
            return QMessageBox.Yes
        if clicked is restart_button:
            return QMessageBox.No
        if clicked is cancel_button:
            return QMessageBox.Cancel
        return QMessageBox.Cancel

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

        resume_existing = False
        if self._has_resumeable_jobs():
            choice = self._ask_resume_behavior()
            if choice == QMessageBox.Cancel:
                return
            resume_existing = choice == QMessageBox.Yes

        if not resume_existing:
            self._reset_status_column()
            for job in self._workflow.jobs:
                job.resume_status = ""
                job.step_statuses = {}
                job.progress_pct = 0
                job.overall_progress_pct = 0
                job.current_step_key = ""
            self._save_session()
        else:
            self._append_log("Fortsetzen vorhandener Workflow-Sitzung …")

        self.status_label.setStyleSheet("")

        self._set_busy(True)
        self._append_log(
            f"\n{'═'*60}"
            f"\n  ▶ Workflow {'fortgesetzt' if resume_existing else 'gestartet'}  ({len(enabled)} aktive Aufträge)"
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
        if 0 <= orig_idx < len(self._workflow.jobs):
            job = self._workflow.jobs[orig_idx]
            job.resume_status = status
            overall_pct = _compute_job_overall_progress(job, status, job.progress_pct)
            job.overall_progress_pct = overall_pct
            item = self.table.item(orig_idx, 4)
            if item is not None:
                tooltip = _format_resume_tooltip(job)
                item.setToolTip(tooltip)
            self._set_row_job_progress(orig_idx, overall_pct)
            self._save_session()

    @Slot(int, int)
    def _on_job_progress(self, orig_idx: int, pct: int):
        if 0 <= orig_idx < self.table.rowCount():
            self._set_row_progress(orig_idx, pct)
        if 0 <= orig_idx < len(self._workflow.jobs):
            job = self._workflow.jobs[orig_idx]
            job.progress_pct = pct
            overall_pct = _compute_job_overall_progress(job, job.resume_status or job.status, pct)
            job.overall_progress_pct = overall_pct
            if 0 <= orig_idx < self.table.rowCount():
                self._set_row_job_progress(orig_idx, overall_pct)

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

    @Slot(str)
    def _on_phase_changed(self, phase: str):
        self.status_label.setText(phase)

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
        if fail > 0:
            msg = (f"❌ FEHLER: {fail} Fehler, {ok} OK, {skip} übersprungen"
                   f"  ({self._format_duration(elapsed)})")
            self.status_label.setStyleSheet(
                "color: white; background: #c0392b; font-weight: bold; padding: 2px 6px;")
        else:
            msg = (f"✅ Fertig: {ok} OK, {skip} übersprungen"
                   f"  ({self._format_duration(elapsed)})")
            self.status_label.setStyleSheet(
                "color: white; background: #27ae60; font-weight: bold; padding: 2px 6px;")
        self._append_log(f"\n{msg}")
        self.status_label.setText(msg)
        self._set_busy(False)
        self._save_session()

        if self._workflow.shutdown_after and fail == 0:
            from .dialogs import ShutdownCountdownDialog
            dlg = ShutdownCountdownDialog(seconds=30, parent=self)
            if dlg.exec():
                self._append_log("\n⏻ Rechner wird heruntergefahren …")
                import subprocess
                subprocess.Popen(["shutdown", "now"])
            else:
                self._append_log("\n⚠ Herunterfahren durch Benutzer abgebrochen.")
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
