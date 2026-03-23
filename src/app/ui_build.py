"""UI construction and table rendering for the main window."""

from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import Qt, Slot
from PySide6.QtGui import QFont, QKeySequence
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QHeaderView,
    QLabel,
    QMainWindow,
    QProgressBar,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QToolBar,
)

from ..ui.delegates import ProgressDelegate
from .helpers import _format_resume_tooltip, _summarize_pipeline, _summarize_source


_ROLE_STEP_PROGRESS = int(Qt.ItemDataRole.UserRole)
_ROLE_JOB_PROGRESS = _ROLE_STEP_PROGRESS + 1


def _build_menu(self: QMainWindow):
    mb = self.menuBar()

    file_menu = mb.addMenu("&Datei")
    action = file_menu.addAction("Workflow laden …")
    action.setShortcut(QKeySequence("Ctrl+I"))
    action.triggered.connect(self._load_workflow)
    action = file_menu.addAction("Workflow speichern …")
    action.setShortcut(QKeySequence("Ctrl+E"))
    action.triggered.connect(self._save_workflow)
    file_menu.addSeparator()
    file_menu.addAction("Workflow leeren", self._clear_workflow)
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


def _build_toolbar(self: QMainWindow):
    tb = QToolBar("Aktionen")
    tb.setMovable(False)
    self.addToolBar(tb)

    tb.addAction("＋ Neuer Workflow", self._new_workflow)
    tb.addSeparator()
    tb.addAction("Bearbeiten", self._edit_job)
    tb.addAction("Workflow", self._open_job_workflow)
    tb.addAction("Entfernen", self._clear_workflow)
    tb.addSeparator()

    self.act_start = tb.addAction("▶  Starten", self._start_selected_workflows)
    self.act_start_all = tb.addAction("▶▶  Alle aktiven", self._start_all_active_workflows)
    self.act_cancel = tb.addAction("■  Abbrechen", self._cancel_workflow)
    self.act_cancel.setEnabled(False)
    tb.addSeparator()

    tb.addAction("Laden", self._load_workflow)
    tb.addAction("Speichern", self._save_workflow)
    tb.addSeparator()

    self._shutdown_cb = QCheckBox("Rechner herunterfahren")
    tb.addWidget(self._shutdown_cb)


def _build_central(self: QMainWindow):
    splitter = QSplitter(Qt.Vertical)

    self.table = QTableWidget(0, 6)
    self.table.setHorizontalHeaderLabels(["#", "Name", "Quelle", "Pipeline", "Status", "Job"])
    header = self.table.horizontalHeader()
    header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
    header.setSectionResizeMode(1, QHeaderView.Stretch)
    header.setSectionResizeMode(2, QHeaderView.Interactive)
    header.setSectionResizeMode(3, QHeaderView.Interactive)
    header.setSectionResizeMode(4, QHeaderView.Interactive)
    header.setSectionResizeMode(5, QHeaderView.Interactive)
    header.resizeSection(2, 180)
    header.resizeSection(3, 160)
    header.resizeSection(4, 260)
    header.resizeSection(5, 120)
    self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
    self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
    self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
    self.table.verticalHeader().setVisible(False)
    self.table.setAlternatingRowColors(True)
    self.table.doubleClicked.connect(self._handle_table_double_click)

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


def _build_statusbar(self: QMainWindow):
    self.status_label = QLabel("Bereit")
    self.statusBar().addWidget(self.status_label, 2)

    self.progress = QProgressBar()
    self.progress.setTextVisible(True)
    self.progress.setMaximumHeight(18)
    self.progress.setValue(0)
    self.progress.setFormat("Gesamt: %v/%m")
    self.statusBar().addPermanentWidget(self.progress, 1)


def _refresh_table(self):
    jobs = self._workflow.jobs
    self.table.setRowCount(len(jobs))
    for row, job in enumerate(jobs):
        self.table.setItem(row, 0, QTableWidgetItem(str(row + 1)))
        self.table.setItem(row, 1, QTableWidgetItem(job.name or "–"))
        self.table.setItem(row, 2, QTableWidgetItem(_summarize_source(job)))
        pipeline_item = QTableWidgetItem(_summarize_pipeline(job))
        pipeline_item.setToolTip("Doppelklick für Workflow-Ansicht")
        self.table.setItem(row, 3, pipeline_item)
        status_item = QTableWidgetItem(job.resume_status or "Wartend")
        status_item.setData(_ROLE_STEP_PROGRESS, job.progress_pct)
        status_item.setToolTip(_format_resume_tooltip(job))
        self.table.setItem(row, 4, status_item)

        job_item = QTableWidgetItem(f"{job.overall_progress_pct}%")
        job_item.setData(_ROLE_JOB_PROGRESS, job.overall_progress_pct)
        job_item.setToolTip("Doppelklick für Workflow-Ansicht")
        self.table.setItem(row, 5, job_item)


def _set_row_status(self, row: int, status: str):
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
    for row in range(self.table.rowCount()):
        status_item = self.table.item(row, 4)
        if status_item is None:
            status_item = QTableWidgetItem()
            self.table.setItem(row, 4, status_item)
        status_item.setText("Wartend")
        status_item.setData(_ROLE_STEP_PROGRESS, 0)
        status_item.setForeground(Qt.black)

        job_item = self.table.item(row, 5)
        if job_item is None:
            job_item = QTableWidgetItem()
            self.table.setItem(row, 5, job_item)
        job_item.setText("0%")
        job_item.setData(_ROLE_JOB_PROGRESS, 0)
    self.table.viewport().update()


def _update_count(self):
    self.status_label.setStyleSheet("")
    count = len(self._workflow.jobs)
    self.status_label.setText(f"{count} Workflow{'s' if count != 1 else ''} geladen" if count else "Bereit")


def _handle_table_double_click(self, index):
    self._open_job_workflow(index.row())


@Slot(str)
def _append_log(self, msg: str):
    self.log_text.append(msg)