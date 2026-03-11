"""Wiederverwendbares Widget zur Verwaltung der Dateiliste eines Auftrags.

Zeigt pro Datei: Quelldatei (nur Anzeige), Ausgabename, YouTube-Titel,
Playlist – alle drei letzten Felder sind direkt in der Tabelle editierbar.

Verwendung:
    widget = FileListWidget(
        last_dir_getter=lambda: settings.last_directory,
        last_dir_setter=lambda d: settings._set_last_dir(d),
    )
    widget.load(job.files)
    ...
    job.files = widget.collect()
"""

from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QAbstractItemView,
    QFileDialog, QLabel,
)

from .workflow import FileEntry


class FileListWidget(QWidget):
    """Tabellen-Widget für die Dateiliste eines Auftrags.

    Spalten
    -------
    0  Quelldatei       – nicht editierbar, voller Pfad als Tooltip
    1  Ausgabename      – editierbar (leer = automatisch aus Quelldatei)
    2  YouTube-Titel    – editierbar (leer = Dateiname)
    3  Playlist         – editierbar
    """

    _COL_PATH     = 0
    _COL_OUTNAME  = 1
    _COL_YT_TITLE = 2
    _COL_PLAYLIST = 3

    def __init__(self, last_dir_getter, last_dir_setter, parent=None):
        """
        Parameters
        ----------
        last_dir_getter : callable() -> str
            Liest das zuletzt verwendete Verzeichnis.
        last_dir_setter : callable(str)
            Speichert das zuletzt verwendete Verzeichnis.
        """
        super().__init__(parent)
        self._get_last_dir = last_dir_getter
        self._set_last_dir = last_dir_setter
        self._build_ui()

    # ── Öffentliche Schnittstelle ─────────────────────────────

    def load(self, entries: list[FileEntry]) -> None:
        """Füllt die Tabelle mit einer Liste von FileEntry-Objekten."""
        self._table.setRowCount(0)
        for entry in entries:
            self._append_row(entry)

    def collect(self) -> list[FileEntry]:
        """Liest alle Tabellenzeilen als FileEntry-Liste aus."""
        result = []
        for row in range(self._table.rowCount()):
            result.append(FileEntry(
                source_path=self._cell_text(row, self._COL_PATH),
                output_filename=self._cell_text(row, self._COL_OUTNAME),
                youtube_title=self._cell_text(row, self._COL_YT_TITLE),
                youtube_playlist=self._cell_text(row, self._COL_PLAYLIST),
            ))
        return result

    def is_empty(self) -> bool:
        return self._table.rowCount() == 0

    # ── UI aufbauen ───────────────────────────────────────────

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(
            ["Quelldatei", "Ausgabename", "YouTube-Titel", "Playlist"])

        hdr = self._table.horizontalHeader()
        hdr.setSectionResizeMode(self._COL_PATH, QHeaderView.Stretch)
        hdr.setSectionResizeMode(self._COL_OUTNAME, QHeaderView.Interactive)
        hdr.setSectionResizeMode(self._COL_YT_TITLE, QHeaderView.Stretch)
        hdr.setSectionResizeMode(self._COL_PLAYLIST, QHeaderView.Interactive)
        hdr.resizeSection(self._COL_OUTNAME, 160)
        hdr.resizeSection(self._COL_PLAYLIST, 140)

        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.verticalHeader().setVisible(False)
        self._table.setAlternatingRowColors(True)
        self._table.setMinimumHeight(130)
        layout.addWidget(self._table)

        # Hinweis-Zeile + Schaltflächen
        btn_row = QHBoxLayout()

        add_btn = QPushButton("＋ Dateien …")
        add_btn.setToolTip(
            "Dateien hinzufügen – Mehrfachauswahl möglich.\n"
            "Der Dialog öffnet sich im zuletzt verwendeten Ordner.")
        add_btn.clicked.connect(self._open_add_files_dialog)
        btn_row.addWidget(add_btn)

        remove_btn = QPushButton("Entfernen")
        remove_btn.setToolTip("Ausgewählte Einträge aus der Liste entfernen")
        remove_btn.clicked.connect(self._remove_selected_rows)
        btn_row.addWidget(remove_btn)

        btn_row.addStretch()

        hint = QLabel("Tipp: Ausgabename und YouTube-Titel direkt in der Zelle bearbeiten")
        hint.setStyleSheet("color: #888; font-size: 11px;")
        btn_row.addWidget(hint)

        layout.addLayout(btn_row)

    # ── Interne Hilfsmethoden ─────────────────────────────────

    def _append_row(self, entry: FileEntry) -> None:
        row = self._table.rowCount()
        self._table.insertRow(row)

        # Quelldatei – nicht editierbar, nur der Dateiname angezeigt, Pfad als Tooltip
        name_item = QTableWidgetItem(Path(entry.source_path).name
                                     if entry.source_path else "")
        name_item.setFlags(name_item.flags() & ~Qt.ItemIsEditable)
        name_item.setToolTip(entry.source_path)
        name_item.setFont(QFont("Monospace", 9))
        self._table.setItem(row, self._COL_PATH, name_item)

        # Editierbare Felder
        self._table.setItem(row, self._COL_OUTNAME,
                            QTableWidgetItem(entry.output_filename))
        self._table.setItem(row, self._COL_YT_TITLE,
                            QTableWidgetItem(entry.youtube_title))
        self._table.setItem(row, self._COL_PLAYLIST,
                            QTableWidgetItem(entry.youtube_playlist))

    def _open_add_files_dialog(self) -> None:
        start = self._get_last_dir() or str(Path.home())
        paths, _ = QFileDialog.getOpenFileNames(
            self, "Dateien hinzufügen", start,
            "Videodateien (*.mp4 *.mkv *.avi *.mov *.mjpg *.mjpeg);;"
            "Alle Dateien (*)")
        if not paths:
            return
        self._set_last_dir(str(Path(paths[0]).parent))
        for p in sorted(paths):
            # YouTube-Titel wird mit dem Dateinamen (ohne Endung) vorbelegt
            stem = Path(p).stem
            self._append_row(FileEntry(
                source_path=p,
                youtube_title=stem,
            ))

    def _remove_selected_rows(self) -> None:
        rows = sorted(
            {idx.row() for idx in self._table.selectedIndexes()},
            reverse=True)
        for row in rows:
            self._table.removeRow(row)

    def _cell_text(self, row: int, col: int) -> str:
        item = self._table.item(row, col)
        # Spalte 0 enthält nur den Dateinamen; der volle Pfad steckt im Tooltip
        if col == self._COL_PATH:
            return item.toolTip() if item else ""
        return item.text().strip() if item else ""
