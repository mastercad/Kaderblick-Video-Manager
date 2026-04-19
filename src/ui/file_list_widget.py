"""Wiederverwendbares Widget zur Verwaltung der Dateiliste eines Workflows.

Zeigt pro Datei: Quelldatei (nur Anzeige), Ausgabename, YouTube-Titel,
Playlist und Kaderblick-Start.

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

from PySide6.QtCore import Qt, QEvent, Signal
from PySide6.QtGui import QFont, QColor
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QAbstractItemView,
    QFileDialog, QMessageBox, QSizePolicy,
)

from ..workflow import FileEntry


class FileListWidget(QWidget):
    """Tabellen-Widget für die Dateiliste eines Auftrags.

    Spalten
    -------
    0  Quelldatei       – nicht editierbar, voller Pfad als Tooltip
    1  Größe            – nicht editierbar, rechtsbündig (z. B. "3,7 GB")
    2  Ausgabename      – editierbar (leer = automatisch aus Quelldatei)
    3  YouTube-Titel    – editierbar (leer = Dateiname)
    4  Playlist         – editierbar
    5  KB-Start (s)     – editierbar, Kaderblick Startzeit in Sekunden
    6  KB-Typ-ID        – editierbar, Kaderblick Video-Typ-ID
    6  KB-Start (s)     – editierbar, Kaderblick Startzeit in Sekunden
    """

    _COL_PATH     = 0
    _COL_SIZE     = 1
    _COL_OUTNAME  = 2
    _COL_YT_TITLE = 3
    _COL_PLAYLIST = 4
    _COL_KB_START = 5
    _COL_MIN_WIDTHS = {
        _COL_PATH: 220,
        _COL_SIZE: 86,
        _COL_OUTNAME: 180,
        _COL_YT_TITLE: 260,
        _COL_PLAYLIST: 160,
        _COL_KB_START: 92,
    }
    _COL_MAX_WIDTHS = {
        _COL_PATH: 420,
        _COL_OUTNAME: 320,
        _COL_YT_TITLE: 520,
        _COL_PLAYLIST: 280,
    }
    # UserRole offsets on the path item
    _ROLE_KB_TYPE  = Qt.UserRole
    _ROLE_KB_CAM   = Qt.UserRole + 1
    _ROLE_YT_DESC  = Qt.UserRole + 2
    _ROLE_MERGE_ID = Qt.UserRole + 3
    _ROLE_TC_SUB   = Qt.UserRole + 4
    _ROLE_GRAPH_SRC = Qt.UserRole + 5
    _ROLE_TC_BEFORE = Qt.UserRole + 6

    # Emitted when bulk match data is confirmed.
    # Arguments: home_team, away_team, date_iso (YYYY-MM-DD)
    match_data_changed = Signal(str, str, str)
    files_changed = Signal()

    # Merge group colors (cycled by group hash)
    _MERGE_COLORS = [
        QColor("#d0e8ff"),   # blau
        QColor("#d0ffd6"),   # grün
        QColor("#fff0b3"),   # gelb
        QColor("#ffd6f5"),   # pink
        QColor("#ffe0cc"),   # orange
    ]

    _MIN_VISIBLE_ROWS = 4
    _DEFAULT_VISIBLE_ROWS = 5
    _MAX_VISIBLE_ROWS = 5
    _ROW_HEIGHT = 28

    @staticmethod
    def _format_bytes(b: int) -> str:
        """Gibt eine Byte-Anzahl als lesbaren String zurück (KB/MB/GB)."""
        if b >= 1_073_741_824:
            return f"{b / 1_073_741_824:.1f} GB"
        if b >= 1_048_576:
            return f"{b / 1_048_576:.0f} MB"
        if b >= 1_024:
            return f"{b / 1_024:.0f} KB"
        return f"{b} B"

    @classmethod
    def _size_text(cls, entry: FileEntry) -> str:
        if entry.source_size_bytes > 0:
            return cls._format_bytes(entry.source_size_bytes)
        if not entry.source_path:
            return ""
        try:
            return cls._format_bytes(Path(entry.source_path).stat().st_size)
        except OSError:
            return "?"

    def __init__(self, last_dir_getter, last_dir_setter, parent=None):
        super().__init__(parent)
        self._get_last_dir = last_dir_getter
        self._set_last_dir = last_dir_setter
        self._kb_video_types: list = []
        self._kb_cameras: list = []
        self._build_ui()

    # ── Öffentliche Schnittstelle ─────────────────────────────

    def load(self, entries: list[FileEntry]) -> None:
        """Füllt die Tabelle mit einer Liste von FileEntry-Objekten."""
        self._table.setRowCount(0)
        for entry in entries:
            self._append_row(entry)
        self._autosize_content_columns()
        self._update_table_height()
        self.files_changed.emit()

    def collect(self) -> list[FileEntry]:
        """Liest alle Tabellenzeilen als FileEntry-Liste aus."""
        result = []
        for row in range(self._table.rowCount()):
            kb_start_text = self._cell_text(row, self._COL_KB_START)
            try:
                kb_start = int(kb_start_text) if kb_start_text else 0
            except ValueError:
                kb_start = 0
            path_item = self._table.item(row, self._COL_PATH)
            kb_type  = path_item.data(self._ROLE_KB_TYPE)  or 0  if path_item else 0
            kb_cam   = path_item.data(self._ROLE_KB_CAM)   or 0  if path_item else 0
            kb_desc  = path_item.data(self._ROLE_YT_DESC)  or "" if path_item else ""
            merge_id = path_item.data(self._ROLE_MERGE_ID) or "" if path_item else ""
            tc_sub   = path_item.data(self._ROLE_TC_SUB) or "" if path_item else ""
            graph_src = path_item.data(self._ROLE_GRAPH_SRC) or "" if path_item else ""
            tc_before = bool(path_item.data(self._ROLE_TC_BEFORE)) if path_item else False
            result.append(FileEntry(
                source_path=self._cell_text(row, self._COL_PATH),
                source_size_bytes=int(path_item.data(Qt.UserRole + 10) or 0) if path_item else 0,
                output_filename=self._cell_text(row, self._COL_OUTNAME),
                youtube_title=self._cell_text(row, self._COL_YT_TITLE),
                youtube_description=kb_desc,
                youtube_playlist=self._cell_text(row, self._COL_PLAYLIST),
                kaderblick_game_start=kb_start,
                kaderblick_video_type_id=kb_type,
                kaderblick_camera_id=kb_cam,
                merge_group_id=merge_id,
                title_card_subtitle=tc_sub,
                graph_source_id=graph_src,
                title_card_before_merge=tc_before,
            ))
        return result

    def is_empty(self) -> bool:
        return self._table.rowCount() == 0

    # ── UI aufbauen ───────────────────────────────────────────

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels(
            ["Quelldatei", "Größe", "Ausgabename", "YouTube-Titel",
             "Playlist", "KB-Start (s)"])

        hdr = self._table.horizontalHeader()
        hdr.setSectionResizeMode(self._COL_PATH,     QHeaderView.Interactive)
        hdr.setSectionResizeMode(self._COL_SIZE,     QHeaderView.Fixed)
        hdr.setSectionResizeMode(self._COL_OUTNAME,  QHeaderView.Interactive)
        hdr.setSectionResizeMode(self._COL_YT_TITLE, QHeaderView.Interactive)
        hdr.setSectionResizeMode(self._COL_PLAYLIST, QHeaderView.Interactive)
        hdr.setSectionResizeMode(self._COL_KB_START, QHeaderView.Fixed)
        hdr.setStretchLastSection(False)
        hdr.setMinimumSectionSize(60)
        self._apply_initial_column_widths()

        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.MultiSelection)
        self._table.verticalHeader().setVisible(False)
        self._table.setAlternatingRowColors(True)
        self._table.verticalHeader().setDefaultSectionSize(self._ROW_HEIGHT)
        self._table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self._table.installEventFilter(self)
        self._update_table_height()
        layout.addWidget(self._table)

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

        bulk_btn = QPushButton("🎬 Alle belegen …")
        bulk_btn.setToolTip(
            "Spieldaten für alle Dateien auf einmal setzen.\n"
            "Wenn Teil-Nummer aktiv ist, wird sie pro Datei automatisch hochgezählt.")
        bulk_btn.clicked.connect(self._bulk_edit)
        btn_row.addWidget(bulk_btn)

        layout.addLayout(btn_row)
        self._update_widget_height()

    # ── Interne Hilfsmethoden ─────────────────────────────────

    def _append_row(self, entry: FileEntry) -> None:
        row = self._table.rowCount()
        self._table.insertRow(row)

        name_item = QTableWidgetItem(Path(entry.source_path).name
                                     if entry.source_path else "")
        name_item.setFlags(name_item.flags() & ~Qt.ItemIsEditable)
        name_item.setToolTip(entry.source_path)
        name_item.setFont(QFont("Monospace", 9))
        name_item.setData(self._ROLE_KB_TYPE,  entry.kaderblick_video_type_id or 0)
        name_item.setData(self._ROLE_KB_CAM,   entry.kaderblick_camera_id or 0)
        name_item.setData(self._ROLE_YT_DESC,  entry.youtube_description or "")
        name_item.setData(self._ROLE_MERGE_ID, getattr(entry, 'merge_group_id', '') or "")
        name_item.setData(self._ROLE_TC_SUB, getattr(entry, 'title_card_subtitle', '') or "")
        name_item.setData(self._ROLE_GRAPH_SRC, getattr(entry, 'graph_source_id', '') or "")
        name_item.setData(self._ROLE_TC_BEFORE, bool(getattr(entry, 'title_card_before_merge', False)))
        name_item.setData(Qt.UserRole + 10, int(entry.source_size_bytes or 0))
        self._table.setItem(row, self._COL_PATH, name_item)

        size_item = QTableWidgetItem(self._size_text(entry))
        size_item.setFlags(size_item.flags() & ~Qt.ItemIsEditable)
        size_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        size_item.setFont(QFont("Monospace", 9))
        self._table.setItem(row, self._COL_SIZE, size_item)

        self._table.setItem(row, self._COL_OUTNAME,
                            QTableWidgetItem(entry.output_filename))
        self._table.setItem(row, self._COL_YT_TITLE,
                            QTableWidgetItem(entry.youtube_title))
        self._table.setItem(row, self._COL_PLAYLIST,
                            QTableWidgetItem(entry.youtube_playlist))

        kb_start_item = QTableWidgetItem(
            str(entry.kaderblick_game_start) if entry.kaderblick_game_start else "0")
        kb_start_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self._table.setItem(row, self._COL_KB_START, kb_start_item)

        self._table.setRowHeight(row, self._ROW_HEIGHT)

        # Merge-Visualisierung anwenden
        self._refresh_merge_visuals()
        self._update_table_height()
        self.files_changed.emit()

    def _update_table_height(self) -> None:
        row_count = self._table.rowCount()
        visible_rows = max(min(row_count, self._MAX_VISIBLE_ROWS), self._MIN_VISIBLE_ROWS)
        if row_count == 0:
            visible_rows = self._MIN_VISIBLE_ROWS
        frame = self._table.frameWidth() * 2
        header_height = self._table.horizontalHeader().height()
        height = header_height + (visible_rows * self._ROW_HEIGHT) + frame
        self._table.setMinimumHeight(height)
        self._table.setMaximumHeight(height)
        self._update_widget_height()

    def _update_widget_height(self) -> None:
        if self.layout() is None:
            return
        self.layout().activate()
        height = self.layout().sizeHint().height()
        self.setMinimumHeight(height)
        self.setMaximumHeight(height)

    def _apply_initial_column_widths(self) -> None:
        for column, width in self._COL_MIN_WIDTHS.items():
            self._table.setColumnWidth(column, width)

    def _autosize_content_columns(self) -> None:
        self._table.resizeColumnsToContents()
        for column, min_width in self._COL_MIN_WIDTHS.items():
            width = max(self._table.columnWidth(column), min_width)
            max_width = self._COL_MAX_WIDTHS.get(column)
            if max_width is not None:
                width = min(width, max_width)
            self._table.setColumnWidth(column, width)

    # ── Kaderblick-Optionen ───────────────────────────────────

    def set_kaderblick_options(self, video_types: list, cameras: list) -> None:
        """Speichert die von der API geladenen Typen/Kameras für den Zeilen-Editor."""
        self._kb_video_types = video_types or []
        self._kb_cameras = cameras or []

    def _refresh_merge_visuals(self) -> None:
        """Aktualisiert die rein visuelle Markierung vorhandener Merge-Gruppen."""
        # Sammle alle Gruppen-IDs in Reihenfolge des ersten Auftretens
        group_order: list[str] = []
        for r in range(self._table.rowCount()):
            item = self._table.item(r, self._COL_PATH)
            gid = (item.data(self._ROLE_MERGE_ID) or "") if item else ""
            if gid and gid not in group_order:
                group_order.append(gid)

        for r in range(self._table.rowCount()):
            path_item = self._table.item(r, self._COL_PATH)
            gid = (path_item.data(self._ROLE_MERGE_ID) or "") if path_item else ""

            color_idx = group_order.index(gid) % len(self._MERGE_COLORS) if gid else -1
            bg = self._MERGE_COLORS[color_idx] if gid else None

            for col in range(self._table.columnCount()):
                cell = self._table.item(r, col)
                if cell is None:
                    continue
                # Farbe setzen
                if bg:
                    cell.setBackground(bg)
                else:
                    cell.setBackground(QColor(0, 0, 0, 0))
                if col not in (self._COL_PATH, self._COL_SIZE):
                    cell.setFlags(cell.flags() | Qt.ItemIsEditable)
                    cell.setForeground(QColor())
                    if gid:
                        cell.setToolTip("Teil einer Merge-Gruppe. Das gemeinsame Ausgabe-Metadatum wird am Merge-Node gepflegt.")
                    else:
                        cell.setToolTip("")

    def _bulk_edit(self) -> None:
        """Setzt Spieldaten für alle Zeilen; Teil-Nummer wird pro Zeile erhöht."""
        from ..integrations.youtube_title_editor import (
            YouTubeTitleEditorDialog, build_video_title, build_video_description,
            build_playlist_title, SegmentData,
        )
        n = self._table.rowCount()
        if n == 0:
            return
        dlg = YouTubeTitleEditorDialog(self, mode="full", auto_increment_part=False,
                                        kb_video_types=self._kb_video_types,
                                        kb_cameras=self._kb_cameras)
        if not dlg.exec():
            return

        match    = dlg.match_data
        seg_base = dlg.segment_data
        playlist = build_playlist_title(match)

        for i in range(n):
            if seg_base.part > 0:
                seg = SegmentData(
                    camera=seg_base.camera,
                    side=seg_base.side,
                    half=seg_base.half,
                    part=seg_base.part + i,
                    type_name=seg_base.type_name,
                )
            else:
                seg = seg_base
            self._set_cell(i, self._COL_YT_TITLE, build_video_title(match, seg))
            self._set_cell(i, self._COL_PLAYLIST,  playlist)
            path_item = self._table.item(i, self._COL_PATH)
            if path_item:
                path_item.setData(self._ROLE_KB_TYPE, dlg.kb_video_type_id)
                path_item.setData(self._ROLE_KB_CAM,  dlg.kb_camera_id)
                path_item.setData(self._ROLE_YT_DESC, build_video_description(match, seg))
        self.match_data_changed.emit(match.home_team, match.away_team, match.date_iso)
        self.files_changed.emit()

    def _open_add_files_dialog(self) -> None:
        start = self._get_last_dir() or str(Path.home())
        paths, _ = QFileDialog.getOpenFileNames(
            self, "Dateien hinzufügen", start,
            "Videodateien (*.mp4 *.mkv *.avi *.mov *.mjpg *.mjpeg);;"
            "Alle Dateien (*)")
        if not paths:
            return
        self._set_last_dir(Path(paths[0]).parent.as_posix())
        for p in sorted(paths):
            self._append_row(FileEntry(
                source_path=p,
            ))

    def eventFilter(self, obj, event) -> bool:
        if obj is self._table and event.type() == QEvent.KeyPress:
            if event.key() in (Qt.Key_Delete, Qt.Key_Backspace):
                self._remove_selected_rows()
                return True
        return super().eventFilter(obj, event)

    def _remove_selected_rows(self) -> None:
        rows = sorted(self._selected_rows(), reverse=True)
        if not rows:
            return
        result = QMessageBox.question(
            self,
            "Einträge entfernen",
            f"{len(rows)} ausgewählte Datei(en) wirklich aus der Liste entfernen?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if result != QMessageBox.Yes:
            return
        for row in rows:
            self._table.removeRow(row)
        self._update_table_height()
        self.files_changed.emit()

    def _selected_rows(self) -> list[int]:
        return sorted({idx.row() for idx in self._table.selectedIndexes()})

    def _set_cell(self, row: int, col: int, text: str) -> None:
        item = self._table.item(row, col)
        if item is None:
            item = QTableWidgetItem()
            self._table.setItem(row, col, item)
        item.setText(text)

    def _cell_text(self, row: int, col: int) -> str:
        item = self._table.item(row, col)
        if col == self._COL_PATH:
            return item.toolTip() if item else ""
        return item.text().strip() if item else ""
