"""Wiederverwendbares Widget zur Verwaltung der Dateiliste eines Auftrags.

Zeigt pro Datei: Quelldatei (nur Anzeige), Ausgabename, YouTube-Titel,
Playlist, 🎬-Button – letzter öffnet den YouTube-Metadaten-Editor.

Verwendung:
    widget = FileListWidget(
        last_dir_getter=lambda: settings.last_directory,
        last_dir_setter=lambda d: settings._set_last_dir(d),
    )
    widget.load(job.files)
    ...
    job.files = widget.collect()
"""

import uuid
from pathlib import Path

from PySide6.QtCore import Qt, QEvent, Signal
from PySide6.QtGui import QFont, QColor
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QAbstractItemView,
    QFileDialog, QInputDialog,
)

from .workflow import FileEntry


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
    7  🎬               – öffnet YouTube-Metadaten-Editor für diese Zeile
    """

    _COL_PATH     = 0
    _COL_SIZE     = 1
    _COL_OUTNAME  = 2
    _COL_YT_TITLE = 3
    _COL_PLAYLIST = 4
    _COL_KB_START = 5
    _COL_EDIT_BTN = 6

    # UserRole offsets on the path item
    _ROLE_KB_TYPE  = Qt.UserRole
    _ROLE_KB_CAM   = Qt.UserRole + 1
    _ROLE_YT_DESC  = Qt.UserRole + 2
    _ROLE_MERGE_ID = Qt.UserRole + 3

    # Emitted when the user confirms match data via any 🎬 dialog.
    # Arguments: home_team, away_team, date_iso (YYYY-MM-DD)
    match_data_changed = Signal(str, str, str)

    # Merge group colors (cycled by group hash)
    _MERGE_COLORS = [
        QColor("#d0e8ff"),   # blau
        QColor("#d0ffd6"),   # grün
        QColor("#fff0b3"),   # gelb
        QColor("#ffd6f5"),   # pink
        QColor("#ffe0cc"),   # orange
    ]

    @staticmethod
    def _fmt_size(path: str) -> str:
        """Gibt die Dateigröße als lesbaren String zurück (KB/MB/GB)."""
        try:
            b = Path(path).stat().st_size
        except OSError:
            return "?"
        if b >= 1_073_741_824:
            return f"{b / 1_073_741_824:.1f} GB"
        if b >= 1_048_576:
            return f"{b / 1_048_576:.0f} MB"
        if b >= 1_024:
            return f"{b / 1_024:.0f} KB"
        return f"{b} B"

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
            result.append(FileEntry(
                source_path=self._cell_text(row, self._COL_PATH),
                output_filename=self._cell_text(row, self._COL_OUTNAME),
                youtube_title=self._cell_text(row, self._COL_YT_TITLE),
                youtube_description=kb_desc,
                youtube_playlist=self._cell_text(row, self._COL_PLAYLIST),
                kaderblick_game_start=kb_start,
                kaderblick_video_type_id=kb_type,
                kaderblick_camera_id=kb_cam,
                merge_group_id=merge_id,
            ))
        return result

    def is_empty(self) -> bool:
        return self._table.rowCount() == 0

    # ── UI aufbauen ───────────────────────────────────────────

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        self._table = QTableWidget(0, 7)
        self._table.setHorizontalHeaderLabels(
            ["Quelldatei", "Größe", "Ausgabename", "YouTube-Titel",
             "Playlist", "KB-Start (s)", ""])

        hdr = self._table.horizontalHeader()
        hdr.setSectionResizeMode(self._COL_PATH,     QHeaderView.Stretch)
        hdr.setSectionResizeMode(self._COL_SIZE,     QHeaderView.Fixed)
        hdr.setSectionResizeMode(self._COL_OUTNAME,  QHeaderView.Interactive)
        hdr.setSectionResizeMode(self._COL_YT_TITLE, QHeaderView.Stretch)
        hdr.setSectionResizeMode(self._COL_PLAYLIST, QHeaderView.Interactive)
        hdr.setSectionResizeMode(self._COL_KB_START, QHeaderView.Fixed)
        hdr.setSectionResizeMode(self._COL_EDIT_BTN, QHeaderView.Fixed)
        hdr.resizeSection(self._COL_SIZE,      72)
        hdr.resizeSection(self._COL_OUTNAME,  160)
        hdr.resizeSection(self._COL_PLAYLIST, 140)
        hdr.resizeSection(self._COL_KB_START,  80)
        hdr.resizeSection(self._COL_EDIT_BTN,  34)

        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.verticalHeader().setVisible(False)
        self._table.setAlternatingRowColors(True)
        self._table.setMinimumHeight(130)
        self._table.installEventFilter(self)
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

        merge_btn = QPushButton("🔗 Zusammenführen")
        merge_btn.setToolTip(
            "Ausgewählte Dateien zu einer Merge-Gruppe zusammenfassen.\n"
            "Die Dateien werden beim Ausführen zu einer Datei verbunden.\n"
            "Titel und Playlist werden von der ersten Datei der Gruppe übernommen.")
        merge_btn.clicked.connect(self._merge_selected)
        btn_row.addWidget(merge_btn)

        unmerge_btn = QPushButton("🔓 Auflösen")
        unmerge_btn.setToolTip("Ausgewählte Dateien aus ihrer Merge-Gruppe entfernen.")
        unmerge_btn.clicked.connect(self._unmerge_selected)
        btn_row.addWidget(unmerge_btn)

        bulk_btn = QPushButton("🎬 Alle belegen …")
        bulk_btn.setToolTip(
            "Spieldaten für alle Dateien auf einmal setzen.\n"
            "Wenn Teil-Nummer aktiv ist, wird sie pro Datei automatisch hochgezählt.")
        bulk_btn.clicked.connect(self._bulk_edit)
        btn_row.addWidget(bulk_btn)

        layout.addLayout(btn_row)

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
        self._table.setItem(row, self._COL_PATH, name_item)

        size_item = QTableWidgetItem(self._fmt_size(entry.source_path)
                                     if entry.source_path else "")
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

        # 🎬-Button
        btn = QPushButton("🎬")
        btn.setToolTip("YouTube-Metadaten und Kaderblick-Zuordnung bearbeiten")
        btn.setFixedWidth(30)
        btn.clicked.connect(lambda _checked, b=btn: self._edit_row_by_widget(b))
        self._table.setCellWidget(row, self._COL_EDIT_BTN, btn)
        self._table.setRowHeight(row, 28)

        # Merge-Visualisierung anwenden
        self._refresh_merge_visuals()

    # ── Kaderblick-Optionen ───────────────────────────────────

    def set_kaderblick_options(self, video_types: list, cameras: list) -> None:
        """Speichert die von der API geladenen Typen/Kameras für den Zeilen-Editor."""
        self._kb_video_types = video_types or []
        self._kb_cameras = cameras or []

    def _edit_row_by_widget(self, widget: QPushButton) -> None:
        """Ermittelt die aktuelle Zeile des Buttons und öffnet den Editor."""
        for r in range(self._table.rowCount()):
            if self._table.cellWidget(r, self._COL_EDIT_BTN) is widget:
                self._edit_row(r)
                return

    def _edit_row(self, row: int) -> None:
        """Einzelzeilen-Editor – übernimmt Werte aus Memory, inkrementiert Teil."""
        from .youtube_title_editor import YouTubeTitleEditorDialog

        path_item = self._table.item(row, self._COL_PATH)
        merge_id = path_item.data(self._ROLE_MERGE_ID) or "" if path_item else ""
        # Nicht-erste Zeilen einer Merge-Gruppe sind gesperrt
        if merge_id and not self._is_first_in_group(row, merge_id):
            return

        current_title = self._cell_text(row, self._COL_YT_TITLE)
        has_generated = " | " in current_title
        cur_kb_type = path_item.data(self._ROLE_KB_TYPE) or 0 if path_item else 0
        cur_kb_cam  = path_item.data(self._ROLE_KB_CAM)  or 0 if path_item else 0
        dlg = YouTubeTitleEditorDialog(
            self,
            mode="full",
            auto_increment_part=not has_generated,
            kb_video_types=self._kb_video_types,
            kb_cameras=self._kb_cameras,
            initial_kb_type_id=cur_kb_type,
            initial_kb_camera_id=cur_kb_cam,
        )
        if dlg.exec():
            self._set_cell(row, self._COL_YT_TITLE, dlg.video_title)
            self._set_cell(row, self._COL_PLAYLIST,  dlg.playlist_title)
            if path_item:
                path_item.setData(self._ROLE_KB_TYPE, dlg.kb_video_type_id)
                path_item.setData(self._ROLE_KB_CAM,  dlg.kb_camera_id)
                path_item.setData(self._ROLE_YT_DESC, dlg.video_description)
            # Merge-Gruppe: Titel und Playlist an alle Folgedateien weitergeben
            if merge_id:
                self._propagate_merge_title(merge_id, dlg.video_title, dlg.playlist_title)
            m = dlg.match_data
            self.match_data_changed.emit(m.home_team, m.away_team, m.date_iso)

    # ── Merge-Logik ───────────────────────────────────────────

    def _is_first_in_group(self, row: int, merge_id: str) -> bool:
        """True wenn row die erste Zeile der angegebenen Merge-Gruppe ist."""
        for r in range(self._table.rowCount()):
            item = self._table.item(r, self._COL_PATH)
            if item and (item.data(self._ROLE_MERGE_ID) or "") == merge_id:
                return r == row
        return False

    def _merge_selected(self) -> None:
        """Fasst ausgewählte Zeilen zu einer Merge-Gruppe zusammen."""
        rows = sorted({idx.row() for idx in self._table.selectedIndexes()})
        if len(rows) < 2:
            return
        group_id = uuid.uuid4().hex[:8]
        for r in rows:
            item = self._table.item(r, self._COL_PATH)
            if item:
                item.setData(self._ROLE_MERGE_ID, group_id)
        self._refresh_merge_visuals()

    def _unmerge_selected(self) -> None:
        """Entfernt ausgewählte Zeilen aus ihrer Merge-Gruppe."""
        rows = sorted({idx.row() for idx in self._table.selectedIndexes()})
        for r in rows:
            item = self._table.item(r, self._COL_PATH)
            if item:
                item.setData(self._ROLE_MERGE_ID, "")
        self._refresh_merge_visuals()

    def _propagate_merge_title(self, merge_id: str, title: str, playlist: str) -> None:
        """Überträgt Titel+Playlist der ersten Datei an alle anderen in der Gruppe."""
        for r in range(self._table.rowCount()):
            item = self._table.item(r, self._COL_PATH)
            if item and (item.data(self._ROLE_MERGE_ID) or "") == merge_id:
                self._set_cell(r, self._COL_YT_TITLE, title)
                self._set_cell(r, self._COL_PLAYLIST,  playlist)

    def _refresh_merge_visuals(self) -> None:
        """Aktualisiert Farben und Editierbarkeit aller Zeilen gemäß Merge-Gruppen."""
        # Sammle alle Gruppen-IDs in Reihenfolge des ersten Auftretens
        group_order: list[str] = []
        for r in range(self._table.rowCount()):
            item = self._table.item(r, self._COL_PATH)
            gid = (item.data(self._ROLE_MERGE_ID) or "") if item else ""
            if gid and gid not in group_order:
                group_order.append(gid)

        seen_groups: set[str] = set()
        for r in range(self._table.rowCount()):
            path_item = self._table.item(r, self._COL_PATH)
            gid = (path_item.data(self._ROLE_MERGE_ID) or "") if path_item else ""
            is_first = gid not in seen_groups
            if gid:
                seen_groups.add(gid)

            color_idx = group_order.index(gid) % len(self._MERGE_COLORS) if gid else -1
            bg = self._MERGE_COLORS[color_idx] if gid else None

            for col in range(self._table.columnCount() - 1):  # skip button column
                cell = self._table.item(r, col)
                if cell is None:
                    continue
                # Farbe setzen
                if bg:
                    cell.setBackground(bg)
                else:
                    cell.setBackground(QColor(0, 0, 0, 0))
                # Editierbarkeit: nicht-erste Dateien einer Gruppe sperren
                if gid and not is_first and col in (self._COL_YT_TITLE, self._COL_PLAYLIST):
                    cell.setFlags(cell.flags() & ~Qt.ItemIsEditable)
                    cell.setForeground(QColor("#888888"))
                    cell.setToolTip("🔒 Wird von der ersten Datei der Merge-Gruppe gesteuert")
                elif col not in (self._COL_PATH, self._COL_SIZE):
                    cell.setFlags(cell.flags() | Qt.ItemIsEditable)
                    cell.setForeground(QColor())  # Standardfarbe

    def _bulk_edit(self) -> None:
        """Setzt Spieldaten für alle Zeilen; Teil-Nummer wird pro Zeile erhöht."""
        from .youtube_title_editor import (
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
            stem = Path(p).stem
            self._append_row(FileEntry(
                source_path=p,
                youtube_title=stem,
            ))

    def eventFilter(self, obj, event) -> bool:
        if obj is self._table and event.type() == QEvent.KeyPress:
            if event.key() in (Qt.Key_Delete, Qt.Key_Backspace):
                self._remove_selected_rows()
                return True
        return super().eventFilter(obj, event)

    def _remove_selected_rows(self) -> None:
        rows = sorted(
            {idx.row() for idx in self._table.selectedIndexes()},
            reverse=True)
        for row in rows:
            self._table.removeRow(row)

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
