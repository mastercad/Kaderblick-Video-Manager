"""Tests für Dateiliste und Merge-Zustand im FileListWidget."""

import sys
from unittest.mock import patch

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QAbstractItemView, QHeaderView, QTableWidgetSelectionRange, QMessageBox, QPushButton

_app = QApplication.instance() or QApplication(sys.argv)

from src.ui.file_list_widget import FileListWidget
from src.workflow import FileEntry


def _widget() -> FileListWidget:
    widget = FileListWidget(lambda: "", lambda _d: None)
    widget.load([
        FileEntry(source_path="/tmp/teil1.mp4", youtube_title="Teil 1"),
        FileEntry(source_path="/tmp/teil2.mp4", youtube_title="Teil 2"),
        FileEntry(source_path="/tmp/teil3.mp4", youtube_title="Teil 3"),
    ])
    return widget


class TestFileListWidgetSelection:
    def test_table_defaults_to_compact_height(self):
        widget = _widget()
        try:
            expected = (
                widget._table.horizontalHeader().height()
                + (widget._MIN_VISIBLE_ROWS * widget._ROW_HEIGHT)
                + (widget._table.frameWidth() * 2)
            )
            assert widget._table.minimumHeight() == expected
            assert widget._table.maximumHeight() == expected
        finally:
            widget.close()

    def test_table_height_stays_capped_at_five_visible_rows(self):
        widget = FileListWidget(lambda: "", lambda _d: None)
        try:
            widget.load([
                FileEntry(source_path=f"/tmp/teil{i}.mp4", youtube_title=f"Teil {i}")
                for i in range(1, 10)
            ])

            expected = (
                widget._table.horizontalHeader().height()
                + (widget._MAX_VISIBLE_ROWS * widget._ROW_HEIGHT)
                + (widget._table.frameWidth() * 2)
            )
            assert widget._table.minimumHeight() == expected
            assert widget._table.maximumHeight() == expected
        finally:
            widget.close()

    def test_table_uses_multi_selection_for_bulk_row_actions(self):
        widget = _widget()
        assert widget._table.selectionMode() == QAbstractItemView.MultiSelection

    def test_content_columns_start_with_sensible_resizable_widths(self):
        widget = _widget()
        try:
            header = widget._table.horizontalHeader()

            assert header.sectionResizeMode(widget._COL_PATH) == QHeaderView.Interactive
            assert header.sectionResizeMode(widget._COL_OUTNAME) == QHeaderView.Interactive
            assert header.sectionResizeMode(widget._COL_YT_TITLE) == QHeaderView.Interactive
            assert header.sectionResizeMode(widget._COL_PLAYLIST) == QHeaderView.Interactive
            assert widget._table.columnWidth(widget._COL_PATH) >= widget._COL_MIN_WIDTHS[widget._COL_PATH]
            assert widget._table.columnWidth(widget._COL_YT_TITLE) >= widget._COL_MIN_WIDTHS[widget._COL_YT_TITLE]
        finally:
            widget.close()

    def test_uses_persisted_source_size_when_path_is_not_local_file(self):
        widget = FileListWidget(lambda: "", lambda _d: None)
        try:
            widget.load([
                FileEntry(
                    source_path="/does/not/exist/halbzeit1.mjpg",
                    source_size_bytes=1_610_612_736,
                    youtube_title="Teil 1",
                )
            ])

            assert widget._table.item(0, widget._COL_SIZE).text() == "1.5 GB"
            assert widget.collect()[0].source_size_bytes == 1_610_612_736
        finally:
            widget.close()

    def test_per_row_editor_button_is_not_exposed(self):
        widget = _widget()
        try:
            button_texts = {btn.text() for btn in widget.findChildren(QPushButton)}

            assert "🎬" not in button_texts
            assert "🔗 Zusammenführen" not in button_texts
            assert "🔓 Auflösen" not in button_texts
        finally:
            widget.close()

    def test_add_files_dialog_uses_last_directory_and_updates_it(self):
        remembered_dirs = []
        widget = FileListWidget(lambda: "/media/video/spieltag-23", remembered_dirs.append)
        try:
            with patch(
                "src.ui.file_list_widget.QFileDialog.getOpenFileNames",
                return_value=(["/media/video/spieltag-23/halbzeit1.mp4"], "Videodateien"),
            ) as picker:
                widget._open_add_files_dialog()

            picker.assert_called_once()
            assert picker.call_args.args[2] == "/media/video/spieltag-23"
            assert remembered_dirs == ["/media/video/spieltag-23"]
            assert widget.collect()[0].youtube_title == ""
        finally:
            widget.close()


class TestFileListWidgetMergeState:
    def test_collect_preserves_existing_merge_group_ids(self):
        widget = _widget()
        try:
            gid = "gruppe1"
            widget._table.item(0, widget._COL_PATH).setData(widget._ROLE_MERGE_ID, gid)
            widget._table.item(1, widget._COL_PATH).setData(widget._ROLE_MERGE_ID, gid)

            entries = widget.collect()

            assert entries[0].merge_group_id == gid
            assert entries[1].merge_group_id == gid
            assert entries[2].merge_group_id == ""
        finally:
            widget.close()

    def test_refresh_merge_visuals_keeps_follow_up_rows_editable(self):
        widget = _widget()
        try:
            gid = "gruppe1"
            widget._table.item(0, widget._COL_PATH).setData(widget._ROLE_MERGE_ID, gid)
            widget._table.item(1, widget._COL_PATH).setData(widget._ROLE_MERGE_ID, gid)
            widget._refresh_merge_visuals()

            assert widget._table.item(1, widget._COL_YT_TITLE).flags() & Qt.ItemIsEditable
        finally:
            widget.close()


class TestFileListWidgetRemove:
    def test_remove_selected_rows_requires_confirmation(self):
        widget = _widget()
        try:
            widget._table.setRangeSelected(QTableWidgetSelectionRange(0, 0, 1, 5), True)

            with patch("src.ui.file_list_widget.QMessageBox.question", return_value=QMessageBox.No):
                widget._remove_selected_rows()

            assert widget._table.rowCount() == 3
        finally:
            widget.close()

    def test_remove_selected_rows_deletes_after_confirmation(self):
        widget = _widget()
        try:
            widget._table.setRangeSelected(QTableWidgetSelectionRange(0, 0, 1, 5), True)

            with patch("src.ui.file_list_widget.QMessageBox.question", return_value=QMessageBox.Yes):
                widget._remove_selected_rows()

            assert widget._table.rowCount() == 1
        finally:
            widget.close()