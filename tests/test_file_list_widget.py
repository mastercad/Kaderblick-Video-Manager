"""Tests für Dateiliste und Merge-Zustand im FileListWidget."""

import sys
from unittest.mock import patch

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QAbstractItemView, QTableWidgetSelectionRange, QMessageBox, QPushButton

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
    def test_table_uses_multi_selection_for_bulk_row_actions(self):
        widget = _widget()
        assert widget._table.selectionMode() == QAbstractItemView.MultiSelection

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