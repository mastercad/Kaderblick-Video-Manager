"""Tests für Merge-/Unmerge-Verhalten im FileListWidget."""

import sys
from unittest.mock import patch

from PySide6.QtWidgets import QApplication, QAbstractItemView, QTableWidgetSelectionRange, QMessageBox

_app = QApplication.instance() or QApplication(sys.argv)

from src.file_list_widget import FileListWidget
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
    def test_table_uses_multi_selection_for_merge_workflow(self):
        widget = _widget()
        assert widget._table.selectionMode() == QAbstractItemView.MultiSelection


class TestFileListWidgetMerge:
    def test_merge_selected_assigns_same_merge_id_to_multiple_rows(self):
        widget = _widget()
        try:
            widget._table.setRangeSelected(QTableWidgetSelectionRange(0, 0, 0, 6), True)
            widget._table.setRangeSelected(QTableWidgetSelectionRange(1, 0, 1, 6), True)

            with patch("src.file_list_widget.QMessageBox.question", return_value=QMessageBox.Ok):
                with patch("src.file_list_widget.QMessageBox.information"):
                    widget._merge_selected()

            gid0 = widget._table.item(0, widget._COL_PATH).data(widget._ROLE_MERGE_ID)
            gid1 = widget._table.item(1, widget._COL_PATH).data(widget._ROLE_MERGE_ID)
            gid2 = widget._table.item(2, widget._COL_PATH).data(widget._ROLE_MERGE_ID)
            assert gid0
            assert gid0 == gid1
            assert gid2 == ""
        finally:
            widget.close()

    def test_unmerge_selected_clears_merge_id_again(self):
        widget = _widget()
        try:
            widget._table.setRangeSelected(QTableWidgetSelectionRange(0, 0, 0, 6), True)
            widget._table.setRangeSelected(QTableWidgetSelectionRange(1, 0, 1, 6), True)
            with patch("src.file_list_widget.QMessageBox.question", return_value=QMessageBox.Ok):
                with patch("src.file_list_widget.QMessageBox.information"):
                    widget._merge_selected()

            widget._table.clearSelection()
            widget._table.setRangeSelected(QTableWidgetSelectionRange(0, 0, 0, 6), True)
            widget._table.setRangeSelected(QTableWidgetSelectionRange(1, 0, 1, 6), True)
            with patch("src.file_list_widget.QMessageBox.question", return_value=QMessageBox.Ok):
                with patch("src.file_list_widget.QMessageBox.information"):
                    widget._unmerge_selected()

            assert widget._table.item(0, widget._COL_PATH).data(widget._ROLE_MERGE_ID) == ""
            assert widget._table.item(1, widget._COL_PATH).data(widget._ROLE_MERGE_ID) == ""
        finally:
            widget.close()

    def test_collect_preserves_merge_group_ids(self):
        widget = _widget()
        try:
            widget._table.setRangeSelected(QTableWidgetSelectionRange(0, 0, 0, 6), True)
            widget._table.setRangeSelected(QTableWidgetSelectionRange(1, 0, 1, 6), True)
            with patch("src.file_list_widget.QMessageBox.question", return_value=QMessageBox.Ok):
                with patch("src.file_list_widget.QMessageBox.information"):
                    widget._merge_selected()

            entries = widget.collect()

            assert entries[0].merge_group_id
            assert entries[0].merge_group_id == entries[1].merge_group_id
            assert entries[2].merge_group_id == ""
        finally:
            widget.close()

    def test_merge_selected_shows_confirmation_and_success_dialog(self):
        widget = _widget()
        try:
            widget._table.setRangeSelected(QTableWidgetSelectionRange(0, 0, 0, 6), True)
            widget._table.setRangeSelected(QTableWidgetSelectionRange(1, 0, 1, 6), True)

            with patch("src.file_list_widget.QMessageBox.question", return_value=QMessageBox.Ok) as question:
                with patch("src.file_list_widget.QMessageBox.information") as info:
                    widget._merge_selected()

            question.assert_called_once()
            assert info.call_count == 1
            assert "OK" in info.call_args.args[2]
        finally:
            widget.close()

    def test_unmerge_selected_shows_confirmation_and_success_dialog(self):
        widget = _widget()
        try:
            gid = "gruppe1"
            widget._table.item(0, widget._COL_PATH).setData(widget._ROLE_MERGE_ID, gid)
            widget._table.item(1, widget._COL_PATH).setData(widget._ROLE_MERGE_ID, gid)
            widget._table.setRangeSelected(QTableWidgetSelectionRange(0, 0, 0, 6), True)
            widget._table.setRangeSelected(QTableWidgetSelectionRange(1, 0, 1, 6), True)

            with patch("src.file_list_widget.QMessageBox.question", return_value=QMessageBox.Ok) as question:
                with patch("src.file_list_widget.QMessageBox.information") as info:
                    widget._unmerge_selected()

            question.assert_called_once()
            assert info.call_count == 1
            assert "OK" in info.call_args.args[2]
        finally:
            widget.close()


class TestFileListWidgetRemove:
    def test_remove_selected_rows_requires_confirmation(self):
        widget = _widget()
        try:
            widget._table.setRangeSelected(QTableWidgetSelectionRange(0, 0, 1, 6), True)

            with patch("src.file_list_widget.QMessageBox.question", return_value=QMessageBox.No):
                widget._remove_selected_rows()

            assert widget._table.rowCount() == 3
        finally:
            widget.close()

    def test_remove_selected_rows_deletes_after_confirmation(self):
        widget = _widget()
        try:
            widget._table.setRangeSelected(QTableWidgetSelectionRange(0, 0, 1, 6), True)

            with patch("src.file_list_widget.QMessageBox.question", return_value=QMessageBox.Yes):
                widget._remove_selected_rows()

            assert widget._table.rowCount() == 1
        finally:
            widget.close()