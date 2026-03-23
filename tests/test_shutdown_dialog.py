"""Tests für ShutdownCountdownDialog (dialogs.py)."""

import sys
import pytest

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt

_app = QApplication.instance() or QApplication(sys.argv)

from src.ui.dialogs import ShutdownCountdownDialog


class TestShutdownCountdownDialog:
    def test_initial_remaining(self):
        dlg = ShutdownCountdownDialog(seconds=10)
        assert dlg._remaining == 10
        assert dlg._total == 10

    def test_tick_decrements(self):
        dlg = ShutdownCountdownDialog(seconds=5)
        dlg._tick()
        assert dlg._remaining == 4

    def test_tick_to_zero_calls_accept(self):
        dlg = ShutdownCountdownDialog(seconds=1)
        dlg._timer.stop()  # prevent auto-fire during test
        accepted = []
        dlg.accepted.connect(lambda: accepted.append(1))
        dlg._tick()  # remaining goes to 0 → accept()
        assert accepted  # signal was emitted

    def test_cancel_calls_reject(self):
        dlg = ShutdownCountdownDialog(seconds=30)
        dlg._timer.stop()
        rejected = []
        dlg.rejected.connect(lambda: rejected.append(1))
        dlg._cancel_btn.click()
        assert rejected  # signal was emitted

    def test_timer_stops_on_reject(self):
        dlg = ShutdownCountdownDialog(seconds=30)
        assert dlg._timer.isActive()
        dlg.reject()
        assert not dlg._timer.isActive()

    def test_timer_stops_on_accept(self):
        dlg = ShutdownCountdownDialog(seconds=1)
        dlg._timer.stop()  # stop auto-fire
        dlg._tick()        # manually triggers accept
        assert not dlg._timer.isActive()

    def test_progress_bar_value_matches_remaining(self):
        dlg = ShutdownCountdownDialog(seconds=10)
        dlg._timer.stop()
        dlg._tick()
        assert dlg._progress.value() == 9

    def test_label_contains_remaining(self):
        dlg = ShutdownCountdownDialog(seconds=15)
        assert "15" in dlg._label.text()
        dlg._timer.stop()
        dlg._tick()
        assert "14" in dlg._label.text()
