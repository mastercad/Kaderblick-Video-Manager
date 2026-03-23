"""Shutdown countdown dialog."""

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import QDialog, QHBoxLayout, QLabel, QProgressBar, QPushButton, QVBoxLayout


class ShutdownCountdownDialog(QDialog):
    def __init__(self, seconds: int = 30, parent=None):
        super().__init__(parent)
        self._remaining = seconds
        self._total = seconds

        self.setWindowTitle("Rechner herunterfahren")
        self.setWindowFlags(self.windowFlags() | Qt.WindowStaysOnTopHint)
        self.setMinimumWidth(380)

        layout = QVBoxLayout(self)
        self._label = QLabel()
        self._label.setAlignment(Qt.AlignCenter)
        self._label.setWordWrap(True)
        self._label.setStyleSheet("font-size: 15px; padding: 12px;")
        layout.addWidget(self._label)

        self._progress = QProgressBar()
        self._progress.setRange(0, seconds)
        self._progress.setValue(seconds)
        self._progress.setTextVisible(False)
        layout.addWidget(self._progress)

        btn_row = QHBoxLayout()
        self._cancel_btn = QPushButton("⛔  Abbrechen")
        self._cancel_btn.setMinimumHeight(36)
        self._cancel_btn.clicked.connect(self.reject)
        btn_row.addStretch()
        btn_row.addWidget(self._cancel_btn)
        layout.addLayout(btn_row)

        self._timer = QTimer(self)
        self._timer.setInterval(1_000)
        self._timer.timeout.connect(self._tick)
        self._timer.start()
        self._update_label()

    def _update_label(self):
        self._label.setText(
            f"⏻ Rechner wird in {self._remaining} Sekunde(n) heruntergefahren.\n\n"
            f"‚Abbrechen' drücken, um den Vorgang zu stoppen."
        )
        self._progress.setValue(self._remaining)

    def _tick(self):
        self._remaining -= 1
        if self._remaining <= 0:
            self._timer.stop()
            self.accept()
        else:
            self._update_label()

    def reject(self):
        self._timer.stop()
        super().reject()