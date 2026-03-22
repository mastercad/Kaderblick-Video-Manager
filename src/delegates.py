"""Tabellen-Delegate für grafischen Fortschritt in Tabellenzellen."""

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QStyledItemDelegate, QApplication, QStyle,
)


class ProgressDelegate(QStyledItemDelegate):
    """Zeichnet einen Fortschrittsbalken als Hintergrund in die Status-Zelle,
    wenn ein Job den Status 'Läuft' hat."""

    _BAR_COLOR = QColor(41, 128, 185, 190)       # kräftiges Blau (gut sichtbar)
    _BAR_DONE_COLOR = QColor(39, 174, 96, 190)  # kräftiges Grün

    def __init__(self, parent=None, *, progress_role=int(Qt.ItemDataRole.UserRole)):
        super().__init__(parent)
        self._progress_role = progress_role

    def paint(self, painter, option, index):
        self.initStyleOption(option, index)
        text = index.data(Qt.DisplayRole) or ""
        pct = index.data(self._progress_role)

        if pct is not None and isinstance(pct, int) and pct > 0:
            # Standardhintergrund (Auswahl, Alternating Rows etc.)
            style = (option.widget.style() if option.widget
                     else QApplication.style())
            style.drawPrimitive(
                QStyle.PE_PanelItemViewItem, option, painter, option.widget)

            # Fortschrittsbalken zeichnen
            rect = option.rect
            fill_width = int(rect.width() * min(pct, 100) / 100)
            bar_rect = rect.adjusted(0, 1, 0, -1)
            bar_rect.setWidth(fill_width)

            color = self._BAR_DONE_COLOR if pct >= 100 else self._BAR_COLOR
            painter.fillRect(bar_rect, color)

            # Text darüber zeichnen
            painter.save()
            if "Fertig" in text or "Übersprungen" in text:
                brush = index.data(Qt.ForegroundRole)
                painter.setPen(brush.color() if brush else Qt.black)
            elif "Fehler" in text:
                painter.setPen(QColor("#c0392b"))
            else:
                painter.setPen(QColor("#0a1628"))   # fast Schwarz – lesbar auf jedem Hintergrund
            painter.drawText(
                rect.adjusted(4, 0, -4, 0),
                Qt.AlignVCenter | Qt.AlignLeft,
                text,
            )
            painter.restore()
        else:
            # Kein Fortschritt → normales Rendering
            super().paint(painter, option, index)
