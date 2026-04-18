"""User interface package."""

from __future__ import annotations

from datetime import date
import re

from PySide6.QtCore import QDate, Signal
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QCalendarWidget, QHBoxLayout, QLineEdit, QMenu, QToolButton, QWidget, QWidgetAction


_DDMMYYYY_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")
_YYYYMMDD_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")


def normalize_date_text(raw: str) -> str:
	value = (raw or "").strip()
	if not value:
		return ""
	match = _DDMMYYYY_RE.fullmatch(value)
	if match:
		day, month, year = match.groups()
		return f"{year}-{month}-{day}"
	match = _YYYYMMDD_RE.fullmatch(value)
	if match:
		return value
	return ""


def format_display_date(date_iso: str) -> str:
	normalized = normalize_date_text(date_iso)
	if not normalized:
		return ""
	try:
		parsed = date.fromisoformat(normalized)
	except ValueError:
		return ""
	return parsed.strftime("%d.%m.%Y")


class AlwaysVisiblePlaceholderLineEdit(QLineEdit):
	effectiveTextChanged = Signal(str)

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self._suppress_effective_signal = False
		self._default_palette = QPalette(self.palette())
		self._placeholder_palette = QPalette(self._default_palette)
		self._placeholder_palette.setColor(QPalette.ColorRole.PlaceholderText, QColor("#5F6773"))
		self.setPalette(self._placeholder_palette)
		self.textChanged.connect(self._emit_effective_text)

	def _emit_effective_text(self, text: str) -> None:
		if self._suppress_effective_signal:
			return
		self.effectiveTextChanged.emit(text)

	def showingPlaceholder(self) -> bool:
		return not bool(super().text()) and bool(super().placeholderText())

	def setPlaceholderText(self, text: str) -> None:
		value = text or ""
		super().setPlaceholderText(value)
		self.setToolTip(value)

	def setText(self, text: str) -> None:
		previous = self._suppress_effective_signal
		self._suppress_effective_signal = False
		try:
			super().setText(text or "")
		finally:
			self._suppress_effective_signal = previous


class ClearableDateField(QWidget):
	effectiveTextChanged = Signal(str)

	def __init__(self, parent=None, *, placeholder: str = "TT.MM.JJJJ"):
		super().__init__(parent)
		layout = QHBoxLayout(self)
		layout.setContentsMargins(0, 0, 0, 0)
		layout.setSpacing(6)

		self._edit = AlwaysVisiblePlaceholderLineEdit(self)
		self._edit.setPlaceholderText(placeholder)
		self._edit.effectiveTextChanged.connect(self.effectiveTextChanged.emit)
		layout.addWidget(self._edit, 1)

		self._picker_button = QToolButton(self)
		self._picker_button.setText("...")
		self._picker_button.setToolTip("Datum auswaehlen")
		self._picker_button.setAutoRaise(False)
		self._picker_button.clicked.connect(self._open_calendar_popup)
		layout.addWidget(self._picker_button)

		self._calendar = QCalendarWidget(self)
		self._calendar.setGridVisible(True)
		self._calendar.clicked.connect(self._on_calendar_date_selected)

		self._calendar_menu = QMenu(self)
		calendar_action = QWidgetAction(self._calendar_menu)
		calendar_action.setDefaultWidget(self._calendar)
		self._calendar_menu.addAction(calendar_action)

	def text(self) -> str:
		return self._edit.text()

	def setText(self, text: str) -> None:
		self._edit.setText(format_display_date(text) or (text or ""))

	def placeholderText(self) -> str:
		return self._edit.placeholderText()

	def setPlaceholderText(self, text: str) -> None:
		self._edit.setPlaceholderText(format_display_date(text) or (text or ""))

	def setStyleSheet(self, style: str) -> None:
		self._edit.setStyleSheet(style)

	def isoValue(self) -> str:
		return normalize_date_text(self._edit.text())

	def _open_calendar_popup(self) -> None:
		selected = self._selected_qdate()
		if selected is not None:
			self._calendar.setSelectedDate(selected)
		else:
			self._calendar.setSelectedDate(QDate.currentDate())
		self._calendar_menu.popup(self._picker_button.mapToGlobal(self._picker_button.rect().bottomLeft()))

	def _on_calendar_date_selected(self, selected: QDate) -> None:
		self._edit.setText(selected.toString("dd.MM.yyyy"))
		self._calendar_menu.hide()

	def _selected_qdate(self) -> QDate | None:
		normalized = self.isoValue() or normalize_date_text(self._edit.placeholderText())
		if not normalized:
			return None
		try:
			parsed = date.fromisoformat(normalized)
		except ValueError:
			return None
		return QDate(parsed.year, parsed.month, parsed.day)


__all__ = ["AlwaysVisiblePlaceholderLineEdit", "ClearableDateField", "format_display_date", "normalize_date_text"]