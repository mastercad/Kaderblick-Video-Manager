"""User interface package."""

from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QLineEdit


class AlwaysVisiblePlaceholderLineEdit(QLineEdit):
	effectiveTextChanged = Signal(str)

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self._full_placeholder_text = ""
		self._showing_placeholder = False
		self._suppress_effective_signal = False
		self._default_palette = QPalette(self.palette())
		self._placeholder_palette = QPalette(self._default_palette)
		self._placeholder_palette.setColor(QPalette.ColorRole.Text, QColor("#7A8697"))
		self.textChanged.connect(self._emit_effective_text)

	def _emit_effective_text(self, _text: str) -> None:
		if self._suppress_effective_signal or self._showing_placeholder:
			return
		self.effectiveTextChanged.emit(super().text())

	def _set_display_text(self, text: str, *, suppress_signal: bool) -> None:
		previous = self._suppress_effective_signal
		self._suppress_effective_signal = suppress_signal
		try:
			super().setText(text)
		finally:
			self._suppress_effective_signal = previous

	def _apply_placeholder_style(self, enabled: bool) -> None:
		self.setPalette(self._placeholder_palette if enabled else self._default_palette)

	def _show_placeholder_display(self) -> None:
		if not self._full_placeholder_text:
			self._showing_placeholder = False
			self._apply_placeholder_style(False)
			return
		self._showing_placeholder = True
		self._apply_placeholder_style(True)
		self._set_display_text(self._full_placeholder_text, suppress_signal=True)
		super().setCursorPosition(0)

	def _hide_placeholder_display(self) -> None:
		if not self._showing_placeholder:
			return
		self._showing_placeholder = False
		self._apply_placeholder_style(False)
		self._set_display_text("", suppress_signal=True)

	def showingPlaceholder(self) -> bool:
		return self._showing_placeholder

	def text(self) -> str:
		return "" if self._showing_placeholder else super().text()

	def clear(self) -> None:
		self.setText("")

	def setText(self, text: str) -> None:
		value = text or ""
		self._showing_placeholder = False
		self._apply_placeholder_style(False)
		self._set_display_text(value, suppress_signal=False)
		if not value:
			self._show_placeholder_display()

	def placeholderText(self) -> str:
		return self._full_placeholder_text

	def setPlaceholderText(self, text: str) -> None:
		self._full_placeholder_text = text or ""
		super().setPlaceholderText("")
		self.setToolTip(self._full_placeholder_text)
		if not self.text():
			self._show_placeholder_display()

	def focusInEvent(self, event) -> None:
		super().focusInEvent(event)

	def focusOutEvent(self, event) -> None:
		super().focusOutEvent(event)
		if not super().text():
			self._show_placeholder_display()

	def keyPressEvent(self, event) -> None:
		if self._showing_placeholder and event.text():
			self._hide_placeholder_display()
		super().keyPressEvent(event)

	def insert(self, new_text: str) -> None:
		if self._showing_placeholder and new_text:
			self._hide_placeholder_display()
		super().insert(new_text)

	def showEvent(self, event) -> None:
		super().showEvent(event)
		if not self.text():
			self._show_placeholder_display()


__all__ = ["AlwaysVisiblePlaceholderLineEdit"]