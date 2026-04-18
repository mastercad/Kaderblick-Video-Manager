from __future__ import annotations

from PySide6.QtCore import QEvent, QObject, Qt
from PySide6.QtWidgets import QAbstractSpinBox, QComboBox, QWidget


class _FocusOnlyWheelGuard(QObject):
    def eventFilter(self, watched, event) -> bool:
        if event.type() == QEvent.Type.Wheel and isinstance(watched, (QComboBox, QAbstractSpinBox)):
            if self._allows_wheel_change(watched):
                return False
            event.ignore()
            return True
        return super().eventFilter(watched, event)

    @staticmethod
    def _allows_wheel_change(widget: QComboBox | QAbstractSpinBox) -> bool:
        if isinstance(widget, QComboBox):
            line_edit = widget.lineEdit()
            return widget.view().isVisible() or widget.hasFocus() or (line_edit is not None and line_edit.hasFocus())
        return widget.hasFocus()


def install_focus_only_wheel_guard(root: QWidget) -> None:
    guard = _FocusOnlyWheelGuard(root)
    setattr(root, "_focus_only_wheel_guard", guard)

    widgets: list[QWidget] = list(root.findChildren(QComboBox))
    widgets.extend(widget for widget in root.findChildren(QAbstractSpinBox) if widget not in widgets)
    for widget in widgets:
        widget.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        widget.installEventFilter(guard)