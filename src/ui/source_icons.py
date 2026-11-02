"""Native icons for workflow source modes.

The table must not encode icons as emoji in its text: glyph availability then
depends on the selected UI font. Theme icons keep text and presentation
separate, while Qt standard icons provide a guaranteed fallback.
"""

from __future__ import annotations

from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication, QStyle, QWidget


_SOURCE_ICONS = {
    "files": ("document-multiple", QStyle.StandardPixmap.SP_FileIcon),
    "folder_scan": ("folder", QStyle.StandardPixmap.SP_DirIcon),
    "pi_download": ("camera-photo", QStyle.StandardPixmap.SP_ComputerIcon),
}


def source_mode_icon(source_mode: str, widget: QWidget | None = None) -> QIcon:
    """Return a native source icon with a non-empty Qt fallback."""
    theme_name, standard_icon = _SOURCE_ICONS.get(
        source_mode,
        ("dialog-question", QStyle.StandardPixmap.SP_MessageBoxQuestion),
    )
    style = widget.style() if widget is not None else QApplication.style()
    fallback = style.standardIcon(standard_icon)
    return QIcon.fromTheme(theme_name, fallback)
