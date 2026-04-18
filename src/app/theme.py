"""Application theming helpers for Kaderblick branding."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from PySide6.QtCore import QSize, Qt
from PySide6.QtGui import QColor, QFont, QFontDatabase, QFontMetrics, QPainter, QPalette
from PySide6.QtWidgets import QApplication, QWidget


_PRIMARY_GREEN = "#06B62E"
_PRIMARY_GREEN_DARK = "#059B28"
_PRIMARY_GREEN_SOFT = "#E9F8EC"
_SURFACE = "#FFFFFF"
_SURFACE_ALT = "#F7FAF7"
_APP_BG = "#F3F5F2"
_BORDER = "#D6E3D6"
_TEXT = "#18212B"
_MUTED = "#667582"
_SHADOW = "rgba(8, 32, 16, 0.08)"
_BRAND_FONT_FALLBACKS = ["Impact", "Arial Black", "Sans Serif"]
_UI_FONT_CANDIDATES = ["Roboto Flex", "Inter", "Montserrat", "Helvetica Neue", "Arial"]
_THEME_APPLIED_PROPERTY = "_kaderblickThemeApplied"
_BRAND_K_SIZE_DELTA = 1.7

_loaded_brand_family: str | None = None


class BrandWordmarkWidget(QWidget):
    def __init__(self, text: str = "KADERBLICK", parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._text = text
        self._first = text[:1]
        self._rest = text[1:]
        self.setObjectName("brandWordmark")
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setFont(brand_wordmark_font())

    def sizeHint(self) -> QSize:
        first_font = QFont(self.font())
        first_font.setPointSizeF(first_font.pointSizeF() + _BRAND_K_SIZE_DELTA)
        first_metrics = QFontMetrics(first_font)
        rest_metrics = QFontMetrics(self.font())
        width = first_metrics.horizontalAdvance(self._first) + rest_metrics.horizontalAdvance(self._rest)
        height = max(first_metrics.height(), rest_metrics.height())
        return QSize(width, height)

    def minimumSizeHint(self) -> QSize:
        return self.sizeHint()

    def paintEvent(self, _event) -> None:
        first_font = QFont(self.font())
        first_font.setPointSizeF(first_font.pointSizeF() + _BRAND_K_SIZE_DELTA)
        first_metrics = QFontMetrics(first_font)
        rest_metrics = QFontMetrics(self.font())
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.TextAntialiasing, True)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)

        top = (self.height() - max(first_metrics.height(), rest_metrics.height())) // 2
        first_baseline_y = top + first_metrics.ascent()
        rest_baseline_y = top + rest_metrics.ascent()

        painter.setFont(first_font)
        painter.setPen(QColor("#018707"))
        painter.drawText(0, first_baseline_y, self._first)

        painter.setFont(self.font())
        painter.setPen(QColor("#FFFFFF"))
        painter.drawText(first_metrics.horizontalAdvance(self._first), rest_baseline_y, self._rest)
        painter.end()


def apply_application_theme(window: QWidget) -> None:
    app = cast(QApplication | None, QApplication.instance())
    if app is None:
        return

    _ensure_brand_font_loaded()
    if not bool(app.property(_THEME_APPLIED_PROPERTY)):
        app.setFont(_default_ui_font())
        app.setPalette(_build_palette(app.palette()))
        app.setStyleSheet(_build_stylesheet())
        app.setProperty(_THEME_APPLIED_PROPERTY, True)

    brand_wordmark = window.findChild(BrandWordmarkWidget, "brandWordmark")
    if brand_wordmark is not None:
        brand_wordmark.setFont(brand_wordmark_font())
        brand_wordmark.updateGeometry()
        brand_wordmark.update()


def brand_wordmark_font() -> QFont:
    family = _loaded_brand_family or next(iter(_BRAND_FONT_FALLBACKS), "Sans Serif")
    font = QFont(family)
    font.setPointSize(22)
    font.setWeight(QFont.Weight.Bold)
    font.setLetterSpacing(QFont.SpacingType.AbsoluteSpacing, 0.6)
    return font


def _default_ui_font() -> QFont:
    available = set(QFontDatabase.families())
    family = next((candidate for candidate in _UI_FONT_CANDIDATES if candidate in available), QApplication.font().family())
    font = QFont(family)
    font.setPointSize(10)
    return font


def _ensure_brand_font_loaded() -> None:
    global _loaded_brand_family
    if _loaded_brand_family is not None:
        return

    font_path = Path(__file__).resolve().parent.parent.parent / "assets" / "ImpactLTStd.woff2"
    if font_path.exists():
        font_id = QFontDatabase.addApplicationFont(str(font_path))
        if font_id >= 0:
            families = QFontDatabase.applicationFontFamilies(font_id)
            if families:
                _loaded_brand_family = families[0]
                return

    for fallback in _BRAND_FONT_FALLBACKS:
        if fallback in set(QFontDatabase.families()):
            _loaded_brand_family = fallback
            return
    _loaded_brand_family = QApplication.font().family()


def _build_palette(seed: QPalette) -> QPalette:
    palette = QPalette(seed)
    text = QColor(_TEXT)
    surface = QColor(_SURFACE)
    surface_alt = QColor(_SURFACE_ALT)
    app_bg = QColor(_APP_BG)
    selection = QColor("#D8F2DE")
    muted = QColor(_MUTED)

    for group in (QPalette.ColorGroup.Active, QPalette.ColorGroup.Inactive, QPalette.ColorGroup.Disabled):
        palette.setColor(group, QPalette.ColorRole.WindowText, text)
        palette.setColor(group, QPalette.ColorRole.Text, text)
        palette.setColor(group, QPalette.ColorRole.ButtonText, text)
        palette.setColor(group, QPalette.ColorRole.Base, surface)
        palette.setColor(group, QPalette.ColorRole.AlternateBase, surface_alt)
        palette.setColor(group, QPalette.ColorRole.Window, app_bg)
        palette.setColor(group, QPalette.ColorRole.Highlight, selection)
        palette.setColor(group, QPalette.ColorRole.HighlightedText, text)
        palette.setColor(group, QPalette.ColorRole.PlaceholderText, muted)

    return palette


def _build_stylesheet() -> str:
    return f"""
    QWidget {{
        color: {_TEXT};
        background: {_APP_BG};
    }}

    QLabel {{
        background: transparent;
    }}

    QMainWindow, QDialog {{
        background: {_APP_BG};
    }}

    QMenuBar {{
        background: {_PRIMARY_GREEN};
        color: white;
        border: none;
        padding: 4px 12px;
        font-weight: 600;
    }}

    QMenuBar::item {{
        background: transparent;
        padding: 8px 12px;
        margin: 2px 4px;
        border-radius: 14px;
    }}

    QMenuBar::item:selected {{
        background: rgba(255, 255, 255, 0.16);
    }}

    QMenu {{
        background: {_SURFACE};
        border: 1px solid {_BORDER};
        border-radius: 12px;
        padding: 6px;
    }}

    QMenu::item {{
        padding: 8px 12px;
        border-radius: 8px;
    }}

    QMenu::item:selected {{
        background: {_PRIMARY_GREEN_SOFT};
        color: {_PRIMARY_GREEN_DARK};
    }}

    QToolBar {{
        background: {_PRIMARY_GREEN};
        border: none;
        spacing: 6px;
        padding: 10px 14px;
    }}

    QToolBar::separator {{
        background: rgba(255, 255, 255, 0.18);
        width: 1px;
        margin: 6px 10px;
    }}

    QToolBar QToolButton {{
        background: transparent;
        color: white;
        border: 1px solid transparent;
        border-radius: 16px;
        padding: 8px 12px;
        font-weight: 600;
    }}

    QToolBar QToolButton:hover {{
        background: rgba(255, 255, 255, 0.14);
    }}

    QToolBar QToolButton:pressed {{
        background: rgba(0, 0, 0, 0.12);
    }}

    QToolBar QToolButton#qt_toolbar_ext_button {{
        background: rgba(255, 255, 255, 0.18);
        border: 1px solid rgba(255, 255, 255, 0.28);
        min-width: 28px;
        padding: 8px;
    }}

    QToolBar QToolButton#qt_toolbar_ext_button:hover {{
        background: rgba(255, 255, 255, 0.28);
    }}

    QToolBar QCheckBox {{
        color: white;
        background: rgba(255, 255, 255, 0.12);
        border: 1px solid rgba(255, 255, 255, 0.2);
        border-radius: 16px;
        padding: 6px 12px;
        font-weight: 600;
        spacing: 8px;
        margin-left: 8px;
    }}

    QToolBar QCheckBox::indicator {{
        width: 18px;
        height: 18px;
        border-radius: 9px;
        border: 1px solid rgba(255, 255, 255, 0.65);
        background: rgba(255, 255, 255, 0.12);
    }}

    QToolBar QCheckBox::indicator:checked {{
        background: {_PRIMARY_GREEN_DARK};
        border-color: white;
    }}

    QWidget#brandWordmark {{
        background: transparent;
    }}

    QStatusBar {{
        background: {_SURFACE};
        border-top: 1px solid {_BORDER};
    }}

    QStatusBar QLabel {{
        background: transparent;
    }}

    QProgressBar {{
        background: #ECF2ED;
        border: none;
        border-radius: 9px;
        min-height: 18px;
        text-align: center;
        color: {_TEXT};
        font-weight: 600;
    }}

    QProgressBar::chunk {{
        background: {_PRIMARY_GREEN};
        border-radius: 9px;
    }}

    QTableWidget,
    QTextEdit,
    QPlainTextEdit,
    QListWidget,
    QTreeWidget,
    QFrame#cardSurface,
    QGroupBox,
    QLineEdit,
    QComboBox,
    QSpinBox,
    QDoubleSpinBox,
    QDateEdit,
    QAbstractSpinBox,
    QScrollArea,
    QTabWidget::pane {{
        background: {_SURFACE};
        border: 1px solid {_BORDER};
        border-radius: 16px;
    }}

    QTableWidget {{
        alternate-background-color: {_SURFACE_ALT};
        gridline-color: #E8EFE8;
        selection-background-color: #D8F2DE;
        selection-color: {_TEXT};
        padding: 4px;
    }}

    QHeaderView::section {{
        background: #EFF7F0;
        color: {_TEXT};
        border: none;
        border-bottom: 1px solid {_BORDER};
        padding: 10px 12px;
        font-weight: 700;
    }}

    QTextEdit,
    QPlainTextEdit {{
        padding: 8px;
    }}

    QLineEdit,
    QComboBox,
    QSpinBox,
    QDoubleSpinBox,
    QDateEdit,
    QAbstractSpinBox,
    QTextEdit,
    QPlainTextEdit {{
        padding: 8px 10px;
        selection-background-color: #D8F2DE;
        selection-color: {_TEXT};
        color: {_TEXT};
    }}

    /* Inline-Editoren in Views duerfen keine Formular-Paddings bekommen,
       sonst wird der Text in flachen Zeilen abgeschnitten. */
    QAbstractItemView QLineEdit,
    QTableView QLineEdit,
    QTreeView QLineEdit,
    QListView QLineEdit {{
        padding: 0 2px;
        margin: 0;
        border-radius: 0;
        min-height: 0px;
    }}

    QComboBox::drop-down,
    QDateEdit::drop-down {{
        border: none;
        width: 24px;
    }}

    QPushButton {{
        background: {_PRIMARY_GREEN};
        color: white;
        border: none;
        border-radius: 12px;
        padding: 8px 14px;
        font-weight: 700;
    }}

    QPushButton:hover {{
        background: {_PRIMARY_GREEN_DARK};
    }}

    QPushButton:disabled {{
        background: #B9C9BA;
        color: #F3F7F3;
    }}

    QPushButton[flat="true"] {{
        background: transparent;
        color: {_PRIMARY_GREEN_DARK};
    }}

    QPushButton[flat="true"]:hover {{
        background: {_PRIMARY_GREEN_SOFT};
    }}

    QGroupBox {{
        margin-top: 12px;
        padding-top: 10px;
        font-weight: 700;
    }}

    QGroupBox::title {{
        subcontrol-origin: margin;
        left: 14px;
        padding: 0 6px;
        color: {_TEXT};
    }}

    QCheckBox {{
        spacing: 8px;
    }}

    QCheckBox::indicator {{
        width: 18px;
        height: 18px;
        border-radius: 9px;
        border: 1px solid #A8C9AF;
        background: white;
    }}

    QCheckBox::indicator:checked {{
        background: {_PRIMARY_GREEN};
        border-color: {_PRIMARY_GREEN};
    }}

    QSplitter::handle {{
        background: transparent;
    }}

    QScrollBar:vertical {{
        background: transparent;
        width: 12px;
        margin: 6px 2px 6px 2px;
    }}

    QScrollBar::handle:vertical {{
        background: #B9D8BE;
        min-height: 28px;
        border-radius: 6px;
    }}

    QScrollBar::handle:vertical:hover {{
        background: #9BC8A3;
    }}

    QScrollBar::add-line:vertical,
    QScrollBar::sub-line:vertical,
    QScrollBar::add-page:vertical,
    QScrollBar::sub-page:vertical {{
        background: transparent;
        border: none;
        height: 0px;
    }}
    """