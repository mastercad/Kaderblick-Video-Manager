from __future__ import annotations

from PySide6.QtCore import QMimeData, QPoint, QRectF, QSize, Qt
from PySide6.QtGui import QColor, QDrag, QPainter, QPixmap
from PySide6.QtWidgets import QListWidget, QListWidgetItem, QStyle, QStyledItemDelegate

from .defs import _NODE_DEFINITIONS, _node_visual_state, _paint_node_card


class _WorkflowNodePalette(QListWidget):
    MIME_TYPE = "application/x-kaderblick-video-manager-node"
    ROLE_KIND = Qt.ItemDataRole.UserRole + 50
    ROLE_NODE_TYPE = Qt.ItemDataRole.UserRole + 51
    KIND_HEADER = "header"
    KIND_NODE = "node"
    NODE_PREVIEW_SIZE = QSize(244, 100)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setDragEnabled(True)
        self.setSelectionMode(QListWidget.SelectionMode.SingleSelection)
        self.setStyleSheet("QListWidget { background: #FFFFFF; border: 1px solid #D7E0EA; border-radius: 10px; }")
        self.setItemDelegate(_WorkflowPaletteDelegate(self))
        self.setSpacing(4)
        self._populate()

    def _populate(self) -> None:
        current_category = None
        for node_type, definition in _NODE_DEFINITIONS.items():
            category = definition["category"]
            if category != current_category:
                header = QListWidgetItem(str(category))
                header.setFlags(Qt.ItemFlag.NoItemFlags)
                header.setData(self.ROLE_KIND, self.KIND_HEADER)
                self.addItem(header)
                current_category = category
            item = QListWidgetItem(str(definition["label"]))
            item.setData(self.ROLE_KIND, self.KIND_NODE)
            item.setData(self.ROLE_NODE_TYPE, node_type)
            item.setData(Qt.ItemDataRole.UserRole, node_type)
            item.setToolTip(str(definition["detail"]))
            self.addItem(item)

    def mimeData(self, items):
        mime = QMimeData()
        if items and items[0].data(Qt.ItemDataRole.UserRole):
            mime.setData(self.MIME_TYPE, str(items[0].data(Qt.ItemDataRole.UserRole)).encode("utf-8"))
        return mime

    def _create_drag_pixmap(self, node_type: str) -> QPixmap:
        pixmap = QPixmap(self.NODE_PREVIEW_SIZE)
        pixmap.fill(Qt.GlobalColor.transparent)

        painter = QPainter(pixmap)
        visual = _node_visual_state(node_type)
        _paint_node_card(
            painter,
            QRectF(6, 6, self.NODE_PREVIEW_SIZE.width() - 12, self.NODE_PREVIEW_SIZE.height() - 12),
            fill_color=visual["fill_color"],
            border_color=QColor("#2563EB"),
            title=visual["label"],
            detail=visual["detail"],
            state_text=visual["state_text"],
            state_color=visual["state_color"],
            progress_fill_color=visual["progress_fill_color"],
            progress_fraction=visual["progress_fraction"],
            has_input=visual["has_input"],
            has_output=visual["has_output"],
            output_branches=visual["output_branches"],
            port_radius=6,
        )
        painter.end()
        return pixmap

    def startDrag(self, supportedActions):
        item = self.currentItem()
        if item is None or item.data(self.ROLE_KIND) != self.KIND_NODE:
            return
        node_type = item.data(self.ROLE_NODE_TYPE)
        if not node_type:
            return

        drag = QDrag(self)
        drag.setMimeData(self.mimeData([item]))
        preview = self._create_drag_pixmap(str(node_type))
        drag.setPixmap(preview)
        drag.setHotSpot(QPoint(18, 18))
        drag.exec(supportedActions)


class _WorkflowPaletteDelegate(QStyledItemDelegate):
    PORT_RADIUS = 6

    def paint(self, painter, option, index):
        painter.save()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        kind = index.data(_WorkflowNodePalette.ROLE_KIND)
        rect = option.rect.adjusted(4, 2, -4, -2)

        if kind == _WorkflowNodePalette.KIND_HEADER:
            painter.setPen(QColor("#64748B"))
            painter.drawText(rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, str(index.data(Qt.ItemDataRole.DisplayRole)))
            painter.restore()
            return

        node_type = str(index.data(_WorkflowNodePalette.ROLE_NODE_TYPE))
        border_color = QColor("#2563EB") if option.state & QStyle.StateFlag.State_Selected else QColor("#CBD5E1")
        node_rect = rect.adjusted(6, 6, -6, -6)
        visual = _node_visual_state(node_type)
        _paint_node_card(
            painter,
            node_rect,
            fill_color=visual["fill_color"],
            border_color=border_color,
            title=visual["label"],
            detail=visual["detail"],
            state_text=visual["state_text"],
            state_color=visual["state_color"],
            progress_fill_color=visual["progress_fill_color"],
            progress_fraction=visual["progress_fraction"],
            has_input=visual["has_input"],
            has_output=visual["has_output"],
            output_branches=visual["output_branches"],
            port_radius=self.PORT_RADIUS,
        )
        painter.restore()

    def sizeHint(self, option, index):
        kind = index.data(_WorkflowNodePalette.ROLE_KIND)
        if kind == _WorkflowNodePalette.KIND_HEADER:
            return QSize(180, 24)
        return QSize(244, 100)