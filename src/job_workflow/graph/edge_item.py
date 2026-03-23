from __future__ import annotations

import math

from PySide6.QtCore import QPointF, Qt
from PySide6.QtGui import QColor, QPen, QPolygonF
from PySide6.QtWidgets import QGraphicsItem, QGraphicsPathItem


class _GraphEdgeItem(QGraphicsPathItem):
    _ARROW_SIZE = 10.0

    def __init__(self, graph, source_id: str, target_id: str, branch: str = ""):
        super().__init__()
        self._graph = graph
        self.source_id = source_id
        self.target_id = target_id
        self.branch = branch
        self.setFlag(QGraphicsItem.GraphicsItemFlag.ItemIsSelectable, True)
        self.setZValue(-1)

    def paint(self, painter, option, widget=None):
        color = QColor("#2563EB") if self.isSelected() else QColor("#94A3B8")
        width = 4 if self.isSelected() else 3
        pen = QPen(color, width)
        pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        pen.setJoinStyle(Qt.PenJoinStyle.RoundJoin)
        self.setPen(pen)
        super().paint(painter, option, widget)

        arrow = self._target_arrow_polygon()
        if arrow is not None:
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(color)
            painter.drawPolygon(arrow)

    def mouseDoubleClickEvent(self, event):
        self._graph.remove_edge(self.source_id, self.target_id)
        event.accept()

    def _target_arrow_polygon(self) -> QPolygonF | None:
        path = self.path()
        if path.isEmpty() or path.elementCount() < 2:
            return None

        tip = path.pointAtPercent(1.0)
        base = None
        for percent in (0.97, 0.94, 0.9, 0.84, 0.76):
            candidate = path.pointAtPercent(percent)
            if _distance(candidate, tip) >= 4.0:
                base = candidate
                break
        if base is None:
            return None

        direction_x = tip.x() - base.x()
        direction_y = tip.y() - base.y()
        length = math.hypot(direction_x, direction_y)
        if length < 1e-6:
            return None

        unit_x = direction_x / length
        unit_y = direction_y / length
        normal_x = -unit_y
        normal_y = unit_x
        tail_x = tip.x() - unit_x * self._ARROW_SIZE
        tail_y = tip.y() - unit_y * self._ARROW_SIZE
        half_width = self._ARROW_SIZE * 0.55

        left = QPointF(tail_x + normal_x * half_width, tail_y + normal_y * half_width)
        right = QPointF(tail_x - normal_x * half_width, tail_y - normal_y * half_width)
        return QPolygonF([tip, left, right])


def _distance(left: QPointF, right: QPointF) -> float:
    return math.hypot(right.x() - left.x(), right.y() - left.y())