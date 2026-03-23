from __future__ import annotations

from PySide6.QtCore import QPointF, Qt
from PySide6.QtGui import QColor, QPen
from PySide6.QtWidgets import QGraphicsItem, QGraphicsRectItem, QGraphicsTextItem

from .defs import (
    _NODE_DEFINITIONS,
    _node_output_branches,
    _node_visual_state,
    _paint_node_card,
)
from ...workflow import WorkflowJob


class _StepNodeItem(QGraphicsRectItem):
    PORT_RADIUS = 7
    PORT_HIT_RADIUS = 16

    def __init__(self, node_id: str, node_type: str, graph):
        super().__init__(0, 0, 220, 88)
        self.node_id = node_id
        self.node_type = node_type
        self._graph = graph
        self._drag_connecting = False
        self.setFlags(
            QGraphicsItem.GraphicsItemFlag.ItemIsMovable
            | QGraphicsItem.GraphicsItemFlag.ItemSendsGeometryChanges
            | QGraphicsItem.GraphicsItemFlag.ItemIsSelectable
        )
        self.setPen(QPen(QColor("#CBD5E1"), 2))
        self._title = QGraphicsTextItem(self)
        self._title.setAcceptedMouseButtons(Qt.MouseButton.NoButton)
        self._title.setDefaultTextColor(QColor("#0F172A"))
        self._title.setPos(12, 8)
        self._title.setVisible(False)
        self._detail = QGraphicsTextItem(self)
        self._detail.setAcceptedMouseButtons(Qt.MouseButton.NoButton)
        self._detail.setDefaultTextColor(QColor("#475569"))
        self._detail.setPos(12, 34)
        self._detail.setVisible(False)
        self._state = QGraphicsTextItem(self)
        self._state.setAcceptedMouseButtons(Qt.MouseButton.NoButton)
        self._state.setDefaultTextColor(QColor("#1D4ED8"))
        self._state.setPos(12, 60)
        self._state.setVisible(False)
        self._visual_state = _node_visual_state(node_type)

    def update_from_job(self, job: WorkflowJob) -> None:
        visual = _node_visual_state(self.node_type, job)
        self._visual_state = visual
        self._title.setPlainText(visual["label"])
        self._detail.setPlainText(visual["detail"])
        self._state.setDefaultTextColor(visual["state_color"])
        self._state.setPlainText(visual["state_text"])
        self.update()

    def input_port_pos(self) -> QPointF:
        rect = self.rect()
        return self.mapToScene(QPointF(rect.left(), rect.center().y()))

    def output_port_pos(self, branch: str = "") -> QPointF:
        rect = self.rect()
        ports = _node_output_branches(self.node_type) or [("", "")]
        for index, (port_branch, _label) in enumerate(ports, start=1):
            if port_branch == branch:
                return self.mapToScene(QPointF(rect.right(), rect.top() + rect.height() * index / (len(ports) + 1)))
        return self.mapToScene(QPointF(rect.right(), rect.center().y()))

    def _hit_input_port(self, pos: QPointF) -> bool:
        return bool(_NODE_DEFINITIONS[self.node_type]["has_input"]) and (pos.x() - self.rect().left()) ** 2 + (pos.y() - self.rect().center().y()) ** 2 <= self.PORT_HIT_RADIUS**2

    def _hit_output_port(self, pos: QPointF) -> str | None:
        if not bool(_NODE_DEFINITIONS[self.node_type]["has_output"]):
            return None
        ports = _node_output_branches(self.node_type) or [("", "")]
        for index, (branch, _label) in enumerate(ports, start=1):
            y = self.rect().top() + self.rect().height() * index / (len(ports) + 1)
            if (pos.x() - self.rect().right()) ** 2 + (pos.y() - y) ** 2 <= self.PORT_HIT_RADIUS**2:
                return branch
        return None

    def mousePressEvent(self, event):
        branch = self._hit_output_port(event.pos())
        if branch is not None:
            self._graph.start_connection(self.node_id, branch)
            self._graph.update_temporary_connection(event.scenePos())
            self._drag_connecting = True
            event.accept()
            return
        if self._graph.has_pending_connection() and self.node_id != self._graph.pending_source() and bool(_NODE_DEFINITIONS[self.node_type]["has_input"]):
            self._graph.finish_connection(self.node_id)
            event.accept()
            return
        if self._hit_input_port(event.pos()):
            self._graph.finish_connection(self.node_id)
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self._drag_connecting and self._graph.pending_source() == self.node_id:
            self._graph.update_temporary_connection(event.scenePos())
            event.accept()
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if self._drag_connecting and self._graph.pending_source() == self.node_id:
            self._drag_connecting = False
            self._graph.complete_connection_at(event.scenePos())
            event.accept()
            return
        self._drag_connecting = False
        super().mouseReleaseEvent(event)

    def paint(self, painter, option, widget=None):
        border_color = QColor("#2563EB") if self.isSelected() else QColor("#CBD5E1")
        _paint_node_card(
            painter,
            self.rect(),
            fill_color=self._visual_state["fill_color"],
            border_color=border_color,
            title=self._visual_state["label"],
            detail=self._visual_state["detail"],
            state_text=self._visual_state["state_text"],
            state_color=self._visual_state["state_color"],
            progress_fill_color=self._visual_state["progress_fill_color"],
            progress_fraction=self._visual_state["progress_fraction"],
            has_input=self._visual_state["has_input"],
            has_output=self._visual_state["has_output"],
            output_branches=self._visual_state["output_branches"],
            port_radius=self.PORT_RADIUS,
        )

    def itemChange(self, change, value):
        if change == QGraphicsItem.GraphicsItemChange.ItemPositionHasChanged:
            self._graph.update_edges_for_nodes({self.node_id})
            self._graph.node_geometry_changed()
        return super().itemChange(change, value)