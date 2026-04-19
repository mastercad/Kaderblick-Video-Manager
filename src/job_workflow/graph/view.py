from __future__ import annotations

from collections import defaultdict
import uuid

from PySide6.QtCore import QPoint, QPointF, QRectF, Qt, Signal
from PySide6.QtGui import QPainter, QPen, QColor
from PySide6.QtWidgets import QFrame, QGraphicsPathItem, QGraphicsScene, QGraphicsView

from .defs import _NODE_DEFINITIONS, _OUTGOING_RULES, _UNIQUE_NODE_TYPES, _node_output_branches
from .edge_item import _GraphEdgeItem
from .geometry import auto_layout_graph, build_connection_path, default_node_position
from .interaction import _WorkflowGraphInteraction
from .node_item import _StepNodeItem
from .palette import _WorkflowNodePalette
from .serializer import serialize_graph_edges, serialize_graph_nodes
from ...workflow import WorkflowJob


class _WorkflowGraphView(QGraphicsView):
    selection_changed = Signal(dict)
    graph_changed = Signal()
    SNAP_DISTANCE = 28
    PORT_PICK_DISTANCE = 18
    ZOOM_STEP = 1.15
    MIN_ZOOM = 0.55
    MAX_ZOOM = 2.5

    def __init__(self, parent=None):
        super().__init__(parent)
        self._scene = QGraphicsScene(self)
        self.setScene(self._scene)
        self.setRenderHint(QPainter.RenderHint.Antialiasing)
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setMinimumHeight(420)
        self.setStyleSheet("background: #F8FAFC; border: 1px solid #D7E0EA; border-radius: 12px;")
        self.setViewportUpdateMode(QGraphicsView.ViewportUpdateMode.FullViewportUpdate)
        self.setCacheMode(QGraphicsView.CacheModeFlag.CacheNone)
        self.setTransformationAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)
        self.setAcceptDrops(True)
        self._nodes: dict[str, _StepNodeItem] = {}
        self._edges: list[tuple[str, str, str, _GraphEdgeItem]] = []
        self._draft_job: WorkflowJob | None = None
        self._is_panning = False
        self._last_pan_pos: QPoint | None = None
        self._interaction = _WorkflowGraphInteraction(self)
        self._scene.selectionChanged.connect(self._interaction.emit_selection)

    def clear_graph(self) -> None:
        self._interaction.reset()
        self._scene.clear()
        self._nodes = {}
        self._edges = []

    def refresh_from_job(self, job: WorkflowJob) -> None:
        self._draft_job = job
        for node in self.node_items():
            node.update_from_job(job)
        self.update_edges()
        rect = self._scene.itemsBoundingRect().adjusted(-30, -30, 30, 30)
        if not rect.isNull():
            self.setSceneRect(rect)

    def fit_scene_contents(self) -> None:
        rect = self.sceneRect()
        if rect.isNull():
            return
        viewport = self.viewport()
        if viewport is None or viewport.width() <= 0 or viewport.height() <= 0:
            return
        self.resetTransform()
        self.fitInView(rect, Qt.AspectRatioMode.KeepAspectRatio)
        if self.transform().m11() > 1.0 or self.transform().m22() > 1.0:
            self.resetTransform()
            self.centerOn(rect.center())

    def graph_nodes(self) -> list[dict[str, str | float]]:
        return serialize_graph_nodes(self._nodes)

    def graph_edges(self) -> list[dict[str, str]]:
        return serialize_graph_edges(self._edges)

    def node_type(self, node_id: str | None) -> str | None:
        if node_id is None:
            return None
        node = self.node_item(node_id)
        return node.node_type if node is not None else None

    def node_item(self, node_id: str | None):
        if node_id is None:
            return None
        return self._nodes.get(node_id)

    def node_items(self) -> list[_StepNodeItem]:
        return list(self._nodes.values())

    def node_entries(self) -> list[tuple[str, _StepNodeItem]]:
        return list(self._nodes.items())

    def find_node_id_by_type(self, node_type: str) -> str | None:
        return next((existing_id for existing_id, node in self.node_entries() if node.node_type == node_type), None)

    def add_node(self, node_type: str, pos: QPointF | None = None, node_id: str | None = None) -> str:
        if node_type in _UNIQUE_NODE_TYPES:
            existing_id = self.find_node_id_by_type(node_type)
            if existing_id is not None:
                existing_node = self.node_item(existing_id)
                if existing_node is not None:
                    existing_node.setSelected(True)
                return existing_id
        node_id = node_id or f"{node_type}-{uuid.uuid4().hex[:6]}"
        node = _StepNodeItem(node_id, node_type, self)
        node.setPos(pos or default_node_position(self._nodes, node_type))
        self._scene.addItem(node)
        self._nodes[node_id] = node
        if self._draft_job is not None:
            node.update_from_job(self._draft_job)
        self.refresh_from_job(self._draft_job or WorkflowJob())
        self.graph_changed.emit()
        return node_id

    def node_geometry_changed(self) -> None:
        if self._draft_job is None:
            return
        self.graph_changed.emit()

    def remove_selected_item(self) -> None:
        self._interaction.remove_selected_item()

    def remove_node(self, node_id: str) -> None:
        node = self._nodes.pop(node_id, None)
        if node is None:
            return
        for source_id, target_id, _branch, edge in list(self._edges):
            if source_id != node_id and target_id != node_id:
                continue
            self._scene.removeItem(edge)
            self._edges.remove((source_id, target_id, _branch, edge))
        self._scene.removeItem(node)
        self.refresh_from_job(self._draft_job or WorkflowJob())
        self.graph_changed.emit()

    def remove_edge(self, source_id: str, target_id: str) -> None:
        self._interaction.remove_edge(source_id, target_id)

    def selected_node_id(self):
        return self._interaction.selected_node_id()

    def selected_edge_key(self):
        return self._interaction.selected_edge_key()

    def has_pending_connection(self) -> bool:
        return self._interaction.has_pending_connection()

    def pending_source(self) -> str | None:
        return self._interaction.pending_source()

    def edge_pairs(self) -> list[tuple[str, str]]:
        return [(source, target) for source, target, _branch, _edge in self._edges]

    def start_connection(self, node_type: str, branch: str = "") -> None:
        self._interaction.start_connection(node_type, branch)

    def finish_connection(self, node_type: str) -> None:
        self._interaction.finish_connection(node_type)

    def cancel_pending_connection(self) -> None:
        self._interaction.cancel_pending_connection()

    def update_temporary_connection(self, scene_pos: QPointF) -> None:
        self._interaction.update_temporary_connection(scene_pos)

    def complete_connection_at(self, scene_pos: QPointF) -> None:
        self._interaction.complete_connection_at(scene_pos)

    def connection_obstacles(self, *, exclude_ids: set[str] | None = None) -> list[QRectF]:
        excluded = exclude_ids or set()
        return [
            node.sceneBoundingRect()
            for node_id, node in self.node_entries()
            if node_id not in excluded
        ]

    def connect_nodes(self, source_id: str, target_id: str, branch: str = "") -> bool:
        return self._interaction.connect_nodes(source_id, target_id, branch)

    def _node_output_at_scene_pos(self, scene_pos: QPointF) -> str | None:
        return self._interaction.node_output_at_scene_pos(scene_pos)

    def _node_output_branch_at_scene_pos(self, scene_pos: QPointF) -> tuple[str, str] | None:
        return self._interaction.node_output_branch_at_scene_pos(scene_pos)

    def dragEnterEvent(self, event):
        if event.mimeData().hasFormat(_WorkflowNodePalette.MIME_TYPE):
            event.acceptProposedAction()
            return
        super().dragEnterEvent(event)

    def dragMoveEvent(self, event):
        if event.mimeData().hasFormat(_WorkflowNodePalette.MIME_TYPE):
            event.acceptProposedAction()
            return
        super().dragMoveEvent(event)

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.MiddleButton:
            self._start_panning(event.pos())
            event.accept()
            return
        if event.button() == Qt.MouseButton.LeftButton and not self.has_pending_connection():
            scene_pos = self.mapToScene(event.pos())
            branch_hit = self._node_output_branch_at_scene_pos(scene_pos)
            if branch_hit is not None:
                source_id, branch = branch_hit
                self.start_connection(source_id, branch)
                self.update_temporary_connection(scene_pos)
                event.accept()
                return
        super().mousePressEvent(event)

    def dropEvent(self, event):
        if event.mimeData().hasFormat(_WorkflowNodePalette.MIME_TYPE):
            node_type = bytes(event.mimeData().data(_WorkflowNodePalette.MIME_TYPE)).decode("utf-8")
            self.add_node(node_type, self.mapToScene(event.position().toPoint()))
            event.acceptProposedAction()
            return
        super().dropEvent(event)

    def mouseMoveEvent(self, event):
        if self._is_panning:
            self._pan_to(event.pos())
            event.accept()
            return
        if self.has_pending_connection():
            self.update_temporary_connection(self.mapToScene(event.pos()))
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.MouseButton.MiddleButton and self._is_panning:
            self._stop_panning()
            event.accept()
            return
        if self.has_pending_connection():
            self.complete_connection_at(self.mapToScene(event.pos()))
            event.accept()
            return
        super().mouseReleaseEvent(event)

    def wheelEvent(self, event):
        delta = event.angleDelta().y()
        if delta == 0:
            super().wheelEvent(event)
            return
        factor = self.ZOOM_STEP if delta > 0 else 1 / self.ZOOM_STEP
        self._apply_zoom_factor(factor)
        event.accept()

    def _apply_zoom_factor(self, factor: float) -> None:
        current_zoom = self.transform().m11()
        if current_zoom <= 0:
            return
        target_zoom = max(self.MIN_ZOOM, min(self.MAX_ZOOM, current_zoom * factor))
        scale_factor = target_zoom / current_zoom
        if abs(scale_factor - 1.0) < 1e-6:
            return
        self.scale(scale_factor, scale_factor)

    def _start_panning(self, pos: QPoint) -> None:
        self._is_panning = True
        self._last_pan_pos = QPoint(pos)
        self.setCursor(Qt.CursorShape.ClosedHandCursor)

    def _pan_to(self, pos: QPoint) -> None:
        if not self._is_panning or self._last_pan_pos is None:
            return
        delta = pos - self._last_pan_pos
        self._last_pan_pos = QPoint(pos)
        self.horizontalScrollBar().setValue(self.horizontalScrollBar().value() - delta.x())
        self.verticalScrollBar().setValue(self.verticalScrollBar().value() - delta.y())

    def _stop_panning(self) -> None:
        self._is_panning = False
        self._last_pan_pos = None
        self.unsetCursor()

    def update_edges(self) -> None:
        self._update_edge_paths()

    def update_edges_for_nodes(self, node_ids: set[str] | tuple[str, ...] | list[str]) -> None:
        self._update_edge_paths(set(node_ids))

    def _update_edge_paths(self, node_ids: set[str] | None = None) -> None:
        for source_id, target_id, branch, edge in self._edges:
            if node_ids is not None and source_id not in node_ids and target_id not in node_ids:
                continue
            from_node = self.node_item(source_id)
            to_node = self.node_item(target_id)
            if from_node is None or to_node is None:
                continue
            start = from_node.output_port_pos(branch)
            end = to_node.input_port_pos()
            edge.setPath(
                build_connection_path(
                    start,
                    end,
                    obstacles=self.connection_obstacles(exclude_ids={source_id, target_id}),
                )
            )