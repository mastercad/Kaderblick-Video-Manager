from __future__ import annotations

from PySide6.QtCore import QPointF, QRectF, Qt
from PySide6.QtGui import QColor, QPen
from PySide6.QtWidgets import QGraphicsPathItem

from .defs import _NODE_DEFINITIONS, _OUTGOING_RULES, _node_output_branches
from .edge_item import _GraphEdgeItem
from .geometry import build_connection_path
from .node_item import _StepNodeItem


class _WorkflowGraphInteraction:
    def __init__(self, graph_view):
        self._graph_view = graph_view
        self._pending_source: str | None = None
        self._pending_branch: str = ""
        self._preview_edge: QGraphicsPathItem | None = None

    def _scene(self):
        try:
            return self._graph_view._scene
        except RuntimeError:
            return None

    def reset(self) -> None:
        self.cancel_pending_connection()

    def selected_node_id(self) -> str | None:
        scene = self._scene()
        if scene is None:
            return None
        try:
            items = scene.selectedItems()
        except RuntimeError:
            return None
        for item in items:
            if isinstance(item, _StepNodeItem):
                return item.node_id
        return None

    def selected_edge_key(self) -> tuple[str, str] | None:
        scene = self._scene()
        if scene is None:
            return None
        try:
            items = scene.selectedItems()
        except RuntimeError:
            return None
        for item in items:
            if isinstance(item, _GraphEdgeItem):
                return item.source_id, item.target_id
        return None

    def emit_selection(self) -> None:
        scene = self._scene()
        if scene is None:
            return
        selected_node = self.selected_node_id()
        if selected_node is not None:
            self._graph_view._last_selection = {
                "kind": "node", "id": selected_node, "type": self._graph_view.node_type(selected_node)
            }
            self._graph_view.selection_changed.emit()
            return
        selected_edge = self.selected_edge_key()
        if selected_edge is not None:
            source_id, target_id = selected_edge
            self._graph_view._last_selection = {"kind": "edge", "source": source_id, "target": target_id}
            self._graph_view.selection_changed.emit()
            return
        self._graph_view._last_selection = {}
        self._graph_view.selection_changed.emit()

    def remove_selected_item(self) -> None:
        selected_node = self.selected_node_id()
        if selected_node is not None:
            self._graph_view.remove_node(selected_node)
            return
        selected_edge = self.selected_edge_key()
        if selected_edge is not None:
            self.remove_edge(*selected_edge)

    def has_pending_connection(self) -> bool:
        return self._pending_source is not None

    def pending_source(self) -> str | None:
        return self._pending_source

    def start_connection(self, source_id: str, branch: str = "") -> None:
        self.cancel_pending_connection()
        self._pending_source = source_id
        self._pending_branch = branch
        self._preview_edge = QGraphicsPathItem()
        self._preview_edge.setPen(QPen(QColor("#2563EB"), 3, Qt.PenStyle.DashLine))
        self._preview_edge.setZValue(-1)
        self._graph_view._scene.addItem(self._preview_edge)

    def finish_connection(self, target_id: str) -> None:
        if self._pending_source is None:
            return
        self.connect_nodes(self._pending_source, target_id, self._pending_branch)
        self.cancel_pending_connection()

    def cancel_pending_connection(self) -> None:
        self._pending_source = None
        self._pending_branch = ""
        if self._preview_edge is not None:
            self._graph_view._scene.removeItem(self._preview_edge)
            self._preview_edge = None

    def update_temporary_connection(self, scene_pos: QPointF) -> None:
        if self._pending_source is None or self._preview_edge is None:
            return
        source_node = self._graph_view.node_item(self._pending_source)
        if source_node is None:
            self.cancel_pending_connection()
            return
        start = source_node.output_port_pos(self._pending_branch)
        target_id = self._snap_target_node_id(scene_pos)
        if target_id is not None:
            target_node = self._graph_view.node_item(target_id)
            if target_node is not None:
                scene_pos = target_node.input_port_pos()
        exclude_ids = {self._pending_source}
        if target_id is not None:
            exclude_ids.add(target_id)
        self._preview_edge.setPath(
            build_connection_path(
                start,
                scene_pos,
                obstacles=self._graph_view.connection_obstacles(exclude_ids=exclude_ids),
            )
        )

    def complete_connection_at(self, scene_pos: QPointF) -> None:
        if self._pending_source is None:
            return
        target_id = self._snap_target_node_id(scene_pos) or self._node_at_scene_pos(scene_pos)
        if target_id is not None and target_id != self._pending_source:
            self.finish_connection(target_id)
            return
        self.cancel_pending_connection()

    def connect_nodes(self, source_id: str, target_id: str, branch: str = "") -> bool:
        if not self._can_connect(source_id, target_id, branch):
            return False
        if any(source == source_id and target == target_id for source, target, _branch, _edge in self._graph_view._edges):
            return False
        source_node = self._graph_view.node_item(source_id)
        if source_node is not None and len(_node_output_branches(source_node.node_type)) > 1:
            if any(
                source == source_id and existing_branch == branch
                for source, _target, existing_branch, _edge in self._graph_view._edges
            ):
                return False
        edge = _GraphEdgeItem(self._graph_view, source_id, target_id, branch)
        self._graph_view._scene.addItem(edge)
        self._graph_view._edges.append((source_id, target_id, branch, edge))
        self._graph_view.update_edges()
        self._graph_view.graph_changed.emit()
        return True

    def remove_edge(self, source_id: str, target_id: str) -> None:
        for edge_source, edge_target, branch, edge in list(self._graph_view._edges):
            if edge_source != source_id or edge_target != target_id:
                continue
            self._graph_view._scene.removeItem(edge)
            self._graph_view._edges.remove((edge_source, edge_target, branch, edge))
            self._graph_view.update_edges()
            self._graph_view.graph_changed.emit()
            return

    def node_output_at_scene_pos(self, scene_pos: QPointF) -> str | None:
        branch_hit = self.node_output_branch_at_scene_pos(scene_pos)
        return branch_hit[0] if branch_hit is not None else None

    def node_output_branch_at_scene_pos(self, scene_pos: QPointF) -> tuple[str, str] | None:
        best_id = None
        best_branch = ""
        best_distance = None
        for node_id, node in self._graph_view.node_entries():
            if not bool(_NODE_DEFINITIONS[node.node_type]["has_output"]):
                continue
            for branch, _label in _node_output_branches(node.node_type) or [("", "")]:
                port_pos = node.output_port_pos(branch)
                distance = (port_pos.x() - scene_pos.x()) ** 2 + (port_pos.y() - scene_pos.y()) ** 2
                if distance > self._graph_view.PORT_PICK_DISTANCE**2:
                    continue
                if best_distance is None or distance < best_distance:
                    best_id = node_id
                    best_branch = branch
                    best_distance = distance
        if best_id is None:
            return None
        return best_id, best_branch

    def _can_connect(self, source_id: str, target_id: str, branch: str = "") -> bool:
        if source_id == target_id:
            return False
        source_node = self._graph_view.node_item(source_id)
        target_node = self._graph_view.node_item(target_id)
        if source_node is None or target_node is None:
            return False
        branches = _node_output_branches(source_node.node_type)
        if len(branches) > 1 and branch not in {branch_key for branch_key, _label in branches}:
            return False
        return target_node.node_type in _OUTGOING_RULES.get(source_node.node_type, set())

    def _snap_target_node_id(self, scene_pos: QPointF) -> str | None:
        best_id = None
        best_distance = None
        for node_id, node in self._graph_view.node_entries():
            if node_id == self._pending_source or not bool(_NODE_DEFINITIONS[node.node_type]["has_input"]):
                continue
            port_pos = node.input_port_pos()
            distance = (port_pos.x() - scene_pos.x()) ** 2 + (port_pos.y() - scene_pos.y()) ** 2
            if distance > self._graph_view.SNAP_DISTANCE**2:
                continue
            if best_distance is None or distance < best_distance:
                best_id = node_id
                best_distance = distance
        return best_id

    def _node_at_scene_pos(self, scene_pos: QPointF) -> str | None:
        search_rect = QRectF(scene_pos.x() - 12, scene_pos.y() - 12, 24, 24)
        for item in self._graph_view._scene.items(search_rect):
            if isinstance(item, _StepNodeItem) and bool(_NODE_DEFINITIONS[item.node_type]["has_input"]):
                return item.node_id
        return None