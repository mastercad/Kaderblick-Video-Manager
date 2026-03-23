from __future__ import annotations

from collections.abc import Mapping

from PySide6.QtCore import QPointF, QRectF
from PySide6.QtGui import QPainterPath

from .defs import _NODE_DEFINITIONS


_LANE_POSITIONS = {
    "transfer": (80, 100),
    "processing": (360, 100),
    "delivery": (680, 100),
}

_PORT_LEAD = 40.0
_NODE_VERTICAL_SPACING = 132.0
_LANE_ORDER = ("transfer", "processing", "delivery")
_LANE_INDEX = {lane: index for index, lane in enumerate(_LANE_ORDER)}


def build_connection_path(
    start: QPointF,
    end: QPointF,
    *,
    obstacles: list[QRectF] | tuple[QRectF, ...] | None = None,
) -> QPainterPath:
    _ = obstacles
    delta_x = end.x() - start.x()
    delta_y = end.y() - start.y()
    path = QPainterPath(start)

    if abs(delta_x) < _PORT_LEAD:
        pull = max(72.0, min(abs(delta_y) * 0.45, 120.0))
        control_x = max(start.x(), end.x()) + pull
        path.cubicTo(
            QPointF(control_x, start.y()),
            QPointF(control_x, end.y()),
            end,
        )
        return path

    pull = max(_PORT_LEAD * 1.2, min(abs(delta_x) * 0.35, 120.0))
    path.cubicTo(
        QPointF(start.x() + pull, start.y()),
        QPointF(end.x() - pull, end.y()),
        end,
    )
    return path


def default_node_position(existing_nodes: Mapping[str, object], node_type: str) -> QPointF:
    lane = str(_NODE_DEFINITIONS[node_type]["lane"])
    default_x, default_y = _LANE_POSITIONS[lane]
    lane_index = sum(
        1
        for existing in existing_nodes.values()
        if _NODE_DEFINITIONS[getattr(existing, "node_type")]["lane"] == lane
    )
    return QPointF(default_x, default_y + lane_index * 120)


def auto_layout_graph(graph_view) -> None:
    lane_nodes: dict[str, list[object]] = {lane: [] for lane in _LANE_ORDER}
    node_map = {node.node_id: node for node in graph_view.node_items()}
    for node in node_map.values():
        lane = str(_NODE_DEFINITIONS[node.node_type]["lane"])
        lane_nodes.setdefault(lane, []).append(node)

    current_y = {node_id: node.pos().y() for node_id, node in node_map.items()}
    topology = _topological_order(node_map, graph_view.graph_edges())
    predecessors, successors = _build_adjacency(graph_view.graph_edges())

    lane_orders: dict[str, list[str]] = {
        lane: [str(getattr(node, "node_id")) for node in sorted(nodes, key=_auto_layout_node_sort_key)]
        for lane, nodes in lane_nodes.items()
    }
    lane_orders["transfer"] = _sort_lane_nodes(
        lane_orders.get("transfer", []),
        current_y,
        topology,
        predecessors,
        successors,
        lane_orders,
        freeze_to_current=True,
    )

    for _ in range(4):
        lane_orders["processing"] = _sort_lane_nodes(
            lane_orders.get("processing", []),
            current_y,
            topology,
            predecessors,
            successors,
            lane_orders,
        )
        lane_orders["delivery"] = _sort_lane_nodes(
            lane_orders.get("delivery", []),
            current_y,
            topology,
            predecessors,
            successors,
            lane_orders,
        )

    for lane in _LANE_ORDER:
        x, start_y = _LANE_POSITIONS[lane]
        for index, node_id in enumerate(lane_orders.get(lane, [])):
            node_map[node_id].setPos(QPointF(x, start_y + index * _NODE_VERTICAL_SPACING))
    graph_view.update_edges()


def _auto_layout_node_sort_key(node: object) -> tuple[float, str]:
    pos = getattr(node, "pos")()
    return (float(pos.y()), str(getattr(node, "node_id")))


def _build_adjacency(edges: list[dict[str, str]]) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    predecessors: dict[str, list[str]] = {}
    successors: dict[str, list[str]] = {}
    for edge in edges:
        source = str(edge["source"])
        target = str(edge["target"])
        successors.setdefault(source, []).append(target)
        predecessors.setdefault(target, []).append(source)
        predecessors.setdefault(source, [])
        successors.setdefault(target, [])
    return predecessors, successors


def _topological_order(node_map: Mapping[str, object], edges: list[dict[str, str]]) -> dict[str, int]:
    predecessors, successors = _build_adjacency(edges)
    indegree = {node_id: len(predecessors.get(node_id, [])) for node_id in node_map}
    ready = sorted(node_id for node_id, degree in indegree.items() if degree == 0)
    ordered: list[str] = []

    while ready:
        node_id = ready.pop(0)
        ordered.append(node_id)
        for target in sorted(successors.get(node_id, [])):
            indegree[target] -= 1
            if indegree[target] == 0:
                ready.append(target)
                ready.sort()

    if len(ordered) != len(node_map):
        remaining = sorted(node_id for node_id in node_map if node_id not in ordered)
        ordered.extend(remaining)

    return {node_id: index for index, node_id in enumerate(ordered)}


def _sort_lane_nodes(
    lane_node_ids: list[str],
    current_y: dict[str, float],
    topology: dict[str, int],
    predecessors: dict[str, list[str]],
    successors: dict[str, list[str]],
    lane_orders: dict[str, list[str]],
    *,
    freeze_to_current: bool = False,
) -> list[str]:
    if freeze_to_current:
        return sorted(lane_node_ids, key=lambda node_id: (current_y.get(node_id, 0.0), topology.get(node_id, 0), node_id))

    positions = {
        node_id: index
        for ordered_nodes in lane_orders.values()
        for index, node_id in enumerate(ordered_nodes)
    }
    return sorted(
        lane_node_ids,
        key=lambda node_id: (
            _lane_barycenter(node_id, positions, predecessors, successors),
            _neighbor_depth(node_id, topology, predecessors, successors),
            current_y.get(node_id, 0.0),
            node_id,
        ),
    )


def _lane_barycenter(
    node_id: str,
    positions: Mapping[str, int],
    predecessors: Mapping[str, list[str]],
    successors: Mapping[str, list[str]],
) -> float:
    incoming = [positions[other] for other in predecessors.get(node_id, []) if other in positions]
    outgoing = [positions[other] for other in successors.get(node_id, []) if other in positions]
    if incoming and outgoing:
        return (sum(incoming) / len(incoming)) * 0.7 + (sum(outgoing) / len(outgoing)) * 0.3
    if incoming:
        return sum(incoming) / len(incoming)
    if outgoing:
        return sum(outgoing) / len(outgoing)
    return positions.get(node_id, 0)


def _neighbor_depth(
    node_id: str,
    topology: Mapping[str, int],
    predecessors: Mapping[str, list[str]],
    successors: Mapping[str, list[str]],
) -> tuple[int, int, int]:
    incoming = predecessors.get(node_id, [])
    outgoing = successors.get(node_id, [])
    return (
        min((topology.get(other, 0) for other in incoming), default=topology.get(node_id, 0)),
        topology.get(node_id, 0),
        min((topology.get(other, 0) for other in outgoing), default=topology.get(node_id, 0)),
    )