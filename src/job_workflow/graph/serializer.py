from __future__ import annotations

from collections.abc import Iterable, Mapping

from PySide6.QtCore import QPointF


def normalize_graph_nodes(raw_nodes: Iterable[object]) -> list[dict[str, str | float]]:
    return [
        node
        for node in raw_nodes
        if isinstance(node, dict) and node.get("id") and node.get("type")
    ]


def normalize_graph_edges(raw_edges: Iterable[object]) -> list[dict[str, str]]:
    return [
        edge
        for edge in raw_edges
        if isinstance(edge, dict) and edge.get("source") and edge.get("target")
    ]


def serialize_graph_nodes(nodes: Mapping[str, object]) -> list[dict[str, str | float]]:
    return [
        {
            "id": node_id,
            "type": getattr(node, "node_type"),
            "x": float(getattr(node, "pos")().x()),
            "y": float(getattr(node, "pos")().y()),
        }
        for node_id, node in nodes.items()
    ]


def serialize_graph_edges(edges: Iterable[tuple[str, str, str, object]]) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for source_id, target_id, branch, _edge in edges:
        edge = {"source": source_id, "target": target_id}
        if branch:
            edge["branch"] = branch
        result.append(edge)
    return result


def restore_graph(graph_view, nodes: Iterable[dict[str, str | float]], edges: Iterable[dict[str, str]]) -> None:
    for node in nodes:
        pos = None
        if "x" in node and "y" in node:
            pos = QPointF(float(node["x"]), float(node["y"]))
        graph_view.add_node(str(node["type"]), pos=pos, node_id=str(node["id"]))
    for edge in edges:
        graph_view.connect_nodes(str(edge["source"]), str(edge["target"]), str(edge.get("branch", "")))
