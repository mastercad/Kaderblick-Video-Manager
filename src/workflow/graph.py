"""Graph-Hilfsfunktionen fuer Workflow-Jobs."""

from __future__ import annotations

from .model import WorkflowJob


_VALIDATION_NODE_TYPES = {"validate_surface", "validate_deep"}
_VALIDATION_BRANCHES = {"ok", "repairable", "irreparable"}


def _job_attr(job: WorkflowJob, name: str, default):
    try:
        raw_dict = object.__getattribute__(job, "__dict__")
    except Exception:
        raw_dict = {}
    return raw_dict.get(name, default)


def graph_node_map(job: WorkflowJob) -> dict[str, str]:
    return {
        str(node.get("id", "")): str(node.get("type", ""))
        for node in _job_attr(job, "graph_nodes", [])
        if isinstance(node, dict) and node.get("id") and node.get("type")
    }


def graph_node_id_for_type(job: WorkflowJob, node_type: str) -> str:
    for node_id, current_type in graph_node_map(job).items():
        if current_type == node_type:
            return node_id
    return ""


def graph_edge_defs(job: WorkflowJob) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for edge in _job_attr(job, "graph_edges", []):
        if not isinstance(edge, dict):
            continue
        source = str(edge.get("source", ""))
        target = str(edge.get("target", ""))
        branch = str(edge.get("branch", "")).strip().lower()
        if source and target:
            result.append(
                {
                    "source": source,
                    "target": target,
                    "branch": branch if branch in _VALIDATION_BRANCHES else "",
                }
            )
    return result


def graph_edges(job: WorkflowJob) -> list[tuple[str, str]]:
    return [(edge["source"], edge["target"]) for edge in graph_edge_defs(job)]


def graph_source_nodes(job: WorkflowJob) -> list[tuple[str, str]]:
    nodes = graph_node_map(job)
    return [
        (node_id, node_type)
        for node_id, node_type in nodes.items()
        if node_type in {"source_files", "source_folder_scan", "source_pi_download"}
    ]


def graph_has_multiple_sources(job: WorkflowJob) -> bool:
    return len(graph_source_nodes(job)) > 1


def graph_outgoing(job: WorkflowJob) -> dict[str, list[str]]:
    outgoing: dict[str, list[str]] = {}
    for source, target in graph_edges(job):
        outgoing.setdefault(source, []).append(target)
    return outgoing


def graph_outgoing_for_branches(
    job: WorkflowJob,
    branch_results: dict[str, str] | None = None,
) -> dict[str, list[str]]:
    nodes = graph_node_map(job)
    outgoing: dict[str, list[str]] = {}
    for edge in graph_edge_defs(job):
        source = edge["source"]
        target = edge["target"]
        branch = edge["branch"]
        selected_branch = (branch_results or {}).get(source, "")
        if nodes.get(source) in _VALIDATION_NODE_TYPES and selected_branch:
            if branch and branch != selected_branch:
                continue
            if not branch:
                continue
        outgoing.setdefault(source, []).append(target)
    return outgoing


def graph_reachable_node_ids(job: WorkflowJob) -> set[str]:
    outgoing = graph_outgoing(job)
    reachable = {node_id for node_id, _node_type in graph_source_nodes(job)}
    changed = True
    while changed:
        changed = False
        for source in list(reachable):
            for target in outgoing.get(source, []):
                if target not in reachable:
                    reachable.add(target)
                    changed = True
    return reachable


def graph_reachable_types(job: WorkflowJob) -> set[str]:
    nodes = graph_node_map(job)
    return {nodes[node_id] for node_id in graph_reachable_node_ids(job) if node_id in nodes}


def graph_merge_node_ids(job: WorkflowJob) -> list[str]:
    nodes = graph_node_map(job)
    reachable = graph_reachable_node_ids(job)
    return [node_id for node_id in reachable if nodes.get(node_id) == "merge"]


def graph_source_reaches_merge(job: WorkflowJob, source_node_id: str) -> bool:
    nodes = graph_node_map(job)
    outgoing = graph_outgoing(job)
    stack = [source_node_id]
    visited: set[str] = set()
    while stack:
        node_id = stack.pop()
        if node_id in visited:
            continue
        visited.add(node_id)
        if nodes.get(node_id) == "merge" and node_id != source_node_id:
            return True
        stack.extend(outgoing.get(node_id, []))
    return False


def graph_source_reaches_type(
    job: WorkflowJob,
    source_node_id: str,
    target_type: str,
    branch_results: dict[str, str] | None = None,
) -> bool:
    nodes = graph_node_map(job)
    outgoing = graph_outgoing_for_branches(job, branch_results)
    stack = [source_node_id]
    visited: set[str] = set()
    while stack:
        node_id = stack.pop()
        if node_id in visited:
            continue
        visited.add(node_id)
        if nodes.get(node_id) == target_type and node_id != source_node_id:
            return True
        stack.extend(outgoing.get(node_id, []))
    return False


def graph_source_has_pre_merge_titlecard(job: WorkflowJob, source_node_id: str) -> bool:
    nodes = graph_node_map(job)
    outgoing = graph_outgoing(job)
    stack: list[tuple[str, bool]] = [(source_node_id, False)]
    visited: set[tuple[str, bool]] = set()
    while stack:
        node_id, seen_titlecard = stack.pop()
        state = (node_id, seen_titlecard)
        if state in visited:
            continue
        visited.add(state)
        node_type = nodes.get(node_id)
        next_seen = seen_titlecard or (node_type == "titlecard" and node_id != source_node_id)
        if node_type == "merge" and node_id != source_node_id:
            if next_seen:
                return True
            continue
        for target in outgoing.get(node_id, []):
            stack.append((target, next_seen))
    return False


def graph_has_post_merge_titlecard(job: WorkflowJob) -> bool:
    nodes = graph_node_map(job)
    outgoing = graph_outgoing(job)
    merge_nodes = graph_merge_node_ids(job)
    if merge_nodes:
        start_nodes = merge_nodes
    else:
        start_nodes = [node_id for node_id, _node_type in graph_source_nodes(job)]
    stack = list(start_nodes)
    visited: set[str] = set()
    while stack:
        node_id = stack.pop()
        if node_id in visited:
            continue
        visited.add(node_id)
        node_type = nodes.get(node_id)
        if node_type == "titlecard" and node_id not in start_nodes:
            return True
        stack.extend(outgoing.get(node_id, []))
    return False


def graph_path_exists_between_types(job: WorkflowJob, start_types: set[str], target_type: str) -> bool:
    nodes = graph_node_map(job)
    outgoing = graph_outgoing(job)
    stack = [node_id for node_id, node_type in nodes.items() if node_type in start_types]
    start_nodes = set(stack)
    visited: set[str] = set()
    while stack:
        node_id = stack.pop()
        if node_id in visited:
            continue
        visited.add(node_id)
        if nodes.get(node_id) == target_type and node_id not in start_nodes:
            return True
        stack.extend(outgoing.get(node_id, []))
    return False


def graph_path_exists_between_types_for_branches(
    job: WorkflowJob,
    start_types: set[str],
    target_type: str,
    branch_results: dict[str, str] | None = None,
) -> bool:
    nodes = graph_node_map(job)
    outgoing = graph_outgoing_for_branches(job, branch_results)
    stack = [node_id for node_id, node_type in nodes.items() if node_type in start_types]
    start_nodes = set(stack)
    visited: set[str] = set()
    while stack:
        node_id = stack.pop()
        if node_id in visited:
            continue
        visited.add(node_id)
        if nodes.get(node_id) == target_type and node_id not in start_nodes:
            return True
        stack.extend(outgoing.get(node_id, []))
    return False


def graph_merge_reaches_type(
    job: WorkflowJob,
    target_type: str,
    branch_results: dict[str, str] | None = None,
) -> bool:
    if branch_results:
        return graph_path_exists_between_types_for_branches(job, {"merge"}, target_type, branch_results)
    return graph_path_exists_between_types(job, {"merge"}, target_type)


def graph_merge_precedes_convert(job: WorkflowJob) -> bool:
    return graph_path_exists_between_types(job, {"merge"}, "convert")


def graph_node_branch_has_targets(
    job: WorkflowJob,
    node_id: str,
    branch: str,
    branch_results: dict[str, str] | None = None,
) -> bool:
    selected = dict(branch_results or {})
    selected[node_id] = branch
    nodes = graph_node_map(job)
    outgoing = graph_outgoing_for_branches(job, selected)
    stack = list(outgoing.get(node_id, []))
    visited: set[str] = set()
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        visited.add(current)
        if nodes.get(current):
            return True
        stack.extend(outgoing.get(current, []))
    return False