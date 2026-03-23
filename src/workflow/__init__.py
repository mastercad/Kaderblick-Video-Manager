"""Workflow-Paket mit stabiler oeffentlicher API."""

from .graph import (
    graph_edge_defs,
    graph_edges,
    graph_has_multiple_sources,
    graph_has_post_merge_titlecard,
    graph_merge_node_ids,
    graph_merge_precedes_convert,
    graph_merge_reaches_type,
    graph_node_branch_has_targets,
    graph_node_id_for_type,
    graph_node_map,
    graph_outgoing,
    graph_outgoing_for_branches,
    graph_path_exists_between_types,
    graph_path_exists_between_types_for_branches,
    graph_reachable_node_ids,
    graph_reachable_types,
    graph_source_has_pre_merge_titlecard,
    graph_source_nodes,
    graph_source_reaches_merge,
    graph_source_reaches_type,
)
from .migration import _migrate_source_to_job
from .model import FileEntry, Workflow, WorkflowJob
from .naming import increment_workflow_name, normalize_workflow_name
from .storage import LAST_WORKFLOW_FILE, WORKFLOW_DIR


__all__ = [
    "FileEntry",
    "Workflow",
    "WorkflowJob",
    "WORKFLOW_DIR",
    "LAST_WORKFLOW_FILE",
    "normalize_workflow_name",
    "increment_workflow_name",
    "_migrate_source_to_job",
    "graph_node_map",
    "graph_node_id_for_type",
    "graph_edge_defs",
    "graph_edges",
    "graph_source_nodes",
    "graph_has_multiple_sources",
    "graph_outgoing",
    "graph_outgoing_for_branches",
    "graph_reachable_node_ids",
    "graph_reachable_types",
    "graph_merge_node_ids",
    "graph_source_reaches_merge",
    "graph_source_reaches_type",
    "graph_source_has_pre_merge_titlecard",
    "graph_has_post_merge_titlecard",
    "graph_path_exists_between_types",
    "graph_path_exists_between_types_for_branches",
    "graph_merge_reaches_type",
    "graph_merge_precedes_convert",
    "graph_node_branch_has_targets",
]