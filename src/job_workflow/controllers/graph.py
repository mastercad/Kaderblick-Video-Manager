from __future__ import annotations

from PySide6.QtCore import QTimer

from ..graph import _NODE_DEFINITIONS, _SOURCE_NODE_TYPES, _STEP_LABELS, _planned_job_steps
from ..graph.builder import build_default_graph
from ..graph.geometry import auto_layout_graph
from ..graph.serializer import (
    normalize_graph_edges,
    normalize_graph_nodes,
    restore_graph,
)
from ..panels.status import build_source_summary
from ...media.merge_analysis import job_merge_warning
from ...workflow import (
    graph_node_branch_has_targets,
    graph_has_post_merge_titlecard,
    graph_reachable_types,
    graph_source_has_pre_merge_titlecard,
    graph_source_nodes,
    graph_source_reaches_merge,
)


class WorkflowGraphController:
    def __init__(self, dialog):
        self._dialog = dialog

    def sync_editor_state(self, *, triggered_step: str | None = None, sync_graph: bool = True) -> None:
        dialog = self._dialog
        draft = dialog._draft
        if dialog._allow_edit and sync_graph:
            self.sync_draft_from_graph(refresh_graph=False)
        has_merge = any(file.merge_group_id for file in draft.files)
        reachable_types = graph_reachable_types(draft) if getattr(draft, "graph_nodes", None) else set()
        has_validation = any(node_type in reachable_types for node_type in {"validate_surface", "validate_deep"})
        if getattr(draft, "graph_nodes", None):
            has_output_stack = any(
                node_type in reachable_types
                for node_type in {
                    "convert",
                    "merge",
                    "titlecard",
                    "validate_surface",
                    "validate_deep",
                    "cleanup",
                    "repair",
                    "yt_version",
                    "stop",
                    "youtube_upload",
                    "kaderblick",
                }
            ) or has_merge
        else:
            has_output_stack = draft.convert_enabled or has_merge or draft.upload_youtube or has_validation

        if not draft.upload_youtube:
            draft.upload_kaderblick = False
        if not has_output_stack:
            draft.title_card_enabled = False
            draft.create_youtube_version = False

        has_titlecard_node = "titlecard" in reachable_types if getattr(draft, "graph_nodes", None) else draft.title_card_enabled

        dialog._youtube_panel.sync_enabled_state(draft.upload_youtube)
        dialog._kb_game_id_edit.setEnabled(draft.upload_youtube and draft.upload_kaderblick)
        dialog._kb_reload_btn.setEnabled(draft.upload_youtube and draft.upload_kaderblick)
        dialog._kb_status_label.setEnabled(draft.upload_youtube and draft.upload_kaderblick)
        titlecard_enabled = has_titlecard_node and has_output_stack
        dialog._tc_home_edit.setEnabled(titlecard_enabled)
        dialog._tc_away_edit.setEnabled(titlecard_enabled)
        dialog._tc_date_edit.setEnabled(titlecard_enabled)
        dialog._tc_duration_spin.setEnabled(titlecard_enabled)
        dialog._tc_logo_edit.setEnabled(titlecard_enabled)
        dialog._tc_logo_browse_btn.setEnabled(titlecard_enabled)
        dialog._tc_bg_edit.setEnabled(titlecard_enabled)
        dialog._tc_fg_edit.setEnabled(titlecard_enabled)
        dialog._tc_bg_pick_btn.setEnabled(titlecard_enabled)
        dialog._tc_fg_pick_btn.setEnabled(titlecard_enabled)
        dialog._tc_preview_frame.setEnabled(titlecard_enabled)
        dialog._amplify_db_spin.setEnabled(draft.amplify_audio)

        merge_sources = {node_id for node_id, _node_type in graph_source_nodes(draft) if graph_source_reaches_merge(draft, node_id)}
        merge_count = len({file.merge_group_id for file in draft.files if file.merge_group_id})
        if hasattr(dialog, "_youtube_panel"):
            dialog._youtube_panel.set_merge_output_mode(bool((merge_sources or merge_count) and draft.upload_youtube))
            dialog._load_youtube_panel_from_draft()
        hints: list[str] = []
        if not has_output_stack:
            hints.append("Titelkarte und YT-Version sind erst sinnvoll, wenn Konvertierung, Upload oder Merge aktiv ist.")
        if merge_sources or merge_count:
            hints.append(
                "Bei aktivem Merge kommen die finalen YouTube-Metadaten fuer Titel, Playlist und Beschreibung aus dem Merge-Bereich."
            )
        if draft.upload_youtube:
            hints.append("YouTube-Upload erzeugt eine Delivery-Lane und erlaubt optional den Kaderblick-Schritt.")
        validation_nodes = [
            str(node.get("id", ""))
            for node in getattr(draft, "graph_nodes", [])
            if isinstance(node, dict) and node.get("type") in {"validate_surface", "validate_deep"} and node.get("id")
        ]
        if validation_nodes and any(not graph_node_branch_has_targets(draft, node_id, "irreparable") for node_id in validation_nodes):
            hints.append("Mindestens ein Prüf-Node hat keinen irreparabel-Branch. Verbinde ihn idealerweise mit Stop / Log oder Cleanup → Stop.")
        if triggered_step == "youtube_upload" and draft.upload_youtube:
            hints.append("Upload aktiviert: Du kannst jetzt optional Kaderblick zuschalten.")
        dialog._editor_hint.setText(" ".join(hints) if hints else "Der Workflow-Editor arbeitet auf denselben Jobdaten wie der Assistent.")

        if hasattr(dialog, "_source_panel"):
            dialog._source_panel.set_mode(draft.source_mode)
        self.refresh_dynamic_sections()

    def refresh_dynamic_sections(self) -> None:
        dialog = self._dialog
        draft = dialog._draft
        if not dialog._ui_ready:
            return
        dialog._title_label.setText(draft.name or "Unbenannter Workflow")
        dialog._meta_label.setText(f"Quelle: {build_source_summary(draft)}    |    Status: {draft.resume_status or 'Wartend'}")
        dialog._pipeline_label.setText("Ablauf: " + "  →  ".join(_STEP_LABELS[step] for step in _planned_job_steps(draft)))
        merge_warning = job_merge_warning(draft)
        dialog._merge_warning_label.setVisible(bool(merge_warning))
        dialog._merge_warning_label.setToolTip(merge_warning)
        dialog._summary_box.refresh_from_job(draft)
        dialog._notes_box.refresh_from_job(draft)
        dialog._graph_view.refresh_from_job(draft)
        if hasattr(dialog, "_source_panel"):
            dialog._source_panel.refresh_from_job(draft)

    def on_graph_selection_changed(self, selection) -> None:
        dialog = self._dialog
        if not dialog._allow_edit:
            return
        if not selection:
            dialog._editor_hint.setText("Node auswählen, um rechts ihre Einstellungen zu bearbeiten.")
            dialog._property_stack.setCurrentIndex(dialog._property_pages["default"])
            return
        if selection.get("kind") == "edge":
            dialog._property_stack.setCurrentIndex(dialog._property_pages["default"])
            dialog._editor_hint.setText("Verbindung ausgewählt. Mit Auswahl entfernen oder Doppelklick auf die Kante löschen.")
            return
        node_type = selection.get("type")
        definition = _NODE_DEFINITIONS.get(node_type, {})
        if definition:
            dialog._editor_hint.setText(f"{definition.get('label', 'Node')} ausgewählt. {definition.get('detail', '')}")
        if node_type in _SOURCE_NODE_TYPES:
            mode_map = {
                "source_files": "files",
                "source_folder_scan": "folder_scan",
                "source_pi_download": "pi_download",
            }
            dialog._draft.source_mode = mode_map.get(node_type, dialog._draft.source_mode)
            if hasattr(dialog, "_source_panel"):
                dialog._source_panel.set_mode(dialog._draft.source_mode)
                dialog._source_panel.refresh_from_job(dialog._draft)
            dialog._property_stack.setCurrentIndex(dialog._property_pages["source"])
        elif node_type == "convert":
            dialog._property_stack.setCurrentIndex(dialog._property_pages["convert"])
        elif node_type in {"merge", "titlecard", "validate_surface", "validate_deep", "cleanup", "repair", "yt_version", "stop", "youtube_upload", "kaderblick"}:
            dialog._property_stack.setCurrentIndex(dialog._property_pages[node_type])
            if node_type in {"merge", "youtube_upload", "kaderblick"}:
                dialog._kb_load_api_data()
        else:
            dialog._property_stack.setCurrentIndex(dialog._property_pages["default"])

    def on_graph_changed(self) -> None:
        self.sync_draft_from_graph()
        if self._dialog._allow_edit:
            self.sync_editor_state(sync_graph=False)

    def remove_selected_graph_node(self) -> None:
        self._dialog._graph_view.remove_selected_item()

    def auto_layout_graph(self) -> None:
        auto_layout_graph(self._dialog._graph_view)

    def rebuild_graph_from_job(self, *, use_stored_graph: bool = True) -> None:
        dialog = self._dialog
        draft = dialog._draft
        dialog._rebuilding_graph = True
        dialog._graph_view.clear_graph()
        stored_nodes = normalize_graph_nodes(draft.graph_nodes)
        stored_edges = normalize_graph_edges(draft.graph_edges)
        if use_stored_graph and stored_nodes:
            restore_graph(dialog._graph_view, stored_nodes, stored_edges)
        else:
            build_default_graph(dialog._graph_view, draft)
        if not (use_stored_graph and stored_nodes):
            self.auto_layout_graph()
        dialog._rebuilding_graph = False
        dialog._graph_view.refresh_from_job(draft)
        self.sync_draft_from_graph(refresh_graph=False)
        if dialog._allow_edit:
            dialog._on_graph_selection_changed(None)
            dialog._selection_label.setText("Keine Auswahl")
            dialog._remove_node_btn.setEnabled(False)
        QTimer.singleShot(0, dialog._graph_view.fit_scene_contents)

    def sync_draft_from_graph(self, refresh_graph: bool = True) -> None:
        dialog = self._dialog
        draft = dialog._draft
        if dialog._rebuilding_graph:
            return
        draft.graph_nodes = dialog._graph_view.graph_nodes()
        draft.graph_edges = dialog._graph_view.graph_edges()

        source_nodes = graph_source_nodes(draft)
        reachable_types = graph_reachable_types(draft)
        mode_map = {
            "source_files": "files",
            "source_folder_scan": "folder_scan",
            "source_pi_download": "pi_download",
        }
        if len(source_nodes) == 1:
            draft.source_mode = mode_map.get(source_nodes[0][1], draft.source_mode)

        files_source_ids = [node_id for node_id, node_type in source_nodes if node_type == "source_files"]
        valid_source_ids = {node_id for node_id, _node_type in source_nodes}
        default_files_source_id = files_source_ids[0] if files_source_ids else ""
        for entry in draft.files:
            if entry.graph_source_id and entry.graph_source_id not in valid_source_ids:
                entry.graph_source_id = ""
            if not entry.graph_source_id and default_files_source_id:
                entry.graph_source_id = default_files_source_id

        merged_sources = {node_id for node_id, _node_type in source_nodes if graph_source_reaches_merge(draft, node_id)}
        pre_merge_title_sources = {node_id for node_id, _node_type in source_nodes if graph_source_has_pre_merge_titlecard(draft, node_id)}
        merge_group_id = "graph-merge" if merged_sources else ""
        for entry in draft.files:
            entry.merge_group_id = merge_group_id if entry.graph_source_id in merged_sources else ""
            entry.title_card_before_merge = entry.graph_source_id in pre_merge_title_sources

        draft.convert_enabled = "convert" in reachable_types
        draft.title_card_enabled = graph_has_post_merge_titlecard(draft)
        draft.create_youtube_version = "yt_version" in reachable_types
        draft.upload_youtube = "youtube_upload" in reachable_types
        draft.upload_kaderblick = "kaderblick" in reachable_types and draft.upload_youtube
        if hasattr(dialog, "_source_panel"):
            dialog._source_panel.set_mode(draft.source_mode)
        if refresh_graph:
            self.refresh_dynamic_sections()