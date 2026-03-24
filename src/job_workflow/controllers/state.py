from __future__ import annotations

from typing import TYPE_CHECKING

from ..graph import _STEP_LABELS, _planned_job_steps
from ..panels.status import build_source_summary
from ...workflow import FileEntry, graph_node_branch_has_targets, graph_reachable_types, graph_source_nodes, graph_source_reaches_merge

if TYPE_CHECKING:
    from ..dialog import JobWorkflowDialog


class WorkflowDialogStateController:
    def __init__(self, dialog: JobWorkflowDialog):
        self._dialog = dialog

    def runtime_snapshot(self):
        return (
            self._dialog._job.name,
            self._dialog._job.resume_status,
            tuple(sorted((self._dialog._job.step_statuses or {}).items()))
            if isinstance(self._dialog._job.step_statuses, dict)
            else (),
            tuple(sorted((self._dialog._job.step_details or {}).items()))
            if isinstance(self._dialog._job.step_details, dict)
            else (),
            self._dialog._job.progress_pct,
            self._dialog._job.overall_progress_pct,
            self._dialog._job.current_step_key,
            self._dialog._job.transfer_status,
            self._dialog._job.transfer_progress_pct,
        )

    def sync_runtime_state_from_job(self) -> None:
        snapshot = self.runtime_snapshot()
        if snapshot == self._dialog._last_runtime_snapshot:
            return
        self._dialog._last_runtime_snapshot = snapshot
        self._dialog._draft.name = self._dialog._job.name
        self._dialog._draft.resume_status = self._dialog._job.resume_status
        self._dialog._draft.step_statuses = (
            dict(self._dialog._job.step_statuses) if isinstance(self._dialog._job.step_statuses, dict) else {}
        )
        self._dialog._draft.step_details = (
            dict(self._dialog._job.step_details) if isinstance(self._dialog._job.step_details, dict) else {}
        )
        self._dialog._draft.progress_pct = self._dialog._job.progress_pct
        self._dialog._draft.overall_progress_pct = self._dialog._job.overall_progress_pct
        self._dialog._draft.current_step_key = self._dialog._job.current_step_key
        self._dialog._draft.transfer_status = self._dialog._job.transfer_status
        self._dialog._draft.transfer_progress_pct = self._dialog._job.transfer_progress_pct
        self.refresh_dynamic_sections()

    def on_editor_changed(self, text: str) -> None:
        self._dialog._draft.name = text.strip()
        self.refresh_dynamic_sections()

    def sync_editor_state(
        self,
        *,
        triggered_step: str | None = None,
        sync_graph: bool = True,
    ) -> None:
        if self._dialog._allow_edit and sync_graph:
            self._dialog._sync_draft_from_graph(refresh_graph=False)
        has_merge = any(file.merge_group_id for file in self._dialog._draft.files)
        reachable_types = graph_reachable_types(self._dialog._draft) if getattr(self._dialog._draft, "graph_nodes", None) else set()
        if getattr(self._dialog._draft, "graph_nodes", None):
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
            ) or has_merge or self._dialog._draft.upload_youtube
        else:
            has_output_stack = self._dialog._draft.convert_enabled or has_merge or self._dialog._draft.upload_youtube

        if not self._dialog._draft.upload_youtube:
            self._dialog._draft.upload_kaderblick = False
        if not has_output_stack:
            self._dialog._draft.title_card_enabled = False
            self._dialog._draft.create_youtube_version = False

        self._dialog._youtube_panel.sync_enabled_state(self._dialog._draft.upload_youtube)
        self._dialog._kb_game_id_edit.setEnabled(
            self._dialog._draft.upload_youtube and self._dialog._draft.upload_kaderblick
        )
        self._dialog._kb_reload_btn.setEnabled(
            self._dialog._draft.upload_youtube and self._dialog._draft.upload_kaderblick
        )
        self._dialog._kb_status_label.setEnabled(
            self._dialog._draft.upload_youtube and self._dialog._draft.upload_kaderblick
        )

        merge_sources = {
            node_id
            for node_id, _node_type in graph_source_nodes(self._dialog._draft)
            if graph_source_reaches_merge(self._dialog._draft, node_id)
        }
        merge_count = len({file.merge_group_id for file in self._dialog._draft.files if file.merge_group_id})
        if hasattr(self._dialog, "_youtube_panel"):
            self._dialog._youtube_panel.set_merge_output_mode(
                bool((merge_sources or merge_count) and self._dialog._draft.upload_youtube)
            )
            self._dialog._load_youtube_panel_from_draft()

        titlecard_enabled = self._dialog._draft.title_card_enabled and has_output_stack
        self._dialog._tc_home_edit.setEnabled(titlecard_enabled)
        self._dialog._tc_away_edit.setEnabled(titlecard_enabled)
        self._dialog._tc_date_edit.setEnabled(titlecard_enabled)
        self._dialog._tc_duration_spin.setEnabled(titlecard_enabled)
        self._dialog._tc_logo_edit.setEnabled(titlecard_enabled)
        self._dialog._tc_bg_edit.setEnabled(titlecard_enabled)
        self._dialog._tc_fg_edit.setEnabled(titlecard_enabled)
        self._dialog._amplify_db_spin.setEnabled(self._dialog._draft.amplify_audio)

        hints: list[str] = []
        if not has_output_stack:
            hints.append(
                "Titelkarte und YT-Version sind erst sinnvoll, wenn Konvertierung, Upload oder Merge aktiv ist."
            )
        if merge_sources or merge_count:
            hints.append(
                "Bei aktivem Merge kommen die finalen YouTube-Metadaten fuer Titel, Playlist und Beschreibung aus dem Merge-Bereich."
            )
        if self._dialog._draft.upload_youtube:
            hints.append("YouTube-Upload erzeugt eine Delivery-Lane und erlaubt optional den Kaderblick-Schritt.")
        validation_nodes = [
            str(node.get("id", ""))
            for node in getattr(self._dialog._draft, "graph_nodes", [])
            if isinstance(node, dict) and node.get("type") in {"validate_surface", "validate_deep"} and node.get("id")
        ]
        if validation_nodes and any(
            not graph_node_branch_has_targets(self._dialog._draft, node_id, "irreparable")
            for node_id in validation_nodes
        ):
            hints.append("Mindestens ein Prüf-Node hat keinen irreparabel-Branch. Verbinde ihn idealerweise mit Stop / Log oder Cleanup → Stop.")
        if triggered_step == "youtube_upload" and self._dialog._draft.upload_youtube:
            hints.append("Upload aktiviert: Du kannst jetzt optional Kaderblick zuschalten.")
        self._dialog._editor_hint.setText(
            " ".join(hints) if hints else "Der Workflow-Editor arbeitet auf denselben Jobdaten wie der Assistent."
        )
        self._dialog._source_panel.set_mode(self._dialog._draft.source_mode)
        self.refresh_dynamic_sections()

    def refresh_dynamic_sections(self) -> None:
        if not self._dialog._ui_ready:
            return
        self._dialog._title_label.setText(self._dialog._draft.name or "Unbenannter Workflow")
        self._dialog._meta_label.setText(
            f"Quelle: {build_source_summary(self._dialog._draft)}    |    Status: {self._dialog._draft.resume_status or 'Wartend'}"
        )
        self._dialog._pipeline_label.setText(
            "Ablauf: " + "  →  ".join(_STEP_LABELS[step] for step in _planned_job_steps(self._dialog._draft))
        )
        merge_warning = self._dialog._job_merge_warning_fn(self._dialog._draft)
        self._dialog._merge_warning_label.setVisible(bool(merge_warning))
        self._dialog._merge_warning_label.setToolTip(merge_warning)
        self._dialog._summary_box.refresh_from_job(self._dialog._draft)
        self._dialog._notes_box.refresh_from_job(self._dialog._draft)
        self._dialog._graph_view.refresh_from_job(self._dialog._draft)
        if hasattr(self._dialog, "_source_panel"):
            self._dialog._source_panel.refresh_from_job(self._dialog._draft)

    def apply_kaderblick_options(self, video_types: list[dict], cameras: list[dict]) -> None:
        self._dialog._kb_video_type_options = video_types
        self._dialog._kb_camera_options = cameras
        self._dialog._sync_kaderblick_selectors()

    def apply_pi_camera_entries(self, entries: list[FileEntry]) -> None:
        self._dialog._pi_file_list.load(entries)
        self._dialog._pi_file_list.setVisible(True)
        self._dialog._draft.files = self._dialog._pi_file_list.collect()
        self.sync_editor_state()

    def on_pi_load_failed(self) -> None:
        self._dialog._draft.files = (
            self._dialog._pi_file_list.collect() if hasattr(self._dialog, "_pi_file_list") else self._dialog._draft.files
        )