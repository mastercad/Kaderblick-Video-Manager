from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..graph import _STEP_LABELS, _planned_job_steps
from ..panels.inspector import SourceMaterialSummary, build_configured_source_summary
from ..panels.status import build_source_summary
from ...workflow import (
    FileEntry,
    WorkflowJob,
    graph_node_branch_has_targets,
    graph_path_exists_between_types,
    graph_reachable_types,
    graph_source_nodes,
    graph_source_reaches_merge,
)

if TYPE_CHECKING:
    from ..dialog import JobWorkflowDialog


@dataclass(frozen=True)
class StepInputContext:
    summary: SourceMaterialSummary | None
    encoder_label: str
    crf_label: str
    fps_label: str
    format_label: str
    resolution_label: str
    base_encoder: str
    base_crf: int


def _effective_merge_encoder(job: WorkflowJob) -> str:
    return str(job.merge_encoder or "inherit") if str(job.merge_encoder or "inherit") not in {"", "inherit"} else str(job.encoder)


def _effective_merge_crf(job: WorkflowJob) -> int:
    return int(job.merge_crf) if int(job.merge_crf or 0) > 0 else int(job.crf)


def resolve_step_input_context(
    job: WorkflowJob,
    step_type: str,
    *,
    youtube_default_crf: int,
) -> StepInputContext:
    has_graph = bool(getattr(job, "graph_nodes", None))
    convert_before_merge = graph_path_exists_between_types(job, {"convert"}, "merge") if has_graph else bool(job.convert_enabled)
    convert_before_yt = graph_path_exists_between_types(job, {"convert"}, "yt_version") if has_graph else bool(job.convert_enabled)
    merge_before_yt = graph_path_exists_between_types(job, {"merge"}, "yt_version") if has_graph else False

    convert_summary = build_configured_source_summary(
        step_label="Konvertieren",
        output_format=str(job.output_format or "source"),
        output_resolution=str(job.output_resolution or "source"),
        fps=int(job.fps or 0),
    )

    if step_type == "merge":
        if convert_before_merge:
            return StepInputContext(
                summary=convert_summary,
                encoder_label="Von Konvertieren übernehmen",
                crf_label="Von Konvertieren übernehmen",
                fps_label="Von Konvertieren übernehmen",
                format_label="Von Konvertieren übernehmen",
                resolution_label="Von Konvertieren übernehmen",
                base_encoder=str(job.encoder),
                base_crf=int(job.crf),
            )
        return StepInputContext(
            summary=None,
            encoder_label="App-Standard übernehmen",
            crf_label="App-Standard-CRF übernehmen",
            fps_label="Von Quelle übernehmen",
            format_label="Von Quelle übernehmen",
            resolution_label="Von Quelle übernehmen",
            base_encoder=str(job.encoder),
            base_crf=int(job.crf),
        )

    if step_type != "yt_version":
        return StepInputContext(
            summary=None,
            encoder_label="App-Standard übernehmen",
            crf_label="App-Standard-CRF übernehmen",
            fps_label="Von Quelle übernehmen",
            format_label="Von Quelle übernehmen",
            resolution_label="Von Quelle übernehmen",
            base_encoder=str(job.encoder),
            base_crf=int(job.crf),
        )

    if merge_before_yt:
        return StepInputContext(
            summary=build_configured_source_summary(
                step_label="Merge",
                output_format=str(job.merge_output_format or "source"),
                output_resolution=str(job.merge_output_resolution or "source"),
                fps=int(job.merge_fps or 0),
                base_summary=convert_summary if convert_before_merge else None,
            ),
            encoder_label="Von Merge übernehmen",
            crf_label="Von Merge übernehmen",
            fps_label="Von Merge übernehmen",
            format_label="Von Merge übernehmen",
            resolution_label="Von Merge übernehmen",
            base_encoder=_effective_merge_encoder(job),
            base_crf=_effective_merge_crf(job),
        )

    if convert_before_yt:
        return StepInputContext(
            summary=convert_summary,
            encoder_label="Von Konvertieren übernehmen",
            crf_label="Von Konvertieren übernehmen",
            fps_label="Von Konvertieren übernehmen",
            format_label="Von Konvertieren übernehmen",
            resolution_label="Von Konvertieren übernehmen",
            base_encoder=str(job.encoder),
            base_crf=int(job.crf),
        )

    return StepInputContext(
        summary=None,
        encoder_label="App-Standard übernehmen",
        crf_label="YouTube-Standard übernehmen",
        fps_label="Von Quelle übernehmen",
        format_label="Von Quelle übernehmen",
        resolution_label="Von Quelle übernehmen",
        base_encoder=str(job.encoder),
        base_crf=int(youtube_default_crf),
    )


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

        has_titlecard_node = "titlecard" in reachable_types if getattr(self._dialog._draft, "graph_nodes", None) else self._dialog._draft.title_card_enabled

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

        titlecard_enabled = has_titlecard_node and has_output_stack
        self._dialog._tc_home_edit.setEnabled(titlecard_enabled)
        self._dialog._tc_away_edit.setEnabled(titlecard_enabled)
        self._dialog._tc_date_edit.setEnabled(titlecard_enabled)
        self._dialog._tc_duration_spin.setEnabled(titlecard_enabled)
        self._dialog._tc_logo_edit.setEnabled(titlecard_enabled)
        self._dialog._tc_logo_browse_btn.setEnabled(titlecard_enabled)
        self._dialog._tc_bg_edit.setEnabled(titlecard_enabled)
        self._dialog._tc_fg_edit.setEnabled(titlecard_enabled)
        self._dialog._tc_bg_pick_btn.setEnabled(titlecard_enabled)
        self._dialog._tc_fg_pick_btn.setEnabled(titlecard_enabled)
        self._dialog._tc_preview_frame.setEnabled(titlecard_enabled)
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
        source_paths = [entry.source_path for entry in self._dialog._draft.files if getattr(entry, "source_path", "")]
        if hasattr(self._dialog, "_merge_encoding_panel"):
            merge_context = resolve_step_input_context(
                self._dialog._draft,
                "merge",
                youtube_default_crf=(self._dialog._settings.youtube.youtube_crf if self._dialog._settings is not None else self._dialog._draft.crf),
            )
            self._dialog._merge_encoding_panel.configure_reference_labels(
                encoder_label=merge_context.encoder_label,
                crf_label=merge_context.crf_label,
                fps_label=merge_context.fps_label,
                format_label=merge_context.format_label,
                resolution_label=merge_context.resolution_label,
            )
            if merge_context.summary is not None:
                self._dialog._merge_encoding_panel._refresh_source_btn.setVisible(False)
                self._dialog._merge_encoding_panel.update_source_summary(merge_context.summary)
            else:
                self._dialog._merge_encoding_panel.update_source_material(source_paths)
        if hasattr(self._dialog, "_yt_version_panel"):
            yt_context = resolve_step_input_context(
                self._dialog._draft,
                "yt_version",
                youtube_default_crf=(self._dialog._settings.youtube.youtube_crf if self._dialog._settings is not None else self._dialog._draft.crf),
            )
            self._dialog._yt_version_panel._encoding_panel.configure_reference_labels(
                encoder_label=yt_context.encoder_label,
                crf_label=yt_context.crf_label,
                fps_label=yt_context.fps_label,
                format_label=yt_context.format_label,
                resolution_label=yt_context.resolution_label,
            )
            if yt_context.summary is not None:
                self._dialog._yt_version_panel._encoding_panel._refresh_source_btn.setVisible(False)
                self._dialog._yt_version_panel._encoding_panel.update_source_summary(yt_context.summary)
            else:
                self._dialog._yt_version_panel._encoding_panel.update_source_material(source_paths)

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