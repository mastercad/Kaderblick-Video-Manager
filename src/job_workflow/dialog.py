from __future__ import annotations

import copy

from PySide6.QtCore import QTimer, Qt
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSplitter,
    QVBoxLayout,
    QWidget,
)

from .controllers import (
    WorkflowDialogStateController,
    WorkflowEditorController,
    WorkflowExternalDataController,
    WorkflowGraphController,
)
from .graph import _NODE_DEFINITIONS, _node_visual_state, _planned_job_steps
from .panels import WorkflowDialogLayoutBuilder, WorkflowNotesPanel, WorkflowStatusPanel
from ..integrations.kaderblick import fetch_cameras, fetch_video_types
from ..media.merge_analysis import job_merge_warning
from ..settings import AppSettings
from ..workflow import WorkflowJob, describe_reset_target, describe_reset_warning, reset_job_for_rebuild
from ..integrations.youtube_title_editor import MatchData, YouTubeTitleEditorDialog


class JobWorkflowDialog(QDialog):
    def __init__(
        self,
        parent,
        job: WorkflowJob,
        *,
        allow_edit: bool = False,
        settings: AppSettings | None = None,
        allow_wizard_shortcut: bool = True,
    ):
        super().__init__(parent)
        self._job = job
        self._draft = copy.deepcopy(job)
        self._allow_edit = allow_edit
        self._allow_wizard_shortcut = allow_wizard_shortcut
        self._settings = settings
        self._ui_ready = False
        self._edit_requested = False
        self._changed = False
        self._rebuilding_graph = False
        self._kb_video_type_options: list[dict] = []
        self._kb_camera_options: list[dict] = []

        self._graph_controller = WorkflowGraphController(self)
        self._editor_controller = WorkflowEditorController(self)
        self._layout_builder = WorkflowDialogLayoutBuilder(self)
        self._state_controller = WorkflowDialogStateController(self)
        self._job_merge_warning_fn = job_merge_warning
        self._external_data = WorkflowExternalDataController(
            parent=self,
            get_settings=lambda: self._settings,
            get_file_list_widget=lambda: getattr(self, "_file_list_widget", None),
            get_pi_file_list=lambda: getattr(self, "_pi_file_list", None),
            get_kaderblick_reload_button=lambda: self._kb_reload_btn,
            get_kaderblick_status_label=lambda: self._kb_status_label,
            get_device_name=lambda: self._device_combo.currentData() or "",
            get_workflow_name=lambda: self._draft.name or self._job.name or "Workflow",
            get_pi_destination=lambda: self._pi_dest_edit.text(),
            get_pi_load_button=lambda: self._pi_load_btn,
            get_pi_load_status=lambda: self._pi_load_status,
            fetch_video_types_fn=lambda kb: fetch_video_types(kb),
            fetch_cameras_fn=lambda kb: fetch_cameras(kb),
            on_kaderblick_options_loaded=self._apply_kaderblick_options,
            on_pi_entries_loaded=self._apply_pi_camera_entries,
            on_pi_load_failed=self._on_pi_load_failed,
        )
        self._kb_load_api_data = self._external_data.load_kaderblick_api_data
        self._load_pi_camera_files = self._external_data.load_pi_camera_files
        self._on_camera_files_loaded = self._external_data.on_camera_files_loaded
        self._on_camera_files_error = self._external_data.on_camera_files_error

        self._live_update_timer = QTimer(self)
        self._live_update_timer.setInterval(200)
        self._live_update_timer.timeout.connect(self._sync_runtime_state_from_job)
        self._last_runtime_snapshot = None

        self.setWindowTitle(f"Workflow-Ansicht – {job.name or 'Workflow'}")
        self.resize(1560, 920)
        self.setMinimumSize(1220, 760)
        self._build_ui()
        self._sync_runtime_state_from_job()
        self._live_update_timer.start()

    @property
    def edit_requested(self) -> bool:
        return self._edit_requested

    @property
    def changed(self) -> bool:
        return self._changed

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)

        self._header_box = self._build_header()
        root.addWidget(self._header_box)

        self._graph_box = self._build_graph_box()
        self._summary_box = WorkflowStatusPanel(self)
        self._notes_box = WorkflowNotesPanel(self)
        self._overall_label = self._summary_box._overall_label
        self._overall_bar = self._summary_box._overall_bar
        self._current_label = self._summary_box._current_label

        main_splitter = QSplitter(Qt.Orientation.Horizontal, self)
        main_splitter.setChildrenCollapsible(False)
        main_splitter.setHandleWidth(10)
        main_splitter.setOpaqueResize(True)
        self._main_splitter = main_splitter
        if self._allow_edit:
            self._palette_box = self._build_palette_box()
            self._inspector_box = self._build_inspector_box()
            self._palette_box.setMinimumWidth(260)
            self._inspector_box.setMinimumWidth(480)
            main_splitter.addWidget(self._palette_box)
            main_splitter.addWidget(self._graph_box)
            main_splitter.addWidget(self._inspector_box)
            main_splitter.setStretchFactor(0, 2)
            main_splitter.setStretchFactor(1, 8)
            main_splitter.setStretchFactor(2, 4)
            main_splitter.setSizes([320, 980, 560])
        else:
            info_column = QWidget(self)
            info_column.setMinimumWidth(420)
            self._info_column = info_column
            info_layout = QVBoxLayout(info_column)
            info_layout.setContentsMargins(0, 0, 0, 0)
            info_layout.setSpacing(10)
            info_layout.addWidget(self._summary_box)
            info_layout.addWidget(self._notes_box)
            info_layout.addStretch()
            main_splitter.addWidget(self._graph_box)
            main_splitter.addWidget(info_column)
            main_splitter.setStretchFactor(0, 7)
            main_splitter.setStretchFactor(1, 3)
            main_splitter.setSizes([1080, 480])
        root.addWidget(main_splitter, 1)

        if self._allow_edit:
            self._load_editor_from_job()
        else:
            self._rebuild_graph_from_job()
        self._ui_ready = True
        self._refresh_dynamic_sections()

        buttons = QDialogButtonBox(self)
        if self._allow_edit:
            if self._allow_wizard_shortcut:
                edit_btn = QPushButton("Im Assistenten bearbeiten")
                edit_btn.clicked.connect(self._request_edit)
                buttons.addButton(edit_btn, QDialogButtonBox.ButtonRole.ActionRole)
            apply_btn = QPushButton("Übernehmen")
            apply_btn.clicked.connect(self._apply_and_accept)
            buttons.addButton(apply_btn, QDialogButtonBox.ButtonRole.AcceptRole)
            close_btn = buttons.addButton("Abbrechen", QDialogButtonBox.ButtonRole.RejectRole)
            close_btn.clicked.connect(self.reject)
        else:
            close_btn = buttons.addButton("Schließen", QDialogButtonBox.ButtonRole.AcceptRole)
            close_btn.clicked.connect(self.accept)
        root.addWidget(buttons)

    def _build_header(self):
        return self._layout_builder.build_header()

    def _build_graph_box(self):
        return self._layout_builder.build_graph_box()

    def _build_palette_box(self):
        return self._layout_builder.build_palette_box()

    def _build_inspector_box(self):
        return self._layout_builder.build_inspector_box()

    def _build_workflow_box(self):
        return self._layout_builder.build_workflow_box()

    def _build_empty_panel(self):
        return self._layout_builder.build_empty_panel()

    def _register_property_page(self, key: str, widget) -> None:
        self._layout_builder.register_property_page(key, widget)

    def _build_source_box(self):
        return self._layout_builder.build_source_box()

    def _build_processing_box(self):
        return self._layout_builder.build_processing_box()

    def _build_youtube_box(self):
        return self._layout_builder.build_youtube_box()

    def _build_kaderblick_box(self):
        return self._layout_builder.build_kaderblick_box()

    def _build_titlecard_box(self):
        return self._layout_builder.build_titlecard_box()

    def _build_repair_box(self):
        return self._layout_builder.build_repair_box()

    def _build_validate_surface_box(self):
        return self._layout_builder.build_validate_surface_box()

    def _build_validate_deep_box(self):
        return self._layout_builder.build_validate_deep_box()

    def _build_cleanup_box(self):
        return self._layout_builder.build_cleanup_box()

    def _build_merge_box(self):
        return self._layout_builder.build_merge_box()

    def _build_yt_version_box(self):
        return self._layout_builder.build_yt_version_box()

    def _build_stop_box(self):
        return self._layout_builder.build_stop_box()

    def _runtime_snapshot(self):
        return self._state_controller.runtime_snapshot()

    def _sync_runtime_state_from_job(self) -> None:
        self._state_controller.sync_runtime_state_from_job()

    def _request_edit(self) -> None:
        self._edit_requested = True
        self.accept()

    def _load_editor_from_job(self) -> None:
        self._editor_controller.load_editor_from_job()

    def _on_editor_changed(self, text: str) -> None:
        self._state_controller.on_editor_changed(text)

    def _sync_editor_state(
        self,
        *,
        triggered_step: str | None = None,
        sync_graph: bool = True,
    ) -> None:
        self._state_controller.sync_editor_state(
            triggered_step=triggered_step,
            sync_graph=sync_graph,
        )

    def _refresh_dynamic_sections(self) -> None:
        self._state_controller.refresh_dynamic_sections()

    def _apply_and_accept(self) -> None:
        self._editor_controller.apply_and_accept()

    def _update_bool_field(self, attr: str, value: bool) -> None:
        self._editor_controller.update_bool_field(attr, value)

    def _update_text_field(self, attr: str, value: str) -> None:
        self._editor_controller.update_text_field(attr, value)

    def _update_float_field(self, attr: str, value: float) -> None:
        self._editor_controller.update_float_field(attr, value)

    def _update_int_field(self, attr: str, value: int) -> None:
        self._editor_controller.update_int_field(attr, value)

    def _sync_kaderblick_selectors(self) -> None:
        self._editor_controller.sync_kaderblick_selectors()

    def _populate_kaderblick_combo(self, combo: QComboBox, items: list[dict], selected_id: int, label: str) -> None:
        self._editor_controller.populate_kaderblick_combo(combo, items, selected_id, label)

    def _on_kaderblick_type_changed(self, index: int) -> None:
        self._editor_controller.on_kaderblick_type_changed(index)

    def _on_kaderblick_camera_changed(self, index: int) -> None:
        self._editor_controller.on_kaderblick_camera_changed(index)

    def _on_match_data_changed(self, home: str, away: str, date_iso: str) -> None:
        self._editor_controller.on_match_data_changed(home, away, date_iso)

    def _load_merge_panel_from_draft(self) -> None:
        self._editor_controller.load_merge_panel_from_draft()

    def _load_youtube_panel_from_draft(self) -> None:
        self._editor_controller.load_youtube_panel_from_draft()

    def _sync_draft_from_merge_panel(self, *, sync_related_fields: bool = False, persist_memory: bool = False) -> None:
        self._editor_controller.sync_draft_from_merge_panel(
            sync_related_fields=sync_related_fields,
            persist_memory=persist_memory,
        )

    def _sync_draft_from_youtube_panel(self, *, sync_related_fields: bool = False, persist_memory: bool = False) -> None:
        self._editor_controller.sync_draft_from_youtube_panel(
            sync_related_fields=sync_related_fields,
            persist_memory=persist_memory,
        )

    def _on_merge_metadata_changed(self) -> None:
        self._editor_controller.on_merge_metadata_changed()

    def _on_youtube_metadata_changed(self) -> None:
        self._editor_controller.on_youtube_metadata_changed()

    def _on_files_changed(self) -> None:
        self._editor_controller.on_files_changed()

    def _on_pi_files_changed(self) -> None:
        self._editor_controller.on_pi_files_changed()

    def _on_graph_selection_changed(self, selection) -> None:
        if not self._allow_edit:
            return
        self._graph_controller.on_graph_selection_changed(selection)
        if not selection:
            self._selection_label.setText("Keine Auswahl")
            self._remove_node_btn.setEnabled(False)
            self._reset_from_node_btn.setEnabled(False)
            return
        if selection.get("kind") == "edge":
            self._selection_label.setText("Verbindung ausgewählt")
        else:
            node_type = str(selection.get("type", "")).strip()
            node_label = str(_NODE_DEFINITIONS.get(node_type, {}).get("label") or node_type or "unbekannt")
            self._selection_label.setText(node_label)
        self._remove_node_btn.setEnabled(True)
        self._reset_from_node_btn.setEnabled(selection.get("kind") == "node")

    def _on_graph_changed(self) -> None:
        self._sync_draft_from_graph()
        if self._allow_edit:
            self._sync_editor_state(sync_graph=False)

    def _remove_selected_graph_node(self) -> None:
        self._graph_view.remove_selected_item()

    def _auto_layout_graph(self) -> None:
        self._graph_controller.auto_layout_graph()

    def _reset_entire_workflow(self) -> None:
        self._run_reset_action(None)

    def _reset_from_selected_graph_node(self) -> None:
        current = self._graph_view.selected_node_id()
        if current is None:
            return
        node_type = self._graph_view.node_type(current)
        if not node_type:
            return
        self._run_reset_action(node_type)

    def _run_reset_action(self, node_type: str | None) -> None:
        label, note = describe_reset_target(self._job, node_type)
        warning = describe_reset_warning(self._job, self._settings or AppSettings(), node_type=node_type)
        prompt = f"Soll {label} zurückgesetzt werden?"
        if note:
            prompt += f"\n\n{note}"
        if warning:
            prompt += f"\n\n{warning}"
        choice = QMessageBox.question(
            self,
            "Workflow zurücksetzen",
            prompt,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if choice != QMessageBox.StandardButton.Yes:
            return

        result = reset_job_for_rebuild(self._job, self._settings or AppSettings(), node_type=node_type)
        self._changed = True
        self._sync_runtime_state_from_job()
        self._refresh_dynamic_sections()
        self._rebuild_graph_from_job()

        parent = self.parent()
        refresh_table = getattr(parent, "_refresh_table", None)
        if callable(refresh_table):
            refresh_table()
        persist_workflow_state = getattr(parent, "_persist_workflow_state", None)
        if callable(persist_workflow_state):
            persist_workflow_state()

        parts = [f"Zurückgesetzt ab {result.effective_node_type}"]
        if result.deleted_paths:
            parts.append(f"{len(result.deleted_paths)} Datei(en) entfernt")
        if result.cleared_upload_ids:
            parts.append(f"{len(result.cleared_upload_ids)} Upload-Verknüpfung(en) geleert")
        QMessageBox.information(self, "Workflow zurückgesetzt", " | ".join(parts))

    def _rebuild_graph_from_job(self, *, use_stored_graph: bool = True) -> None:
        self._graph_controller.rebuild_graph_from_job(use_stored_graph=use_stored_graph)

    def _sync_draft_from_graph(self, refresh_graph: bool = True) -> None:
        self._graph_controller.sync_draft_from_graph(refresh_graph=refresh_graph)

    def _browse_dir(self, line_edit: QLineEdit, title: str) -> None:
        settings_last_dir = self._settings.last_directory if self._settings is not None else ""
        start = line_edit.text().strip() or settings_last_dir
        chosen = QFileDialog.getExistingDirectory(self, title, start)
        if chosen:
            line_edit.setText(chosen)
            if self._settings is not None:
                self._settings.last_directory = chosen
                self._settings.save()

    def _on_file_pattern_changed(self, text: str) -> None:
        self._editor_controller.on_file_pattern_changed(text)

    def _on_encoder_changed(self, index: int) -> None:
        self._editor_controller.on_encoder_changed(index)

    def _on_merge_encoder_changed(self, index: int) -> None:
        self._editor_controller.on_merge_encoder_changed(index)

    def _on_yt_version_encoder_changed(self, index: int) -> None:
        self._editor_controller.on_yt_version_encoder_changed(index)

    def _on_amplify_toggled(self, checked: bool) -> None:
        self._editor_controller.on_amplify_toggled(checked)

    def _on_device_changed(self, index: int) -> None:
        self._editor_controller.on_device_changed(index)

    def _apply_kaderblick_options(self, video_types: list[dict], cameras: list[dict]) -> None:
        self._state_controller.apply_kaderblick_options(video_types, cameras)

    def _apply_pi_camera_entries(self, entries) -> None:
        self._state_controller.apply_pi_camera_entries(entries)

    def _on_pi_load_failed(self) -> None:
        self._state_controller.on_pi_load_failed()

    def _open_match_editor_for_playlist(self) -> None:
        initial = self._youtube_metadata_panel.current_match()
        dlg = YouTubeTitleEditorDialog(self, mode="playlist", initial_match=initial)
        if not dlg.exec():
            return
        self._youtube_metadata_panel.apply_match_data(dlg.match_data)