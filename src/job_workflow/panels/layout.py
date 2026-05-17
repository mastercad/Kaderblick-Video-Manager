from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from ..graph import _WorkflowGraphView, _WorkflowNodePalette, _workflow_editor_encoder_choices
from .inspector import (
    CleanupPanel,
    KaderblickPanel,
    ProcessingPanel,
    RepairPanel,
    SourceMaterialAnalyzer,
    StepEncodingPanel,
    StopPanel,
    TitlecardPanel,
    ValidationPanel,
    YTVersionPanel,
    YouTubeUploadPanel,
)
from .merge import MergeMetadataPanel
from .source import WorkflowSourcePanel

if TYPE_CHECKING:
    from ..dialog import JobWorkflowDialog


class WorkflowDialogLayoutBuilder:
    def __init__(self, dialog: JobWorkflowDialog):
        self._dialog = dialog
        self._source_analyzer = SourceMaterialAnalyzer()

    def build_header(self) -> QWidget:
        box = QFrame(self._dialog)
        box.setStyleSheet("QFrame { background: #F8FAFC; border: 1px solid #D7E0EA; border-radius: 12px; }")
        layout = QVBoxLayout(box)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(6)

        title_row = QHBoxLayout()
        self._dialog._title_label = QLabel(self._dialog._draft.name or "Unbenannter Workflow")
        self._dialog._title_label.setStyleSheet("font-size: 18px; font-weight: 700; color: #0F172A;")
        title_row.addWidget(self._dialog._title_label)
        title_row.addStretch()
        self._dialog._merge_warning_label = QLabel("⚠", self._dialog)
        self._dialog._merge_warning_label.setStyleSheet("color: #B45309; font-size: 18px; font-weight: 700;")
        self._dialog._merge_warning_label.setVisible(False)
        title_row.addWidget(self._dialog._merge_warning_label)
        layout.addLayout(title_row)

        self._dialog._meta_label = QLabel(self._dialog)
        self._dialog._meta_label.setStyleSheet("color: #475569;")
        layout.addWidget(self._dialog._meta_label)

        self._dialog._pipeline_label = QLabel(self._dialog)
        self._dialog._pipeline_label.setStyleSheet("color: #334155; font-weight: 600;")
        layout.addWidget(self._dialog._pipeline_label)
        return box

    def build_graph_box(self) -> QWidget:
        box = QGroupBox("Workflow-Graph", self._dialog)
        box.setStyleSheet(
            "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
        )
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)

        hint = QLabel(
            "Graphische Ansicht des Workflow-Ablaufs. Knoten lassen sich frei ziehen; Verbindungen definieren die tatsächliche Reihenfolge."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #475569;")
        layout.addWidget(hint)

        self._dialog._graph_view = _WorkflowGraphView(self._dialog)
        self._dialog._graph_view.selection_changed.connect(self._dialog._on_graph_selection_changed)
        self._dialog._graph_view.graph_changed.connect(self._dialog._on_graph_changed)
        layout.addWidget(self._dialog._graph_view, 1)
        return box

    def build_palette_box(self) -> QWidget:
        box = QGroupBox("Canvas", self._dialog)
        box.setStyleSheet(
            "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
        )
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)

        self._dialog._node_palette = _WorkflowNodePalette(self._dialog)
        layout.addWidget(self._dialog._node_palette, 1)

        self._dialog._selection_label = QLabel("Keine Auswahl", self._dialog)
        self._dialog._selection_label.setWordWrap(True)
        self._dialog._selection_label.setStyleSheet("color: #475569;")
        layout.addWidget(self._dialog._selection_label)

        btn_row = QVBoxLayout()
        self._dialog._remove_node_btn = QPushButton("Auswahl entfernen", self._dialog)
        self._dialog._remove_node_btn.clicked.connect(self._dialog._remove_selected_graph_node)
        self._dialog._remove_node_btn.setEnabled(False)
        btn_row.addWidget(self._dialog._remove_node_btn)
        self._dialog._reset_from_node_btn = QPushButton("Ab Auswahl zurücksetzen", self._dialog)
        self._dialog._reset_from_node_btn.clicked.connect(self._dialog._reset_from_selected_graph_node)
        self._dialog._reset_from_node_btn.setEnabled(False)
        btn_row.addWidget(self._dialog._reset_from_node_btn)
        auto_btn = QPushButton("Auto-Layout", self._dialog)
        auto_btn.clicked.connect(self._dialog._auto_layout_graph)
        btn_row.addWidget(auto_btn)
        layout.addLayout(btn_row)
        return box

    def build_inspector_box(self) -> QWidget:
        container = QWidget(self._dialog)
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)
        layout.addWidget(self._dialog._summary_box)
        layout.addWidget(self.build_workflow_box())

        self._dialog._property_stack = QStackedWidget(self._dialog)
        self._dialog._property_pages = {}
        self.register_property_page("default", self.build_empty_panel())
        self.register_property_page("source", self.build_source_box())
        self.register_property_page("convert", self.build_processing_box())
        self.register_property_page("merge", self.build_merge_box())
        self.register_property_page("titlecard", self.build_titlecard_box())
        self.register_property_page("validate_surface", self.build_validate_surface_box())
        self.register_property_page("validate_deep", self.build_validate_deep_box())
        self.register_property_page("cleanup", self.build_cleanup_box())
        self.register_property_page("repair", self.build_repair_box())
        self.register_property_page("yt_version", self.build_yt_version_box())
        self.register_property_page("stop", self.build_stop_box())
        self.register_property_page("youtube_upload", self.build_youtube_box())
        self.register_property_page("kaderblick", self.build_kaderblick_box())
        property_scroll = QScrollArea(self._dialog)
        property_scroll.setWidgetResizable(True)
        property_scroll.setFrameShape(QFrame.Shape.NoFrame)
        property_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        property_content = QWidget(self._dialog)
        property_layout = QVBoxLayout(property_content)
        property_layout.setContentsMargins(0, 0, 0, 0)
        property_layout.setSpacing(10)
        property_layout.addWidget(self._dialog._property_stack)
        property_layout.addWidget(self._dialog._notes_box)
        property_layout.addStretch()

        property_scroll.setWidget(property_content)
        layout.addWidget(property_scroll, 1)
        return container

    def build_workflow_box(self) -> QWidget:
        box = QGroupBox("Workflow", self._dialog)
        box.setStyleSheet(
            "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
        )
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(10)

        name_row = QHBoxLayout()
        name_row.addWidget(QLabel("Name:"))
        self._dialog._name_edit = QLineEdit(self._dialog._draft.name)
        self._dialog._name_edit.setPlaceholderText("Anzeigename für den Workflow")
        self._dialog._name_edit.textChanged.connect(self._dialog._on_editor_changed)
        name_row.addWidget(self._dialog._name_edit, 1)
        layout.addLayout(name_row)

        self._dialog._overwrite_cb = QCheckBox("Vorhandene Ergebnisse überschreiben", self._dialog)
        self._dialog._overwrite_cb.toggled.connect(
            lambda checked: self._dialog._update_bool_field("overwrite", checked)
        )
        layout.addWidget(self._dialog._overwrite_cb)

        self._dialog._merge_label = QLabel(self._dialog)
        self._dialog._merge_label.setWordWrap(True)
        self._dialog._merge_label.setStyleSheet("color: #92400E; font-weight: 600;")
        layout.addWidget(self._dialog._merge_label)

        self._dialog._editor_hint = QLabel(self._dialog)
        self._dialog._editor_hint.setWordWrap(True)
        self._dialog._editor_hint.setStyleSheet("color: #475569;")
        layout.addWidget(self._dialog._editor_hint)

        self._dialog._reset_workflow_btn = QPushButton("Workflow zurücksetzen", self._dialog)
        self._dialog._reset_workflow_btn.clicked.connect(self._dialog._reset_entire_workflow)
        layout.addWidget(self._dialog._reset_workflow_btn)
        return box

    def build_empty_panel(self) -> QWidget:
        panel = QWidget(self._dialog)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        label = QLabel("Node auswählen, um rechts die passenden Einstellungen zu bearbeiten.")
        label.setWordWrap(True)
        label.setStyleSheet("color: #475569;")
        layout.addWidget(label)
        layout.addStretch()
        return panel

    def register_property_page(self, key: str, widget: QWidget) -> None:
        self._dialog._property_pages[key] = self._dialog._property_stack.addWidget(widget)

    def _wrap_compact_property_panel(self, widget: QWidget) -> QWidget:
        container = QWidget(self._dialog)
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        layout.addWidget(widget)
        layout.addStretch()
        return container

    def build_source_box(self) -> QWidget:
        self._dialog._source_panel = WorkflowSourcePanel(
            self._dialog,
            settings=self._dialog._settings,
            update_text_field=self._dialog._update_text_field,
            update_bool_field=self._dialog._update_bool_field,
            on_file_pattern_changed=self._dialog._on_file_pattern_changed,
            on_device_changed=self._dialog._on_device_changed,
            on_files_changed=self._dialog._on_files_changed,
            on_pi_files_changed=self._dialog._on_pi_files_changed,
            on_match_data_changed=self._dialog._on_match_data_changed,
            on_load_pi_camera_files=self._dialog._load_pi_camera_files,
        )
        self._dialog._source_mode_label = self._dialog._source_panel._source_mode_label
        self._dialog._source_detail_label = self._dialog._source_panel._source_detail_label
        self._dialog._source_mode_widgets = self._dialog._source_panel._source_mode_widgets
        self._dialog._file_list_widget = self._dialog._source_panel._file_list_widget
        self._dialog._files_dst_edit = self._dialog._source_panel._files_dst_edit
        self._dialog._files_move_cb = self._dialog._source_panel._files_move_cb
        self._dialog._folder_src_edit = self._dialog._source_panel._folder_src_edit
        self._dialog._file_pattern_edit = self._dialog._source_panel._file_pattern_edit
        self._dialog._folder_dst_edit = self._dialog._source_panel._folder_dst_edit
        self._dialog._move_files_cb = self._dialog._source_panel._move_files_cb
        self._dialog._folder_prefix_edit = self._dialog._source_panel._folder_prefix_edit
        self._dialog._device_combo = self._dialog._source_panel._device_combo
        self._dialog._pi_dest_edit = self._dialog._source_panel._pi_dest_edit
        self._dialog._delete_after_dl_cb = self._dialog._source_panel._delete_after_dl_cb
        self._dialog._pi_prefix_edit = self._dialog._source_panel._pi_prefix_edit
        self._dialog._pi_load_btn = self._dialog._source_panel._pi_load_btn
        self._dialog._pi_load_status = self._dialog._source_panel._pi_load_status
        self._dialog._pi_file_list = self._dialog._source_panel._pi_file_list
        self._dialog._source_panel.set_mode(self._dialog._draft.source_mode)
        return self._wrap_compact_property_panel(self._dialog._source_panel)

    def build_processing_box(self) -> QWidget:
        self._dialog._processing_panel = ProcessingPanel(
            self._dialog,
            encoder_choices=_workflow_editor_encoder_choices(),
            on_crf_changed=lambda value: self._dialog._update_int_field("crf", value),
            on_encoder_changed=self._dialog._on_encoder_changed,
            on_preset_changed=lambda text: self._dialog._update_text_field("preset", text),
            on_no_bframes_changed=lambda checked: self._dialog._update_bool_field("no_bframes", checked),
            on_fps_changed=lambda value: self._dialog._update_int_field("fps", value),
            on_format_changed=lambda text: self._dialog._update_text_field("output_format", text),
            on_resolution_changed=lambda text: self._dialog._update_text_field("output_resolution", text),
            on_merge_audio_changed=lambda checked: self._dialog._update_bool_field("merge_audio", checked),
            on_amplify_toggled=self._dialog._on_amplify_toggled,
            on_amplify_db_changed=lambda value: self._dialog._update_float_field("amplify_db", value),
            on_audio_sync_changed=lambda checked: self._dialog._update_bool_field("audio_sync", checked),
        )
        self._dialog._crf_spin = self._dialog._processing_panel._crf_spin
        self._dialog._encoder_combo = self._dialog._processing_panel._encoder_combo
        self._dialog._preset_combo = self._dialog._processing_panel._preset_combo
        self._dialog._no_bframes_cb = self._dialog._processing_panel._no_bframes_cb
        self._dialog._fps_spin = self._dialog._processing_panel._fps_spin
        self._dialog._resolution_combo = self._dialog._processing_panel._resolution_combo
        self._dialog._format_combo = self._dialog._processing_panel._format_combo
        self._dialog._merge_audio_cb = self._dialog._processing_panel._merge_audio_cb
        self._dialog._amplify_audio_cb = self._dialog._processing_panel._amplify_audio_cb
        self._dialog._amplify_db_spin = self._dialog._processing_panel._amplify_db_spin
        self._dialog._audio_sync_cb = self._dialog._processing_panel._audio_sync_cb
        return self._dialog._processing_panel

    def build_validate_surface_box(self) -> QWidget:
        return ValidationPanel(
            "Quick-Check",
            "Prüft schnell, ob das aktuelle Artefakt grundsätzlich lesbar und für die weitere Verarbeitung kompatibel ist."
        )

    def build_validate_deep_box(self) -> QWidget:
        return ValidationPanel(
            "Deep-Scan",
            "Dekodiert die Datei vollständig, prüft Warnungen, Zeitstempel und vergleicht bei Bedarf die gelesene Frame-Anzahl gegen die erwartete Laufzeit."
        )

    def build_cleanup_box(self) -> QWidget:
        self._dialog._cleanup_panel = CleanupPanel(self._dialog)
        return self._dialog._cleanup_panel

    def build_youtube_box(self) -> QWidget:
        self._dialog._youtube_panel = YouTubeUploadPanel(
            self._dialog,
            settings=self._dialog._settings,
            on_metadata_changed=self._dialog._on_youtube_metadata_changed,
            on_playlist_helper=self._dialog._open_match_editor_for_playlist,
        )
        self._dialog._playlist_helper_btn = self._dialog._youtube_panel._playlist_helper_btn
        self._dialog._youtube_metadata_panel = self._dialog._youtube_panel._metadata_panel
        return self._dialog._youtube_panel

    def build_kaderblick_box(self) -> QWidget:
        self._dialog._kaderblick_panel = KaderblickPanel(
            self._dialog,
            settings=self._dialog._settings,
            on_game_id_changed=lambda text: self._dialog._update_text_field("default_kaderblick_game_id", text),
            on_type_changed=self._dialog._on_kaderblick_type_changed,
            on_camera_changed=self._dialog._on_kaderblick_camera_changed,
            on_reload=lambda: self._dialog._kb_load_api_data(force=True),
        )
        self._dialog._kb_game_id_edit = self._dialog._kaderblick_panel._kb_game_id_edit
        self._dialog._kb_type_combo = self._dialog._kaderblick_panel._kb_type_combo
        self._dialog._kb_camera_combo = self._dialog._kaderblick_panel._kb_camera_combo
        self._dialog._kb_reload_btn = self._dialog._kaderblick_panel._kb_reload_btn
        self._dialog._kb_status_label = self._dialog._kaderblick_panel._kb_status_label
        return self._dialog._kaderblick_panel

    def build_titlecard_box(self) -> QWidget:
        self._dialog._titlecard_panel = TitlecardPanel(
            self._dialog,
            on_home_changed=lambda text: self._dialog._update_text_field("title_card_home_team", text),
            on_away_changed=lambda text: self._dialog._update_text_field("title_card_away_team", text),
            on_date_changed=lambda text: self._dialog._update_text_field("title_card_date", text),
            on_duration_changed=lambda value: self._dialog._update_float_field("title_card_duration", value),
            on_logo_changed=lambda text: self._dialog._update_text_field("title_card_logo_path", text),
            on_bg_changed=lambda text: self._dialog._update_text_field("title_card_bg_color", text),
            on_fg_changed=lambda text: self._dialog._update_text_field("title_card_fg_color", text),
        )
        self._dialog._tc_home_edit = self._dialog._titlecard_panel._tc_home_edit
        self._dialog._tc_away_edit = self._dialog._titlecard_panel._tc_away_edit
        self._dialog._tc_date_edit = self._dialog._titlecard_panel._tc_date_edit
        self._dialog._tc_duration_spin = self._dialog._titlecard_panel._tc_duration_spin
        self._dialog._tc_logo_edit = self._dialog._titlecard_panel._tc_logo_edit
        self._dialog._tc_logo_browse_btn = self._dialog._titlecard_panel._tc_logo_browse_btn
        self._dialog._tc_bg_edit = self._dialog._titlecard_panel._tc_bg_edit
        self._dialog._tc_fg_edit = self._dialog._titlecard_panel._tc_fg_edit
        self._dialog._tc_bg_pick_btn = self._dialog._titlecard_panel._tc_bg_pick_btn
        self._dialog._tc_fg_pick_btn = self._dialog._titlecard_panel._tc_fg_pick_btn
        self._dialog._tc_preview_frame = self._dialog._titlecard_panel._tc_preview_frame
        return self._dialog._titlecard_panel

    def build_repair_box(self) -> QWidget:
        self._dialog._repair_panel = RepairPanel(self._dialog)
        return self._dialog._repair_panel

    def build_merge_box(self) -> QWidget:
        box = QGroupBox("Zusammenführen", self._dialog)
        box.setStyleSheet(
            "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
        )
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)
        info = QLabel(
            "Hier pflegst du die gemeinsamen Ausgabe-Metadaten des Merge-Ergebnisses. "
            "Daraus werden Titel, Playlist, Beschreibung und standardmäßig auch der lokale Dateiname des zusammengeführten Videos abgeleitet. "
            "Wenn später ein YouTube-Upload folgt, übernimmt der Upload dieselben Daten ohne zweite Pflegestelle."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: #475569;")
        layout.addWidget(info)
        self._dialog._merge_encoding_panel = StepEncodingPanel(
            "Merge-Encoding",
            self._dialog,
            source_analyzer=self._source_analyzer,
            encoder_choices=_workflow_editor_encoder_choices(),
            on_crf_changed=lambda value: self._dialog._update_int_field("merge_crf", value),
            on_encoder_changed=self._dialog._on_merge_encoder_changed,
            on_preset_changed=lambda text: self._dialog._update_text_field("merge_preset", text),
            on_no_bframes_changed=lambda checked: self._dialog._update_bool_field("merge_no_bframes", checked),
            on_fps_changed=lambda value: self._dialog._update_int_field("merge_fps", value),
            on_format_changed=lambda text: self._dialog._update_text_field("merge_output_format", text),
            on_resolution_changed=lambda text: self._dialog._update_text_field("merge_output_resolution", text),
        )
        layout.addWidget(self._dialog._merge_encoding_panel)
        self._dialog._merge_panel = MergeMetadataPanel(self._dialog, settings=self._dialog._settings)
        self._dialog._merge_panel.metadata_changed.connect(self._dialog._on_merge_metadata_changed)
        layout.addWidget(self._dialog._merge_panel)
        return box

    def build_yt_version_box(self) -> QWidget:
        self._dialog._yt_version_panel = YTVersionPanel(
            self._dialog,
            source_analyzer=self._source_analyzer,
            encoder_choices=_workflow_editor_encoder_choices(),
            on_crf_changed=lambda value: self._dialog._update_int_field("yt_version_crf", value),
            on_encoder_changed=self._dialog._on_yt_version_encoder_changed,
            on_preset_changed=lambda text: self._dialog._update_text_field("yt_version_preset", text),
            on_no_bframes_changed=lambda checked: self._dialog._update_bool_field("yt_version_no_bframes", checked),
            on_fps_changed=lambda value: self._dialog._update_int_field("yt_version_fps", value),
            on_format_changed=lambda text: self._dialog._update_text_field("yt_version_output_format", text),
            on_resolution_changed=lambda text: self._dialog._update_text_field("yt_version_output_resolution", text),
        )
        return self._dialog._yt_version_panel

    def build_stop_box(self) -> QWidget:
        self._dialog._stop_panel = StopPanel(self._dialog)
        return self._dialog._stop_panel