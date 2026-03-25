from __future__ import annotations

import copy
from datetime import date
from pathlib import Path

from ...integrations.youtube_title_editor import MatchData, load_memory
from .state import resolve_step_input_context


class WorkflowEditorController:
    def __init__(self, dialog):
        self._dialog = dialog

    def refresh_titlecard_preview_context(self) -> None:
        dialog = self._dialog
        if not hasattr(dialog, "_titlecard_panel"):
            return
        subtitle = ""
        for entry in dialog._draft.files:
            subtitle = str(getattr(entry, "title_card_subtitle", "") or "").strip()
            if subtitle:
                break
            source_path = str(getattr(entry, "source_path", "") or "").strip()
            if source_path:
                subtitle = Path(source_path).stem
                break
        dialog._titlecard_panel.set_preview_subtitle(subtitle)

    def _resolved_kaderblick_target_ids(self) -> tuple[int, int]:
        dialog = self._dialog
        if hasattr(dialog, "_youtube_panel") and dialog._youtube_panel.is_merge_output_mode():
            return (
                int(dialog._draft.merge_output_kaderblick_video_type_id or 0),
                int(dialog._draft.merge_output_kaderblick_camera_id or 0),
            )
        return (
            int(dialog._draft.youtube_kaderblick_video_type_id or dialog._draft.default_kaderblick_video_type_id or 0),
            int(dialog._draft.youtube_kaderblick_camera_id or dialog._draft.default_kaderblick_camera_id or 0),
        )

    def _sync_kaderblick_defaults_from_metadata(self) -> None:
        video_type_id, camera_id = self._resolved_kaderblick_target_ids()
        self._dialog._draft.default_kaderblick_video_type_id = video_type_id
        self._dialog._draft.default_kaderblick_camera_id = camera_id

    def _kaderblick_option_items(self) -> tuple[list[dict], list[dict]]:
        dialog = self._dialog
        video_types = list(dialog._kb_video_type_options)
        cameras = list(dialog._kb_camera_options)
        if video_types or cameras:
            return video_types, cameras
        if hasattr(dialog, "_merge_panel") and (dialog._merge_panel._kb_video_types or dialog._merge_panel._kb_cameras):
            return list(dialog._merge_panel._kb_video_types), list(dialog._merge_panel._kb_cameras)
        if hasattr(dialog, "_youtube_metadata_panel") and (
            dialog._youtube_metadata_panel._kb_video_types or dialog._youtube_metadata_panel._kb_cameras
        ):
            return (
                list(dialog._youtube_metadata_panel._kb_video_types),
                list(dialog._youtube_metadata_panel._kb_cameras),
            )
        return [], []

    def refresh_kaderblick_mirror(self) -> None:
        dialog = self._dialog
        selected_video_type_id, selected_camera_id = self._resolved_kaderblick_target_ids()
        self._sync_kaderblick_defaults_from_metadata()
        video_types, cameras = self._kaderblick_option_items()
        self.populate_kaderblick_combo(
            dialog._kb_type_combo,
            video_types,
            selected_video_type_id,
            "Video-Typ",
        )
        self.populate_kaderblick_combo(
            dialog._kb_camera_combo,
            cameras,
            selected_camera_id,
            "Kamera",
        )

    def _fallback_match(self) -> MatchData:
        dialog = self._dialog
        memory = load_memory()
        last_match = memory.get("last_match", {})
        return MatchData(
            date_iso=dialog._draft.title_card_date.strip() or date.today().isoformat(),
            competition=dialog._draft.default_youtube_competition.strip() or str(last_match.get("competition", "")),
            home_team=dialog._draft.title_card_home_team.strip() or str(last_match.get("home_team", "")),
            away_team=dialog._draft.title_card_away_team.strip() or str(last_match.get("away_team", "")),
        )

    def load_editor_from_job(self) -> None:
        dialog = self._dialog
        draft = dialog._draft
        dialog._rebuilding_graph = True
        dialog._name_edit.setText(draft.name)
        dialog._overwrite_cb.setChecked(draft.overwrite)
        dialog._kb_game_id_edit.setText(draft.default_kaderblick_game_id)
        fallback_match = self._fallback_match()
        dialog._tc_home_edit.setText(draft.title_card_home_team or fallback_match.home_team)
        dialog._tc_away_edit.setText(draft.title_card_away_team or fallback_match.away_team)
        dialog._tc_date_edit.setText(draft.title_card_date or fallback_match.date_iso)
        dialog._tc_duration_spin.setValue(draft.title_card_duration)
        dialog._tc_logo_edit.setText(draft.title_card_logo_path)
        dialog._tc_bg_edit.setText(draft.title_card_bg_color or "#000000")
        dialog._tc_fg_edit.setText(draft.title_card_fg_color or "#FFFFFF")
        self.refresh_titlecard_preview_context()
        dialog._crf_spin.setValue(draft.crf)
        dialog._preset_combo.setCurrentText(draft.preset)
        dialog._no_bframes_cb.setChecked(draft.no_bframes)
        dialog._fps_spin.setValue(draft.fps)
        dialog._resolution_combo.setCurrentIndex(max(dialog._resolution_combo.findData(draft.output_resolution), 0))
        dialog._format_combo.setCurrentIndex(max(dialog._format_combo.findData(draft.output_format), 0))
        dialog._merge_audio_cb.setChecked(draft.merge_audio)
        dialog._amplify_audio_cb.setChecked(draft.amplify_audio)
        dialog._amplify_db_spin.setValue(draft.amplify_db)
        dialog._audio_sync_cb.setChecked(draft.audio_sync)
        encoder_index = dialog._encoder_combo.findData(draft.encoder)
        dialog._encoder_combo.setCurrentIndex(encoder_index if encoder_index >= 0 else 0)
        dialog._processing_panel.sync_profile_from_values()
        dialog._source_panel.load_from_job(draft)
        if hasattr(dialog, "_merge_encoding_panel"):
            merge_context = resolve_step_input_context(
                draft,
                "merge",
                youtube_default_crf=(dialog._settings.youtube.youtube_crf if dialog._settings is not None else draft.crf),
            )
            dialog._merge_encoding_panel.configure_reference_labels(
                encoder_label=merge_context.encoder_label,
                crf_label=merge_context.crf_label,
                fps_label=merge_context.fps_label,
                format_label=merge_context.format_label,
                resolution_label=merge_context.resolution_label,
            )
            dialog._merge_encoding_panel.load_values(
                encoder=draft.merge_encoder,
                crf=draft.merge_crf,
                preset=draft.merge_preset,
                no_bframes=draft.merge_no_bframes,
                fps=draft.merge_fps,
                output_format=draft.merge_output_format,
                output_resolution=draft.merge_output_resolution,
                base_encoder=merge_context.base_encoder,
                base_crf=merge_context.base_crf,
            )
            if merge_context.summary is not None:
                dialog._merge_encoding_panel._refresh_source_btn.setVisible(False)
                dialog._merge_encoding_panel.update_source_summary(merge_context.summary)
            else:
                dialog._merge_encoding_panel.update_source_material([entry.source_path for entry in draft.files])
        if hasattr(dialog, "_yt_version_panel"):
            yt_context = resolve_step_input_context(
                draft,
                "yt_version",
                youtube_default_crf=(dialog._settings.youtube.youtube_crf if dialog._settings is not None else draft.crf),
            )
            dialog._yt_version_panel._encoding_panel.configure_reference_labels(
                encoder_label=yt_context.encoder_label,
                crf_label=yt_context.crf_label,
                fps_label=yt_context.fps_label,
                format_label=yt_context.format_label,
                resolution_label=yt_context.resolution_label,
            )
            dialog._yt_version_panel.load_values(
                encoder=draft.yt_version_encoder,
                crf=draft.yt_version_crf,
                preset=draft.yt_version_preset,
                no_bframes=draft.yt_version_no_bframes,
                fps=draft.yt_version_fps,
                output_format=draft.yt_version_output_format,
                output_resolution=draft.yt_version_output_resolution,
                base_encoder=yt_context.base_encoder,
                base_crf=yt_context.base_crf,
            )
            if yt_context.summary is not None:
                dialog._yt_version_panel._encoding_panel._refresh_source_btn.setVisible(False)
                dialog._yt_version_panel._encoding_panel.update_source_summary(yt_context.summary)
            else:
                dialog._yt_version_panel._encoding_panel.update_source_material([entry.source_path for entry in draft.files])
        self.sync_kaderblick_selectors()
        self.load_merge_panel_from_draft()
        self.load_youtube_panel_from_draft()
        dialog._rebuilding_graph = False
        dialog._rebuild_graph_from_job()
        dialog._sync_editor_state()

    def apply_and_accept(self) -> None:
        dialog = self._dialog
        draft = dialog._draft
        draft.name = dialog._name_edit.text().strip()
        dialog._sync_draft_from_graph(refresh_graph=False)
        self.sync_draft_from_merge_panel(sync_related_fields=True, persist_memory=True)
        self.sync_draft_from_youtube_panel(sync_related_fields=True, persist_memory=True)
        if draft.source_mode == "files" and dialog._file_list_widget is not None:
            draft.files = dialog._file_list_widget.collect()
        elif draft.source_mode == "pi_download" and hasattr(dialog, "_pi_file_list"):
            draft.files = dialog._pi_file_list.collect() if not dialog._pi_file_list.is_empty() else []

        for attr in (
            "name",
            "source_mode",
            "copy_destination",
            "move_files",
            "source_folder",
            "file_pattern",
            "output_prefix",
            "device_name",
            "download_destination",
            "delete_after_download",
            "convert_enabled",
            "title_card_enabled",
            "create_youtube_version",
            "upload_youtube",
            "upload_kaderblick",
            "overwrite",
            "default_youtube_title",
            "default_youtube_playlist",
            "default_youtube_description",
            "default_youtube_competition",
            "youtube_match_data",
            "youtube_segment_data",
            "youtube_kaderblick_video_type_id",
            "youtube_kaderblick_camera_id",
            "merge_output_title",
            "merge_output_playlist",
            "merge_output_description",
            "default_kaderblick_game_id",
            "default_kaderblick_video_type_id",
            "default_kaderblick_camera_id",
            "encoder",
            "preset",
            "no_bframes",
            "title_card_home_team",
            "title_card_away_team",
            "title_card_date",
            "title_card_duration",
            "title_card_logo_path",
            "title_card_bg_color",
            "title_card_fg_color",
            "crf",
            "fps",
            "output_resolution",
            "output_format",
            "merge_audio",
            "amplify_audio",
            "amplify_db",
            "audio_sync",
            "merge_encoder",
            "merge_crf",
            "merge_preset",
            "merge_no_bframes",
            "merge_fps",
            "merge_output_format",
            "merge_output_resolution",
            "yt_version_encoder",
            "yt_version_crf",
            "yt_version_preset",
            "yt_version_no_bframes",
            "yt_version_fps",
            "yt_version_output_format",
            "yt_version_output_resolution",
        ):
            setattr(dialog._job, attr, copy.deepcopy(getattr(draft, attr)))
        dialog._job.merge_match_data = copy.deepcopy(draft.merge_match_data)
        dialog._job.merge_segment_data = copy.deepcopy(draft.merge_segment_data)
        dialog._job.merge_output_kaderblick_video_type_id = draft.merge_output_kaderblick_video_type_id
        dialog._job.merge_output_kaderblick_camera_id = draft.merge_output_kaderblick_camera_id
        dialog._job.graph_nodes = copy.deepcopy(draft.graph_nodes)
        dialog._job.graph_edges = copy.deepcopy(draft.graph_edges)
        dialog._job.files = copy.deepcopy(draft.files)
        dialog._changed = True
        dialog.accept()

    def update_bool_field(self, attr: str, value: bool) -> None:
        setattr(self._dialog._draft, attr, value)
        self._dialog._sync_editor_state()

    def update_text_field(self, attr: str, value: str) -> None:
        setattr(self._dialog._draft, attr, value.strip())
        self._dialog._refresh_dynamic_sections()

    def update_float_field(self, attr: str, value: float) -> None:
        setattr(self._dialog._draft, attr, float(value))
        self._dialog._refresh_dynamic_sections()

    def update_int_field(self, attr: str, value: int) -> None:
        setattr(self._dialog._draft, attr, int(value))
        self._dialog._refresh_dynamic_sections()

    def sync_kaderblick_selectors(self) -> None:
        self.refresh_kaderblick_mirror()
        self.load_merge_panel_from_draft()
        self.load_youtube_panel_from_draft()

    @staticmethod
    def populate_kaderblick_combo(combo, items: list[dict], selected_id: int, label: str) -> None:
        combo.blockSignals(True)
        combo.clear()
        combo.addItem(f"({label} nicht gesetzt)", 0)
        found_selected = selected_id == 0
        for item in items:
            item_id = int(item.get("id") or 0)
            item_name = str(item.get("name") or item.get("label") or f"{label} {item_id}")
            combo.addItem(item_name, item_id)
            if item_id == selected_id:
                found_selected = True
        if selected_id and not found_selected:
            combo.addItem(f"Unbekannt ({selected_id})", selected_id)
        combo.setCurrentIndex(max(combo.findData(selected_id), 0))
        combo.blockSignals(False)

    def on_kaderblick_type_changed(self, index: int) -> None:
        dialog = self._dialog
        dialog._draft.default_kaderblick_video_type_id = int(dialog._kb_type_combo.itemData(index) or 0)
        dialog._refresh_dynamic_sections()

    def on_kaderblick_camera_changed(self, index: int) -> None:
        dialog = self._dialog
        dialog._draft.default_kaderblick_camera_id = int(dialog._kb_camera_combo.itemData(index) or 0)
        dialog._refresh_dynamic_sections()

    def on_merge_encoder_changed(self, index: int) -> None:
        dialog = self._dialog
        dialog._draft.merge_encoder = str(dialog._merge_encoding_panel._encoder_combo.itemData(index) or "inherit")
        dialog._refresh_dynamic_sections()

    def on_yt_version_encoder_changed(self, index: int) -> None:
        dialog = self._dialog
        dialog._draft.yt_version_encoder = str(dialog._yt_version_panel._encoding_panel._encoder_combo.itemData(index) or "inherit")
        dialog._refresh_dynamic_sections()

    def on_match_data_changed(self, home: str, away: str, date_iso: str) -> None:
        dialog = self._dialog
        if home:
            dialog._draft.title_card_home_team = home
            dialog._tc_home_edit.setText(home)
        if away:
            dialog._draft.title_card_away_team = away
            dialog._tc_away_edit.setText(away)
        if date_iso:
            dialog._draft.title_card_date = date_iso
            dialog._tc_date_edit.setText(date_iso)
        dialog._refresh_dynamic_sections()

    def load_merge_panel_from_draft(self) -> None:
        dialog = self._dialog
        if not hasattr(dialog, "_merge_panel"):
            return
        dialog._loading_merge_metadata = True
        try:
            dialog._merge_panel.set_kaderblick_options(dialog._kb_video_type_options, dialog._kb_camera_options)
            dialog._merge_panel.load_from_job(
                match_data=dialog._draft.merge_match_data,
                segment_data=dialog._draft.merge_segment_data,
                kb_type_id=dialog._draft.merge_output_kaderblick_video_type_id,
                kb_camera_id=dialog._draft.merge_output_kaderblick_camera_id,
                fallback_match=self._fallback_match(),
                fallback_title=dialog._draft.merge_output_title,
                fallback_playlist=dialog._draft.merge_output_playlist,
                fallback_description=dialog._draft.merge_output_description,
            )
        finally:
            dialog._loading_merge_metadata = False

    def load_youtube_panel_from_draft(self) -> None:
        dialog = self._dialog
        if not hasattr(dialog, "_youtube_metadata_panel"):
            return
        dialog._loading_youtube_metadata = True
        try:
            panel = dialog._youtube_metadata_panel
            panel.set_kaderblick_options(dialog._kb_video_type_options, dialog._kb_camera_options)
            if dialog._youtube_panel.is_merge_output_mode():
                panel.load_from_job(
                    match_data=dialog._draft.merge_match_data,
                    segment_data=dialog._draft.merge_segment_data,
                    kb_type_id=dialog._draft.merge_output_kaderblick_video_type_id,
                    kb_camera_id=dialog._draft.merge_output_kaderblick_camera_id,
                    fallback_match=self._fallback_match(),
                    fallback_title=dialog._draft.merge_output_title,
                    fallback_playlist=dialog._draft.merge_output_playlist,
                    fallback_description=dialog._draft.merge_output_description,
                )
                return
            panel.load_from_job(
                match_data=dialog._draft.youtube_match_data,
                segment_data=dialog._draft.youtube_segment_data,
                kb_type_id=dialog._draft.youtube_kaderblick_video_type_id,
                kb_camera_id=dialog._draft.youtube_kaderblick_camera_id,
                fallback_match=self._fallback_match(),
                fallback_title=dialog._draft.default_youtube_title,
                fallback_playlist=dialog._draft.default_youtube_playlist,
                fallback_description=dialog._draft.default_youtube_description,
            )
        finally:
            dialog._loading_youtube_metadata = False

    def sync_draft_from_merge_panel(self, *, sync_related_fields: bool = False, persist_memory: bool = False) -> None:
        dialog = self._dialog
        if not hasattr(dialog, "_merge_panel"):
            return
        has_merge_output = any(getattr(entry, "merge_group_id", "") for entry in dialog._draft.files)
        if not has_merge_output:
            dialog._draft.merge_output_title = ""
            dialog._draft.merge_output_playlist = ""
            dialog._draft.merge_output_description = ""
            dialog._draft.merge_match_data = {}
            dialog._draft.merge_segment_data = {}
            dialog._draft.merge_output_kaderblick_video_type_id = 0
            dialog._draft.merge_output_kaderblick_camera_id = 0
            return
        state = dialog._merge_panel.export_state()
        dialog._draft.merge_output_title = str(state.get("merge_output_title") or "")
        dialog._draft.merge_output_playlist = str(state.get("merge_output_playlist") or "")
        dialog._draft.merge_output_description = str(state.get("merge_output_description") or "")
        dialog._draft.merge_match_data = copy.deepcopy(state.get("merge_match_data") or {})
        dialog._draft.merge_segment_data = copy.deepcopy(state.get("merge_segment_data") or {})
        dialog._draft.merge_output_kaderblick_video_type_id = int(state.get("merge_output_kaderblick_video_type_id") or 0)
        dialog._draft.merge_output_kaderblick_camera_id = int(state.get("merge_output_kaderblick_camera_id") or 0)
        self._sync_kaderblick_defaults_from_metadata()

        if sync_related_fields:
            match = dialog._merge_panel.current_match()
            if match.competition:
                dialog._draft.default_youtube_competition = match.competition
            if match.home_team:
                dialog._draft.title_card_home_team = match.home_team
            if match.away_team:
                dialog._draft.title_card_away_team = match.away_team
            if match.date_iso:
                dialog._draft.title_card_date = match.date_iso

        if persist_memory:
            dialog._merge_panel.persist_memory()

    def on_merge_metadata_changed(self) -> None:
        if getattr(self._dialog, "_loading_merge_metadata", False):
            return
        self.sync_draft_from_merge_panel()
        self.refresh_kaderblick_mirror()
        if hasattr(self._dialog, "_youtube_panel") and self._dialog._youtube_panel.is_merge_output_mode():
            self.load_youtube_panel_from_draft()
        self._dialog._refresh_dynamic_sections()

    def sync_draft_from_youtube_panel(self, *, sync_related_fields: bool = False, persist_memory: bool = False) -> None:
        dialog = self._dialog
        if not hasattr(dialog, "_youtube_metadata_panel") or dialog._youtube_panel.is_merge_output_mode():
            return
        state = dialog._youtube_metadata_panel.export_state()
        dialog._draft.default_youtube_title = str(state.get("title") or "")
        dialog._draft.default_youtube_playlist = str(state.get("playlist") or "")
        dialog._draft.default_youtube_description = str(state.get("description") or "")
        dialog._draft.youtube_match_data = copy.deepcopy(state.get("match_data") or {})
        dialog._draft.youtube_segment_data = copy.deepcopy(state.get("segment_data") or {})
        dialog._draft.youtube_kaderblick_video_type_id = int(state.get("kaderblick_video_type_id") or 0)
        dialog._draft.youtube_kaderblick_camera_id = int(state.get("kaderblick_camera_id") or 0)
        self._sync_kaderblick_defaults_from_metadata()

        match = dialog._youtube_metadata_panel.current_match()
        dialog._draft.default_youtube_competition = match.competition

        if sync_related_fields:
            if match.home_team:
                dialog._draft.title_card_home_team = match.home_team
            if match.away_team:
                dialog._draft.title_card_away_team = match.away_team
            if match.date_iso:
                dialog._draft.title_card_date = match.date_iso

        if persist_memory:
            dialog._youtube_metadata_panel.persist_memory()

    def on_youtube_metadata_changed(self) -> None:
        if getattr(self._dialog, "_loading_youtube_metadata", False):
            return
        self.sync_draft_from_youtube_panel(sync_related_fields=True)
        self.refresh_kaderblick_mirror()
        if self._dialog._draft.title_card_home_team != self._dialog._tc_home_edit.text():
            self._dialog._tc_home_edit.setText(self._dialog._draft.title_card_home_team)
        if self._dialog._draft.title_card_away_team != self._dialog._tc_away_edit.text():
            self._dialog._tc_away_edit.setText(self._dialog._draft.title_card_away_team)
        if self._dialog._draft.title_card_date != self._dialog._tc_date_edit.text():
            self._dialog._tc_date_edit.setText(self._dialog._draft.title_card_date)
        self._dialog._refresh_dynamic_sections()

    def on_files_changed(self) -> None:
        dialog = self._dialog
        if dialog._file_list_widget is None:
            return
        dialog._draft.files = dialog._file_list_widget.collect()
        self.refresh_titlecard_preview_context()
        dialog._sync_editor_state()

    def on_pi_files_changed(self) -> None:
        dialog = self._dialog
        if not hasattr(dialog, "_pi_file_list"):
            return
        dialog._draft.files = dialog._pi_file_list.collect()
        self.refresh_titlecard_preview_context()
        dialog._sync_editor_state()

    def on_file_pattern_changed(self, text: str) -> None:
        self._dialog._draft.file_pattern = text.strip() or "*.mp4"
        self._dialog._refresh_dynamic_sections()

    def on_encoder_changed(self, index: int) -> None:
        dialog = self._dialog
        dialog._draft.encoder = dialog._encoder_combo.itemData(index) or "auto"
        dialog._refresh_dynamic_sections()

    def on_amplify_toggled(self, checked: bool) -> None:
        dialog = self._dialog
        dialog._draft.amplify_audio = checked
        dialog._amplify_db_spin.setEnabled(checked)
        dialog._refresh_dynamic_sections()

    def on_device_changed(self, index: int) -> None:
        dialog = self._dialog
        dialog._draft.device_name = dialog._device_combo.itemData(index) or ""
        dialog._refresh_dynamic_sections()
