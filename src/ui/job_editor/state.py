from datetime import date
from pathlib import Path

from PySide6.QtWidgets import QFileDialog, QHBoxLayout, QPushButton, QLineEdit

from ...workflow import WorkflowJob


class JobEditorStateMixin:
    def _refresh_nav(self) -> None:
        for index, lbl in enumerate(self._step_labels):
            if index < self._current:
                lbl.setStyleSheet(self._STEP_DONE)
            elif index == self._current:
                lbl.setStyleSheet(self._STEP_ACTIVE)
            else:
                lbl.setStyleSheet(self._STEP_TODO)

        self._back_btn.setVisible(self._current > 0)
        is_last = self._current == len(self._STEPS) - 1
        self._next_btn.setVisible(not is_last)
        self._finish_btn.setVisible(is_last)
        self._stack.setCurrentIndex(self._current)

    def _go_next(self) -> None:
        if not self._validate_page(self._current):
            return
        self._current += 1
        if self._current == 3:
            self._sync_upload_visibility()
        self._refresh_nav()

    def _go_back(self) -> None:
        if self._current > 0:
            self._current -= 1
            self._refresh_nav()

    def _finish(self) -> None:
        if not self._validate_page(self._current):
            return
        self._write_job()
        self.accept()

    def _open_workflow_preview(self) -> None:
        from . import JobWorkflowDialog, QMessageBox

        if self._mode_group.checkedId() == -1:
            QMessageBox.warning(self, "Keine Quelle", "Bitte zuerst eine Dateiquelle auswählen.")
            return
        self._write_job()
        dlg = JobWorkflowDialog(self, self._job, allow_edit=False, settings=self._settings)
        dlg.exec()

    def _validate_page(self, page: int) -> bool:
        from . import QMessageBox

        if page == 0:
            mode_id = self._mode_group.checkedId()
            if mode_id == -1:
                QMessageBox.warning(self, "Keine Quelle", "Bitte eine Dateiquelle auswählen.")
                return False
            if mode_id == 0 and self._file_list.is_empty():
                QMessageBox.warning(self, "Keine Dateien", "Bitte mindestens eine Datei hinzufügen.")
                return False
            if mode_id == 1 and not self._folder_src_edit.text().strip():
                QMessageBox.warning(self, "Kein Quellordner", "Bitte einen Quellordner angeben.")
                return False
            if mode_id == 2:
                if not self._device_combo.currentData():
                    QMessageBox.warning(self, "Kein Gerät", "Bitte ein Pi-Kamera-Gerät auswählen.")
                    return False
                workflow_name = self._name_edit.text().strip()
                device_name = self._device_combo.currentData() or ""
                default_pi_dest = self._settings.workflow_raw_dir_for(workflow_name, device_name)
                if not self._pi_dest_edit.text().strip() and not default_pi_dest:
                    QMessageBox.warning(self, "Kein Zielverzeichnis", "Bitte ein lokales Zielverzeichnis angeben.")
                    return False
        return True

    def _populate_from_job(self) -> None:
        from ...integrations.youtube_title_editor import load_memory

        job = self._job
        mem = load_memory()
        last_match = mem.get("last_match", {})
        today_iso = date.today().isoformat()
        self._name_edit.setText(job.name)

        mode_map = {"files": 0, "folder_scan": 1, "pi_download": 2}
        mode_id = mode_map.get(job.source_mode, 0)
        btn = self._mode_group.button(mode_id)
        if btn:
            btn.setChecked(True)
        self._source_stack.setCurrentIndex(mode_id)

        self._file_list.load(job.files)
        self._files_dst_edit.setText(job.copy_destination)
        self._files_move_cb.setChecked(job.move_files)

        self._folder_src_edit.setText(job.source_folder)
        self._file_pattern_edit.setText(job.file_pattern or "*.mp4")
        self._folder_dst_edit.setText(job.copy_destination)
        self._move_files_cb.setChecked(job.move_files)
        self._folder_prefix_edit.setText(job.output_prefix)

        dev_idx = self._device_combo.findData(job.device_name)
        if dev_idx >= 0:
            self._device_combo.setCurrentIndex(dev_idx)
        self._pi_dest_edit.setText(job.download_destination)
        self._delete_after_dl_cb.setChecked(job.delete_after_download)
        self._pi_prefix_edit.setText(job.output_prefix)
        if job.source_mode == "pi_download" and job.files:
            self._pi_file_list.load(job.files)
            self._pi_file_list.setVisible(True)
            self._pi_load_status.setText(f"✓ {len(job.files)} Aufnahme(n) gespeichert.")
            self._pi_load_status.setStyleSheet("color:green;")

        self._convert_enabled_cb.setChecked(job.convert_enabled)
        self._encoding_widget.setEnabled(job.convert_enabled)
        enc_idx = self._encoder_combo.findData(job.encoder)
        if enc_idx >= 0:
            self._encoder_combo.setCurrentIndex(enc_idx)
        self._preset_combo.setCurrentText(job.preset)
        self._crf_spin.setValue(job.crf)
        self._fps_spin.setValue(job.fps)
        self._resolution_combo.setCurrentIndex(max(self._resolution_combo.findData(job.output_resolution), 0))
        self._format_combo.setCurrentIndex(max(self._format_combo.findData(job.output_format), 0))
        self._no_bframes_cb.setChecked(job.no_bframes)
        self._overwrite_cb.setChecked(job.overwrite)

        self._merge_audio_cb.setChecked(job.merge_audio)
        self._amplify_audio_cb.setChecked(job.amplify_audio)
        self._amplify_db_spin.setValue(job.amplify_db)
        self._amplify_db_spin.setEnabled(job.amplify_audio)
        self._audio_sync_cb.setChecked(job.audio_sync)

        self._yt_upload_cb.setChecked(job.upload_youtube)
        self._yt_create_cb.setChecked(job.create_youtube_version)
        self._yt_title_edit.setText(job.default_youtube_title)
        self._yt_playlist_edit.setText(job.default_youtube_playlist)
        self._yt_competition = job.default_youtube_competition or str(last_match.get("competition", ""))
        self._yt_details.setVisible(job.upload_youtube)

        self._kb_upload_cb.setChecked(job.upload_kaderblick)
        self._kb_upload_cb.setEnabled(job.upload_youtube)
        self._kb_game_id_edit.setText(job.default_kaderblick_game_id)
        self._kb_details_widget.setVisible(job.upload_kaderblick)

        self._tc_enabled_cb.setChecked(job.title_card_enabled)
        self._tc_details.setEnabled(job.title_card_enabled)
        self._tc_logo_edit.setText(job.title_card_logo_path)
        self._tc_home_edit.setText(job.title_card_home_team or str(last_match.get("home_team", "")))
        self._tc_away_edit.setText(job.title_card_away_team or str(last_match.get("away_team", "")))
        date_value = job.title_card_date.strip() if job.title_card_date else ""
        if not date_value:
            try:
                year, month, day = today_iso.split("-")
                date_value = f"{day}.{month}.{year}"
            except Exception:
                date_value = today_iso
        self._tc_date_edit.setText(date_value)
        self._tc_duration_spin.setValue(job.title_card_duration)
        bg = job.title_card_bg_color or "#000000"
        fg = job.title_card_fg_color or "#FFFFFF"
        self._tc_bg_color = bg
        self._tc_fg_color = fg
        self._update_color_btn("bg", bg)
        self._update_color_btn("fg", fg)

    def _write_job(self) -> None:
        job = self._job
        mode_id = self._mode_group.checkedId()
        mode_map = {0: "files", 1: "folder_scan", 2: "pi_download"}
        job.source_mode = mode_map[mode_id]
        job.name = self._name_edit.text().strip()

        if mode_id == 0:
            job.files = self._file_list.collect()
            job.copy_destination = self._files_dst_edit.text().strip()
            job.move_files = self._files_move_cb.isChecked()
            job.source_folder = ""
            job.device_name = ""
            if not job.name:
                job.name = Path(job.files[0].source_path).stem if job.files else "Workflow"
        elif mode_id == 1:
            job.source_folder = self._folder_src_edit.text().strip()
            job.file_pattern = self._file_pattern_edit.text().strip() or "*.mp4"
            job.copy_destination = self._folder_dst_edit.text().strip()
            job.move_files = self._move_files_cb.isChecked()
            job.output_prefix = self._folder_prefix_edit.text().strip()
            job.files = []
            job.device_name = ""
            if not job.name:
                job.name = Path(job.source_folder).name or "Ordner"
        elif mode_id == 2:
            job.device_name = self._device_combo.currentData()
            job.download_destination = self._pi_dest_edit.text().strip()
            job.delete_after_download = self._delete_after_dl_cb.isChecked()
            job.output_prefix = self._pi_prefix_edit.text().strip()
            job.files = self._pi_file_list.collect() if not self._pi_file_list.is_empty() else []
            job.source_folder = ""
            if not job.name:
                job.name = job.device_name or "Pi-Kamera"

        job.convert_enabled = self._convert_enabled_cb.isChecked()
        job.encoder = self._encoder_combo.currentData()
        job.preset = self._preset_combo.currentText()
        job.crf = self._crf_spin.value()
        job.fps = self._fps_spin.value()
        job.output_resolution = str(self._resolution_combo.currentData() or "source")
        job.output_format = str(self._format_combo.currentData() or "mp4")
        job.no_bframes = self._no_bframes_cb.isChecked()
        job.overwrite = self._overwrite_cb.isChecked()

        job.merge_audio = self._merge_audio_cb.isChecked()
        job.amplify_audio = self._amplify_audio_cb.isChecked()
        job.amplify_db = self._amplify_db_spin.value()
        job.audio_sync = self._audio_sync_cb.isChecked()

        job.create_youtube_version = self._yt_create_cb.isChecked()
        job.upload_youtube = self._yt_upload_cb.isChecked()
        job.default_youtube_title = self._yt_title_edit.text().strip()
        job.default_youtube_playlist = self._yt_playlist_edit.text().strip()
        job.default_youtube_competition = self._yt_competition.strip()

        job.upload_kaderblick = self._kb_upload_cb.isChecked()
        job.default_kaderblick_game_id = self._kb_game_id_edit.text().strip()

        job.title_card_enabled = self._tc_enabled_cb.isChecked()
        job.title_card_logo_path = self._tc_logo_edit.text().strip()
        job.title_card_home_team = self._tc_home_edit.text().strip()
        job.title_card_away_team = self._tc_away_edit.text().strip()
        job.title_card_date = self._tc_date_edit.text().strip()
        job.title_card_duration = self._tc_duration_spin.value()
        job.title_card_bg_color = self._tc_bg_color
        job.title_card_fg_color = self._tc_fg_color

    @staticmethod
    def _hbox(*widgets) -> QHBoxLayout:
        lay = QHBoxLayout()
        for widget in widgets:
            lay.addWidget(widget)
        return lay

    @staticmethod
    def _browse_btn(callback) -> QPushButton:
        btn = QPushButton("…")
        btn.setFixedWidth(32)
        btn.clicked.connect(callback)
        return btn

    def _browse_dir(self, line_edit: QLineEdit, title: str) -> None:
        start = line_edit.text().strip() or self._settings.last_directory or str(Path.home())
        chosen = QFileDialog.getExistingDirectory(self, title, start)
        if chosen:
            line_edit.setText(chosen)
            self._save_last_dir(chosen)

    def _save_last_dir(self, directory: str) -> None:
        self._settings.last_directory = directory
        self._settings.save()