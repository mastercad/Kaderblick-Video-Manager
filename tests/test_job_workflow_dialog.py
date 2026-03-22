import sys
from types import SimpleNamespace
from unittest.mock import patch

from PySide6.QtWidgets import QApplication

from src.settings import AppSettings
from src.workflow import FileEntry, WorkflowJob
from src.job_workflow_dialog import JobWorkflowDialog, _planned_job_steps


_app = QApplication.instance() or QApplication(sys.argv)


def _make_job(**kwargs) -> WorkflowJob:
    job = WorkflowJob(
        name="Workflow Job",
        source_mode="files",
        files=[FileEntry(source_path="/tmp/a.mp4")],
        **kwargs,
    )
    return job


def _settings() -> AppSettings:
    return AppSettings()


class TestJobWorkflowDialog:
    def test_editor_applies_basic_step_flags(self):
        job = _make_job(convert_enabled=True, upload_youtube=False, upload_kaderblick=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._name_edit.setText("Neu")
        dlg._step_checkboxes["convert"].setChecked(False)
        dlg._step_checkboxes["youtube_upload"].setChecked(True)
        dlg._step_checkboxes["kaderblick"].setChecked(True)
        dlg._apply_and_accept()

        assert dlg.changed is True
        assert job.name == "Neu"
        assert job.convert_enabled is False
        assert job.upload_youtube is True
        assert job.upload_kaderblick is True

    def test_editor_disables_kaderblick_without_youtube(self):
        job = _make_job(upload_youtube=True, upload_kaderblick=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._step_checkboxes["youtube_upload"].setChecked(False)

        assert dlg._draft.upload_youtube is False
        assert dlg._draft.upload_kaderblick is False
        assert dlg._step_checkboxes["kaderblick"].isEnabled() is False

    def test_editor_disables_output_steps_when_no_output_stack_remains(self):
        job = _make_job(convert_enabled=True, title_card_enabled=True, create_youtube_version=True, upload_youtube=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._step_checkboxes["convert"].setChecked(False)

        assert dlg._draft.convert_enabled is False
        assert dlg._draft.title_card_enabled is False
        assert dlg._draft.create_youtube_version is False
        assert dlg._step_checkboxes["titlecard"].isEnabled() is False
        assert dlg._step_checkboxes["yt_version"].isEnabled() is False

    def test_editor_keeps_merge_step_from_source_groups(self):
        job = WorkflowJob(
            name="Merge Job",
            source_mode="files",
            files=[
                FileEntry(source_path="/tmp/a.mp4", merge_group_id="g1"),
                FileEntry(source_path="/tmp/b.mp4", merge_group_id="g1"),
            ],
            convert_enabled=True,
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        assert "merge" in _planned_job_steps(dlg._draft)
        assert "Merge ist aktiv" in dlg._merge_label.text()
        assert dlg._file_list_widget is not None

    def test_preview_mode_has_no_editor_controls(self):
        job = _make_job()
        dlg = JobWorkflowDialog(None, job, allow_edit=False, settings=_settings())

        assert not hasattr(dlg, "_name_edit")

    def test_editor_applies_step_options_and_files(self):
        job = _make_job(title_card_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._yt_title_edit.setText("Liga Spiel")
        dlg._yt_playlist_edit.setText("Playlist 1")
        dlg._yt_competition_edit.setText("Pokal")
        dlg._kb_game_id_edit.setText("77")
        dlg._kb_type_spin.setValue(3)
        dlg._kb_camera_spin.setValue(4)
        dlg._tc_home_edit.setText("Heim")
        dlg._tc_away_edit.setText("Gast")
        dlg._tc_date_edit.setText("2026-03-22")
        dlg._tc_logo_edit.setText("/tmp/logo.png")
        dlg._tc_bg_edit.setText("#112233")
        dlg._tc_fg_edit.setText("#FFFFFF")
        dlg._tc_duration_spin.setValue(4.5)
        dlg._encoder_combo.setCurrentIndex(max(dlg._encoder_combo.findData("libx264"), 0))
        dlg._preset_combo.setCurrentText("slow")
        dlg._crf_spin.setValue(21)
        dlg._fps_spin.setValue(30)
        dlg._format_combo.setCurrentText("avi")
        dlg._overwrite_cb.setChecked(True)
        dlg._merge_audio_cb.setChecked(True)
        dlg._amplify_audio_cb.setChecked(True)
        dlg._amplify_db_spin.setValue(8.0)
        dlg._audio_sync_cb.setChecked(True)

        with patch("src.file_list_widget.QMessageBox.question", return_value=0x00000400), patch("src.file_list_widget.QMessageBox.information"):
            dlg._file_list_widget._table.selectRow(0)
            dlg._file_list_widget._open_add_files_dialog = lambda: None

        dlg._apply_and_accept()

        assert job.default_youtube_title == "Liga Spiel"
        assert job.default_youtube_playlist == "Playlist 1"
        assert job.default_youtube_competition == "Pokal"
        assert job.default_kaderblick_game_id == "77"
        assert job.default_kaderblick_video_type_id == 3
        assert job.default_kaderblick_camera_id == 4
        assert job.title_card_home_team == "Heim"
        assert job.title_card_away_team == "Gast"
        assert job.title_card_date == "2026-03-22"
        assert job.title_card_logo_path == "/tmp/logo.png"
        assert job.title_card_bg_color == "#112233"
        assert job.title_card_fg_color == "#FFFFFF"
        assert job.title_card_duration == 4.5
        assert job.encoder == "libx264"
        assert job.preset == "slow"
        assert job.crf == 21
        assert job.fps == 30
        assert job.output_format == "avi"
        assert job.overwrite is True
        assert job.merge_audio is True
        assert job.amplify_audio is True
        assert job.amplify_db == 8.0
        assert job.audio_sync is True

    def test_editor_tracks_merge_changes_from_file_list(self):
        job = WorkflowJob(
            name="Merge Job",
            source_mode="files",
            files=[
                FileEntry(source_path="/tmp/a.mp4"),
                FileEntry(source_path="/tmp/b.mp4"),
            ],
            convert_enabled=True,
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._file_list_widget._table.selectRow(0)
        dlg._file_list_widget._table.selectRow(1)
        with patch("src.file_list_widget.QMessageBox.question", return_value=0x00000400), patch("src.file_list_widget.QMessageBox.information"):
            dlg._file_list_widget._merge_selected()

        assert "merge" in _planned_job_steps(dlg._draft)
        assert "Merge ist aktiv" in dlg._merge_label.text()

    def test_editor_edits_folder_scan_source_fields(self):
        job = WorkflowJob(
            name="Ordner Job",
            source_mode="folder_scan",
            source_folder="/input",
            file_pattern="*.mov",
            copy_destination="/output",
            move_files=False,
            output_prefix="A_",
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        assert dlg._source_mode_widgets["folder_scan"].isHidden() is False
        dlg._folder_src_edit.setText("/spiele")
        dlg._file_pattern_edit.setText("*.mp4")
        dlg._folder_dst_edit.setText("/fertig")
        dlg._move_files_cb.setChecked(True)
        dlg._folder_prefix_edit.setText("B_")
        dlg._apply_and_accept()

        assert job.source_folder == "/spiele"
        assert job.file_pattern == "*.mp4"
        assert job.copy_destination == "/fertig"
        assert job.move_files is True
        assert job.output_prefix == "B_"

    def test_editor_edits_pi_source_fields(self):
        settings = _settings()
        if settings.cameras.devices:
            device_name = settings.cameras.devices[0].name
        else:
            device_name = ""
        job = WorkflowJob(
            name="Pi Job",
            source_mode="pi_download",
            device_name=device_name,
            download_destination="/downloads",
            delete_after_download=False,
            output_prefix="cam_",
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        assert dlg._source_mode_widgets["pi_download"].isHidden() is False
        if dlg._device_combo.count() > 1:
            dlg._device_combo.setCurrentIndex(1)
            expected_device = dlg._device_combo.currentData()
        else:
            expected_device = ""
        dlg._pi_dest_edit.setText("/neu")
        dlg._delete_after_dl_cb.setChecked(True)
        dlg._pi_prefix_edit.setText("kb_")
        dlg._apply_and_accept()

        assert job.device_name == expected_device
        assert job.download_destination == "/neu"
        assert job.delete_after_download is True
        assert job.output_prefix == "kb_"

    def test_editor_disables_upload_detail_fields_when_upload_off(self):
        job = _make_job(upload_youtube=True, upload_kaderblick=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._step_checkboxes["youtube_upload"].setChecked(False)

        assert dlg._yt_title_edit.isEnabled() is False
        assert dlg._yt_playlist_edit.isEnabled() is False
        assert dlg._yt_competition_edit.isEnabled() is False
        assert dlg._kb_game_id_edit.isEnabled() is False
        assert dlg._kb_type_spin.isEnabled() is False
        assert dlg._kb_camera_spin.isEnabled() is False

    def test_editor_disables_titlecard_detail_fields_when_titlecard_off(self):
        job = _make_job(title_card_enabled=True, convert_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._step_checkboxes["titlecard"].setChecked(False)

        assert dlg._tc_home_edit.isEnabled() is False
        assert dlg._tc_logo_edit.isEnabled() is False
        assert dlg._tc_bg_edit.isEnabled() is False
        assert dlg._tc_fg_edit.isEnabled() is False

    def test_playlist_helper_updates_playlist_and_match_fields(self):
        class _DummyMatchData:
            competition = "Kreispokal"
            home_team = "FC Heim"
            away_team = "FC Gast"
            date_iso = "2026-03-22"

        class _DummyPlaylistDialog:
            def __init__(self, *args, **kwargs):
                self.playlist_title = "Kreispokal | FC Heim - FC Gast"
                self.match_data = _DummyMatchData()

            def exec(self):
                return True

        job = _make_job(upload_youtube=True, title_card_enabled=True, convert_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        with patch("src.job_workflow_dialog.YouTubeTitleEditorDialog", _DummyPlaylistDialog), patch("src.job_workflow_dialog.MatchData", SimpleNamespace):
            dlg._open_match_editor_for_playlist()

        assert dlg._yt_playlist_edit.text() == "Kreispokal | FC Heim - FC Gast"
        assert dlg._yt_competition_edit.text() == "Kreispokal"
        assert dlg._tc_home_edit.text() == "FC Heim"
        assert dlg._tc_away_edit.text() == "FC Gast"
        assert dlg._tc_date_edit.text() == "2026-03-22"

    def test_kaderblick_loader_updates_status_and_file_widgets(self):
        settings = _settings()
        settings.kaderblick.jwt_token = "token"
        job = _make_job(upload_youtube=True, upload_kaderblick=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        with patch("src.job_workflow_dialog.fetch_video_types", return_value=[{"id": 1}]), patch("src.job_workflow_dialog.fetch_cameras", return_value=[{"id": 2}]):
            dlg._kb_load_api_data(force=True)

        assert "1 Typen, 1 Kameras geladen" in dlg._kb_status_label.text()

    def test_pi_loader_populates_selectable_file_list(self):
        settings = _settings()
        settings.cameras.devices = [SimpleNamespace(name="Pi 1", ip="10.0.0.5")]
        settings.cameras.destination = "/dest"
        job = WorkflowJob(source_mode="pi_download", device_name="Pi 1", download_destination="/dest")
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        dlg._device_combo.setCurrentIndex(1)
        dlg._on_camera_files_loaded([{"base": "halbzeit1"}, {"base": "halbzeit2"}])

        assert dlg._pi_file_list.isHidden() is False
        assert len(dlg._draft.files) == 2
        assert dlg._draft.files[0].source_path.endswith("/dest/Pi 1/halbzeit1.mjpg")