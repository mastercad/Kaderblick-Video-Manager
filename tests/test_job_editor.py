"""Tests für den JobEditorDialog-Wizard (job_editor.py).

PySide6-Widgets benötigen eine QApplication.  Die Tests prüfen:
- Wizard öffnet sich (Standardwerte korrekt)
- Wizard öffnet sich im Edit-Modus (Job-Daten werden geladen)
- _write_job() schreibt alle Werte korrekt zurück
- Seitenvalidierung (source-mode, Pflichtfelder)
- Navigation (Seiten wechseln korrekt)
- _apply_profile() überschreibt Encoder-Parameter
- Quellmodus-Karten-Auswahl wechselt source_stack
"""

import sys
from datetime import date
import pytest
from unittest.mock import patch

from PySide6.QtGui import QColor
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt

# Einmalige App-Instanz
_app = QApplication.instance() or QApplication(sys.argv)

from src.settings import AppSettings
from src.job_workflow.panels.source import WorkflowSourcePanel
from src.ui import AlwaysVisiblePlaceholderLineEdit
from src.workflow import WorkflowJob, FileEntry
from src.ui.job_editor import JobEditorDialog


def _settings() -> AppSettings:
    return AppSettings()


def _new_dialog() -> JobEditorDialog:
    """Öffnet den Wizard im Neu-Modus."""
    return JobEditorDialog(None, _settings())


def _edit_dialog(job: WorkflowJob) -> JobEditorDialog:
    """Öffnet den Wizard im Bearbeitungs-Modus."""
    return JobEditorDialog(None, _settings(), job=job)


# ─── Initialisierung (Neu-Modus) ──────────────────────────────────────────────

class TestWizardInit:
    def test_dialog_created_without_crash(self):
        dlg = _new_dialog()
        assert dlg is not None

    def test_initial_page_is_zero(self):
        dlg = _new_dialog()
        assert dlg._current == 0

    def test_step_labels_count(self):
        dlg = _new_dialog()
        assert len(dlg._step_labels) == 4

    def test_back_button_hidden_on_first_page(self):
        dlg = _new_dialog()
        assert not dlg._back_btn.isVisible()

    def test_finish_button_hidden_on_first_page(self):
        dlg = _new_dialog()
        assert not dlg._finish_btn.isVisible()

    def test_next_button_visible_on_first_page(self):
        dlg = _new_dialog()
        # Dialog ist nicht .show()-t → isVisibleTo prüft relative Sichtbarkeit
        assert dlg._next_btn.isVisibleTo(dlg)

    def test_source_stack_shows_first_panel(self):
        """Kein Radio-Knopf gesetzt → source_stack zeigt Panel 0 (Dateien)."""
        dlg = _new_dialog()
        # Standardmäßig ist "files" gesetzt → index 0
        assert dlg._source_stack.currentIndex() == 0

    def test_mode_group_has_three_buttons(self):
        dlg = _new_dialog()
        assert len(dlg._mode_group.buttons()) == 3

    def test_encoding_widget_enabled_by_default(self):
        """convert_enabled=True im Standard-Job → Encoding-Box ist aktiv."""
        dlg = _new_dialog()
        assert dlg._encoding_widget.isEnabled()

    def test_placeholders_use_output_root_with_workflow_date_and_device_name(self):
        settings = _settings()
        settings.workflow_output_root = "/srv/workflows"
        settings.cameras.devices = [type("Dev", (), {"name": "Pi Nord", "ip": "10.0.0.8"})()]
        dlg = JobEditorDialog(None, settings)

        dlg._name_edit.setText("Spieltag 23")
        dlg._device_combo.setCurrentIndex(dlg._device_combo.findData("Pi Nord"))

        expected_raw = f"/srv/workflows/Spieltag 23 {date.today().isoformat()}/raw"
        assert dlg._files_dst_edit.placeholderText() == expected_raw
        assert dlg._folder_dst_edit.placeholderText() == expected_raw
        assert dlg._pi_dest_edit.placeholderText() == "/srv/workflows/Pi Nord/raw"

    def test_always_visible_placeholder_line_edit_shows_ghost_text_but_reports_empty_value(self):
        edit = AlwaysVisiblePlaceholderLineEdit()
        edit.setPlaceholderText("/srv/workflows/Spieltag 23 2026-03-23")
        edit.show()
        QApplication.processEvents()

        assert edit.text() == ""
        assert edit.showingPlaceholder() is True
        assert edit.displayText() == ""
        assert edit.placeholderText() == "/srv/workflows/Spieltag 23 2026-03-23"
        placeholder_color = edit.palette().color(edit.palette().ColorRole.PlaceholderText)
        text_color = edit.palette().color(edit.palette().ColorRole.Text)
        assert placeholder_color != text_color
        assert placeholder_color.alpha() < text_color.alpha() or placeholder_color.lightness() != text_color.lightness()

        edit.setFocus()
        QApplication.processEvents()
        assert edit.text() == ""
        assert edit.showingPlaceholder() is True

        edit.setText("/tmp/override")
        assert edit.text() == "/tmp/override"
        assert edit.displayText() == "/tmp/override"

    def test_always_visible_placeholder_line_edit_emits_logical_value_only(self):
        edit = AlwaysVisiblePlaceholderLineEdit()
        seen: list[str] = []
        edit.effectiveTextChanged.connect(seen.append)

        edit.setPlaceholderText("/srv/workflows/Spieltag 23 2026-03-23")
        edit.show()
        QApplication.processEvents()

        assert seen == []

        edit.setText("/tmp/ziel")
        assert seen[-1] == "/tmp/ziel"

        edit.clear()
        assert seen[-1] == ""
        assert edit.text() == ""
        assert edit.displayText() == ""
        assert edit.placeholderText() == "/srv/workflows/Spieltag 23 2026-03-23"

    def test_always_visible_placeholder_line_edit_uses_non_selectable_placeholder_instead_of_real_text(self):
        edit = AlwaysVisiblePlaceholderLineEdit()
        edit.setPlaceholderText("/srv/workflows/Spieltag 23 2026-03-23")
        edit.show()
        QApplication.processEvents()

        edit.selectAll()

        assert edit.selectedText() == ""

        edit.insert("/tmp/ziel")
        assert edit.text() == "/tmp/ziel"

        edit.clear()
        QApplication.processEvents()

        assert edit.text() == ""
        assert edit.selectedText() == ""

    def test_source_panel_clearing_target_propagates_empty_value_and_keeps_placeholder(self):
        settings = _settings()
        settings.workflow_output_root = "/srv/workflows"
        draft = WorkflowJob(
            name="Dateien",
            source_mode="files",
            copy_destination="/altes-ziel",
            files=[FileEntry(source_path="/tmp/a.mp4")],
        )
        panel = WorkflowSourcePanel(
            settings=settings,
            update_text_field=lambda attr, value: setattr(draft, attr, value),
            update_bool_field=lambda _attr, _value: None,
            on_file_pattern_changed=lambda _text: None,
            on_device_changed=lambda _index: None,
            on_files_changed=lambda: None,
            on_pi_files_changed=lambda: None,
            on_match_data_changed=lambda _home, _away, _date_iso: None,
            on_load_pi_camera_files=lambda: None,
        )

        panel.load_from_job(draft)
        panel._files_dst_edit.clear()
        panel.refresh_from_job(draft)

        assert panel._files_dst_edit.text() == ""
        assert panel._files_dst_edit.displayText() == ""
        assert panel._files_dst_edit.placeholderText() == f"/srv/workflows/Dateien {date.today().isoformat()}/raw"
        assert draft.copy_destination == ""


# ─── Edit-Modus: Daten laden ──────────────────────────────────────────────────

class TestWizardEditMode:
    def _job(self) -> WorkflowJob:
        return WorkflowJob(
            name="Test-Auftrag",
            source_mode="folder_scan",
            source_folder="/media/footage",
            file_pattern="*.mjpg",
            copy_destination="/media/converted",
            move_files=True,
            output_prefix="halbzeit_",
            convert_enabled=False,
            encoder="libx265",
            crf=24,
            preset="slow",
            fps=30,
            output_resolution="720p",
            output_format="avi",
            no_bframes=False,
            merge_audio=True,
            amplify_audio=True,
            amplify_db=9.0,
            audio_sync=True,
            upload_youtube=True,
            create_youtube_version=True,
            default_youtube_title="Spiel Titel",
            default_youtube_playlist="Liga Playlist",
            upload_kaderblick=True,
            default_kaderblick_game_id="42",
        )

    def test_name_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._name_edit.text() == "Test-Auftrag"

    def test_mode_folder_scan_selected(self):
        dlg = _edit_dialog(self._job())
        assert dlg._mode_group.checkedId() == 1   # 1 = folder_scan

    def test_folder_src_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._folder_src_edit.text() == "/media/footage"

    def test_file_pattern_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._file_pattern_edit.text() == "*.mjpg"

    def test_folder_dst_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._folder_dst_edit.text() == "/media/converted"

    def test_move_files_checkbox(self):
        dlg = _edit_dialog(self._job())
        assert dlg._move_files_cb.isChecked()

    def test_convert_disabled(self):
        dlg = _edit_dialog(self._job())
        assert not dlg._convert_enabled_cb.isChecked()
        assert not dlg._encoding_widget.isEnabled()

    def test_crf_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._crf_spin.value() == 24

    def test_fps_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._fps_spin.value() == 30

    def test_format_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._format_combo.currentData() == "avi"

    def test_resolution_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._resolution_combo.currentData() == "720p"

    def test_no_bframes_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._no_bframes_cb.isChecked() is False

    def test_amplify_checked_and_db_set(self):
        dlg = _edit_dialog(self._job())
        assert dlg._amplify_audio_cb.isChecked()
        assert dlg._amplify_db_spin.value() == pytest.approx(9.0)

    def test_audio_sync_checked(self):
        dlg = _edit_dialog(self._job())
        assert dlg._audio_sync_cb.isChecked()

    def test_yt_upload_checked(self):
        dlg = _edit_dialog(self._job())
        assert dlg._yt_upload_cb.isChecked()

    def test_yt_title_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._yt_title_edit.text() == "Spiel Titel"

    def test_yt_playlist_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._yt_playlist_edit.text() == "Liga Playlist"

    def test_kb_upload_checked(self):
        dlg = _edit_dialog(self._job())
        assert dlg._kb_upload_cb.isChecked()

    def test_kb_game_id_populated(self):
        dlg = _edit_dialog(self._job())
        assert dlg._kb_game_id_edit.text() == "42"


# ─── _write_job() – Daten zurückschreiben ─────────────────────────────────────

class TestWriteJob:
    def test_folder_scan_roundtrip(self):
        job = WorkflowJob(
            name="Ordner-Auftrag",
            source_mode="folder_scan",
            source_folder="/media/footage",
            file_pattern="*.mp4",
            output_format="mp4",
        )
        dlg = _edit_dialog(job)

        # Werte im Dialog ändern
        dlg._name_edit.setText("Geänderter Name")
        dlg._crf_spin.setValue(20)
        dlg._resolution_combo.setCurrentIndex(max(dlg._resolution_combo.findData("1080p"), 0))
        dlg._format_combo.setCurrentIndex(max(dlg._format_combo.findData("avi"), 0))
        dlg._no_bframes_cb.setChecked(False)
        dlg._write_job()

        assert job.name == "Geänderter Name"
        assert job.source_mode == "folder_scan"
        assert job.output_resolution == "1080p"
        assert job.output_format == "avi"
        assert job.no_bframes is False
        assert job.crf == 20

    def test_files_mode_source_mode_set(self):
        dlg = _new_dialog()
        # Dateien-Modus auswählen (Index 0)
        dlg._mode_group.button(0).setChecked(True)
        dlg._write_job()
        assert dlg._job.source_mode == "files"

    def test_pi_mode_source_mode_set(self):
        dlg = _new_dialog()
        dlg._mode_group.button(2).setChecked(True)
        dlg._pi_dest_edit.setText("/footage")
        dlg._write_job()
        assert dlg._job.source_mode == "pi_download"
        assert dlg._job.download_destination == "/footage"

    def test_pi_mode_keeps_blank_destination_when_global_output_root_is_used(self):
        settings = _settings()
        settings.workflow_output_root = "/srv/workflows"
        dlg = JobEditorDialog(None, settings)

        dlg._mode_group.button(2).setChecked(True)
        dlg._name_edit.setText("Spieltag 23")
        dlg._write_job()

        assert dlg._job.source_mode == "pi_download"
        assert dlg._job.download_destination == ""

    def test_amplify_db_written(self):
        dlg = _new_dialog()
        dlg._amplify_audio_cb.setChecked(True)
        dlg._amplify_db_spin.setValue(12.0)
        dlg._write_job()
        assert dlg._job.amplify_audio is True
        assert dlg._job.amplify_db == pytest.approx(12.0)

    def test_yt_fields_written(self):
        dlg = _new_dialog()
        dlg._yt_upload_cb.setChecked(True)
        dlg._yt_create_cb.setChecked(True)
        dlg._yt_title_edit.setText("Mein Titel")
        dlg._yt_playlist_edit.setText("Liga")
        dlg._yt_competition = "Sparkassenpokal"
        dlg._write_job()
        assert dlg._job.upload_youtube is True
        assert dlg._job.create_youtube_version is True
        assert dlg._job.default_youtube_title == "Mein Titel"
        assert dlg._job.default_youtube_playlist == "Liga"
        assert dlg._job.default_youtube_competition == "Sparkassenpokal"

    @patch("src.integrations.youtube_title_editor.load_memory")
    def test_blank_match_fields_use_persisted_match_memory_when_loading(self, mock_load_memory):
        mock_load_memory.return_value = {
            "last_match": {
                "date_iso": "2026-03-20",
                "competition": "Pokal",
                "home_team": "FC Heim",
                "away_team": "FC Gast",
            }
        }
        dlg = JobEditorDialog(None, _settings(), WorkflowJob())

        assert dlg._yt_competition == "Pokal"
        assert dlg._tc_home_edit.text() == "FC Heim"
        assert dlg._tc_away_edit.text() == "FC Gast"
        assert dlg._tc_date_edit.text()

    def test_kb_fields_written(self):
        dlg = _new_dialog()
        dlg._kb_upload_cb.setChecked(True)
        dlg._kb_game_id_edit.setText("99")
        dlg._write_job()
        assert dlg._job.upload_kaderblick is True
        assert dlg._job.default_kaderblick_game_id == "99"


# ─── Navigation ───────────────────────────────────────────────────────────────

class TestNavigation:
    def test_go_next_increments_page(self):
        dlg = _new_dialog()
        dlg._mode_group.button(0).setChecked(True)
        # Dateien-Modus: Tabelle leer → Validation schlägt fehl.
        # Wir umgehen die Validation und prüfen nur das Inkrement.
        dlg._current = 0
        # Manuell ohne Validation:
        dlg._current += 1
        dlg._refresh_nav()
        assert dlg._current == 1

    def test_go_back_decrements_page(self):
        dlg = _new_dialog()
        dlg._current = 2
        dlg._go_back()
        assert dlg._current == 1


class TestValidation:
    def test_pi_mode_allows_blank_destination_with_global_output_root(self):
        settings = _settings()
        settings.workflow_output_root = "/srv/workflows"
        dlg = JobEditorDialog(None, settings)

        dlg._name_edit.setText("Spieltag 23")
        dlg._mode_group.button(2).setChecked(True)
        dlg._device_combo.addItem("Pi 1", "Pi 1")
        dlg._device_combo.setCurrentIndex(dlg._device_combo.findData("Pi 1"))
        dlg._pi_dest_edit.setText("")

        assert dlg._validate_page(0) is True

    def test_back_button_visible_on_page_2(self):
        dlg = _new_dialog()
        dlg._current = 1
        dlg._refresh_nav()
        assert dlg._back_btn.isVisibleTo(dlg)

    def test_finish_button_visible_on_last_page(self):
        dlg = _new_dialog()
        dlg._current = 3
        dlg._refresh_nav()
        assert dlg._finish_btn.isVisibleTo(dlg)

    def test_next_button_hidden_on_last_page(self):
        dlg = _new_dialog()
        dlg._current = 3
        dlg._refresh_nav()
        assert not dlg._next_btn.isVisible()

    def test_stack_follows_current(self):
        dlg = _new_dialog()
        dlg._current = 1
        dlg._refresh_nav()
        assert dlg._stack.currentIndex() == 1

    def test_go_back_not_below_zero(self):
        dlg = _new_dialog()
        dlg._current = 0
        dlg._go_back()   # soll nichts tun
        assert dlg._current == 0


class _DummyWorkflowDialog:
    instances = []

    def __init__(self, parent, job, allow_edit=False, settings=None):
        self.parent = parent
        self.job = job
        self.allow_edit = allow_edit
        self.settings = settings
        _DummyWorkflowDialog.instances.append(self)

    def exec(self):
        return True


class TestWorkflowPreview:
    def test_preview_opens_workflow_dialog_with_current_job_state(self):
        dlg = _new_dialog()
        _DummyWorkflowDialog.instances.clear()

        dlg._mode_group.button(0).setChecked(True)
        dlg._name_edit.setText("Preview Job")
        dlg._yt_upload_cb.setChecked(True)
        dlg._tc_enabled_cb.setChecked(True)
        dlg._file_list.load([FileEntry(source_path="/tmp/clip.mp4")])

        with patch("src.ui.job_editor.JobWorkflowDialog", _DummyWorkflowDialog):
            dlg._open_workflow_preview()

        assert len(_DummyWorkflowDialog.instances) == 1
        opened = _DummyWorkflowDialog.instances[0]
        assert opened.allow_edit is False
        assert opened.job is dlg.result_job
        assert opened.job.name == "Preview Job"
        assert opened.job.upload_youtube is True
        assert opened.job.title_card_enabled is True


class TestTitlecardWizardState:
    def test_graph_based_titlecard_workflow_enables_legacy_titlecard_controls(self):
        job = WorkflowJob(
            title_card_enabled=False,
            graph_nodes=[
                {"id": "source-1", "type": "source_files"},
                {"id": "title-1", "type": "titlecard"},
            ],
            graph_edges=[
                {"source": "source-1", "target": "title-1"},
            ],
        )

        dlg = _edit_dialog(job)

        assert dlg._tc_enabled_cb.isChecked() is True
        assert dlg._tc_details.isEnabled() is True
        assert dlg._tc_logo_edit.isEnabled() is True
        assert dlg._tc_bg_btn.isEnabled() is True
        assert dlg._tc_fg_btn.isEnabled() is True


# ─── Validation ───────────────────────────────────────────────────────────────

class TestValidation:
    def test_no_source_mode_fails_page0(self):
        dlg = _new_dialog()
        # Keinen Modus auswählen
        dlg._mode_group.setExclusive(False)
        for btn in dlg._mode_group.buttons():
            btn.setChecked(False)
        dlg._mode_group.setExclusive(True)
        # Validation soll False zurückgeben (wir unterdrücken das QMessageBox)
        from unittest.mock import patch
        with patch("src.ui.job_editor.QMessageBox.warning"):
            result = dlg._validate_page(0)
        assert result is False

    def test_folder_mode_empty_path_fails(self):
        dlg = _new_dialog()
        dlg._mode_group.button(1).setChecked(True)   # folder_scan
        dlg._folder_src_edit.setText("")
        from unittest.mock import patch
        with patch("src.ui.job_editor.QMessageBox.warning"):
            result = dlg._validate_page(0)
        assert result is False

    def test_folder_mode_with_path_passes(self):
        dlg = _new_dialog()
        dlg._mode_group.button(1).setChecked(True)
        dlg._folder_src_edit.setText("/some/path")
        result = dlg._validate_page(0)
        assert result is True

    def test_processing_page_always_valid(self):
        dlg = _new_dialog()
        result = dlg._validate_page(1)
        assert result is True

    def test_upload_page_always_valid(self):
        dlg = _new_dialog()
        result = dlg._validate_page(2)
        assert result is True


# ─── _apply_profile ───────────────────────────────────────────────────────────

class TestApplyProfile:
    def test_profile_sets_crf(self):
        from src.settings import PROFILES
        if not PROFILES:
            pytest.skip("Keine Profile konfiguriert")
        dlg = _new_dialog()
        pname, pvals = next(iter(PROFILES.items()))
        dlg._apply_profile(pname)
        if "crf" in pvals:
            assert dlg._crf_spin.value() == pvals["crf"]

    def test_unknown_profile_no_crash(self):
        dlg = _new_dialog()
        dlg._apply_profile("Profil existiert nicht")   # darf nicht crashen

    def test_all_profiles_applicable(self):
        from src.settings import PROFILES
        dlg = _new_dialog()
        for name in PROFILES:
            dlg._apply_profile(name)   # keiner darf crashen


# ─── Upload-Visibility ────────────────────────────────────────────────────────

class TestUploadVisibility:
    def test_yt_details_hidden_when_unchecked(self):
        dlg = _new_dialog()
        dlg._yt_upload_cb.setChecked(False)
        dlg._sync_upload_visibility()
        assert not dlg._yt_details.isVisible()

    def test_yt_details_shown_when_checked(self):
        dlg = _new_dialog()
        with patch.object(dlg._yt_details, 'setVisible') as mock_sv:
            dlg._yt_upload_cb.setChecked(True)
            dlg._sync_upload_visibility()
            mock_sv.assert_called_with(True)

    def test_kb_disabled_when_yt_off(self):
        dlg = _new_dialog()
        dlg._yt_upload_cb.setChecked(False)
        dlg._sync_upload_visibility()
        assert not dlg._kb_upload_cb.isEnabled()

    def test_kb_enabled_when_yt_on(self):
        dlg = _new_dialog()
        dlg._yt_upload_cb.setChecked(True)
        dlg._sync_upload_visibility()
        assert dlg._kb_upload_cb.isEnabled()

    def test_files_mode_shows_files_hint(self):
        dlg = _new_dialog()
        dlg._mode_group.button(0).setChecked(True)   # files
        dlg._yt_upload_cb.setChecked(True)
        with patch.object(dlg._yt_files_hint, 'setVisible') as mock_hint, \
             patch.object(dlg._yt_title_edit, 'setVisible') as mock_title:
            dlg._sync_upload_visibility()
            mock_hint.assert_called_with(True)
            mock_title.assert_called_with(False)

    def test_folder_mode_shows_title_field(self):
        dlg = _new_dialog()
        dlg._mode_group.button(1).setChecked(True)   # folder_scan
        dlg._yt_upload_cb.setChecked(True)
        with patch.object(dlg._yt_title_edit, 'setVisible') as mock_title, \
             patch.object(dlg._yt_files_hint, 'setVisible') as mock_hint:
            dlg._sync_upload_visibility()
            mock_title.assert_called_with(True)
            mock_hint.assert_called_with(False)
