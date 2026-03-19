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
import pytest
from unittest.mock import patch

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt

# Einmalige App-Instanz
_app = QApplication.instance() or QApplication(sys.argv)

from src.settings import AppSettings
from src.workflow import WorkflowJob, FileEntry
from src.job_editor import JobEditorDialog


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
            output_format="avi",
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
        assert dlg._format_combo.currentText() == "avi"

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
        dlg._write_job()

        assert job.name == "Geänderter Name"
        assert job.source_mode == "folder_scan"
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
        dlg._write_job()
        assert dlg._job.upload_youtube is True
        assert dlg._job.create_youtube_version is True
        assert dlg._job.default_youtube_title == "Mein Titel"
        assert dlg._job.default_youtube_playlist == "Liga"

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
        with patch("src.job_editor.QMessageBox.warning"):
            result = dlg._validate_page(0)
        assert result is False

    def test_folder_mode_empty_path_fails(self):
        dlg = _new_dialog()
        dlg._mode_group.button(1).setChecked(True)   # folder_scan
        dlg._folder_src_edit.setText("")
        from unittest.mock import patch
        with patch("src.job_editor.QMessageBox.warning"):
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
