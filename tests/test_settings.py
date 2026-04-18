import json
from datetime import date

from PySide6.QtCore import QDate
from PySide6.QtWidgets import QApplication

from src import settings as settings_module
from src.settings import DeviceSettings
from src.workflow import WorkflowJob
from src.workflow_steps.executor_support import ExecutorSupport
from src.ui.dialogs import CameraSettingsDialog, GeneralSettingsDialog, KaderblickSettingsDialog


_app = QApplication.instance() or QApplication([])


class TestSettingsPaths:
    def test_settings_file_is_stored_in_config_directory(self):
        assert settings_module.SETTINGS_FILE.name == "settings.json"
        assert settings_module.SETTINGS_FILE.parent.name == "config"


class TestAppSettingsLoad:
    def test_default_restore_last_workflow_is_enabled(self):
        loaded = settings_module.AppSettings()

        assert loaded.restore_last_workflow is True

    def test_load_restores_kaderblick_tokens(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        config_file.parent.mkdir(parents=True, exist_ok=True)
        config_file.write_text(
            json.dumps(
                {
                    "kaderblick": {
                        "auth_mode": "jwt",
                        "jwt_token": "jwt-value",
                        "jwt_refresh_token": "refresh-value",
                        "bearer_token": "",
                    }
                }
            ),
            encoding="utf-8",
        )

        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        loaded = settings_module.AppSettings.load()

        assert loaded.kaderblick.auth_mode == "jwt"
        assert loaded.kaderblick.jwt_token == "jwt-value"
        assert loaded.kaderblick.jwt_refresh_token == "refresh-value"

    def test_load_defaults_blank_kaderblick_auth_mode_to_jwt(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        config_file.parent.mkdir(parents=True, exist_ok=True)
        config_file.write_text(
            json.dumps(
                {
                    "kaderblick": {
                        "auth_mode": "",
                        "jwt_token": "",
                        "jwt_refresh_token": "",
                        "bearer_token": "",
                    }
                }
            ),
            encoding="utf-8",
        )

        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        loaded = settings_module.AppSettings.load()

        assert loaded.kaderblick.auth_mode == "jwt"

    def test_load_preserves_explicit_kaderblick_auth_mode_even_with_other_tokens_present(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        config_file.parent.mkdir(parents=True, exist_ok=True)
        config_file.write_text(
            json.dumps(
                {
                    "kaderblick": {
                        "auth_mode": "jwt",
                        "jwt_token": "",
                        "jwt_refresh_token": "",
                        "bearer_token": "bearer-value",
                    }
                }
            ),
            encoding="utf-8",
        )

        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        loaded = settings_module.AppSettings.load()

        assert loaded.kaderblick.auth_mode == "jwt"

    def test_load_ignores_data_settings_file_when_config_is_missing(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        data_settings_file = tmp_path / "data" / "settings.json"
        data_settings_file.parent.mkdir(parents=True, exist_ok=True)
        data_settings_file.write_text(
            json.dumps(
                {
                    "video": {"fps": 30},
                    "kaderblick": {
                        "auth_mode": "jwt",
                        "jwt_token": "legacy-jwt",
                        "jwt_refresh_token": "legacy-refresh",
                        "bearer_token": "",
                    }
                }
            ),
            encoding="utf-8",
        )

        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        loaded = settings_module.AppSettings.load()

        assert loaded.video.fps == settings_module.VideoSettings().fps
        assert loaded.kaderblick.jwt_token == ""
        assert loaded.kaderblick.jwt_refresh_token == ""

    def test_load_restores_last_directory_from_config(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        config_file.parent.mkdir(parents=True, exist_ok=True)
        config_file.write_text(
            json.dumps(
                {
                    "last_directory": "/media/video/spieltag-23",
                    "restore_last_workflow": True,
                }
            ),
            encoding="utf-8",
        )

        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        loaded = settings_module.AppSettings.load()

        assert loaded.last_directory == "/media/video/spieltag-23"

    def test_save_preserves_existing_kaderblick_tokens_for_unrelated_changes(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        config_file.parent.mkdir(parents=True, exist_ok=True)
        config_file.write_text(
            json.dumps(
                {
                    "kaderblick": {
                        "auth_mode": "jwt",
                        "jwt_token": "saved-jwt",
                        "jwt_refresh_token": "saved-refresh",
                        "bearer_token": "",
                    },
                    "last_directory": "/alt"
                }
            ),
            encoding="utf-8",
        )

        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        app_settings = settings_module.AppSettings()
        app_settings.last_directory = "/neu"
        app_settings.save()

        payload = json.loads(config_file.read_text(encoding="utf-8"))
        assert payload["last_directory"] == "/neu"
        assert payload["kaderblick"]["jwt_token"] == "saved-jwt"
        assert payload["kaderblick"]["jwt_refresh_token"] == "saved-refresh"

    def test_save_can_explicitly_clear_kaderblick_tokens(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        config_file.parent.mkdir(parents=True, exist_ok=True)
        config_file.write_text(
            json.dumps(
                {
                    "kaderblick": {
                        "auth_mode": "jwt",
                        "jwt_token": "saved-jwt",
                        "jwt_refresh_token": "saved-refresh",
                        "bearer_token": "",
                    }
                }
            ),
            encoding="utf-8",
        )

        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        app_settings = settings_module.AppSettings.load()
        app_settings.kaderblick.jwt_token = ""
        app_settings.kaderblick.jwt_refresh_token = ""
        app_settings.save(preserve_existing_secrets=False)

        payload = json.loads(config_file.read_text(encoding="utf-8"))
        assert payload["kaderblick"]["jwt_token"] == ""
        assert payload["kaderblick"]["jwt_refresh_token"] == ""

    def test_save_only_merges_existing_secrets_from_config_file(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        app_settings = settings_module.AppSettings()
        app_settings.last_directory = "/neu"
        app_settings.save()

        payload = json.loads(config_file.read_text(encoding="utf-8"))
        assert payload["last_directory"] == "/neu"
        assert payload["kaderblick"]["jwt_token"] == ""
        assert payload["kaderblick"]["jwt_refresh_token"] == ""

    def test_save_writes_to_config_settings_file(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        app_settings = settings_module.AppSettings()
        app_settings.restore_last_workflow = True
        app_settings.save()

        assert config_file.exists()
        payload = json.loads(config_file.read_text(encoding="utf-8"))
        assert payload["restore_last_workflow"] is True

    def test_save_and_load_preserve_global_workflow_output_root(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        app_settings = settings_module.AppSettings()
        app_settings.workflow_output_root = "/media/video/workflows"
        app_settings.save()

        loaded = settings_module.AppSettings.load()

        assert loaded.workflow_output_root == "/media/video/workflows"
        assert loaded.workflow_output_dir_for("Spieltag 23") == f"/media/video/workflows/Spieltag 23 {date.today().isoformat()}"
        assert loaded.workflow_output_dir_for("Spieltag 23", "Pi Nord") == "/media/video/workflows/Pi Nord"

    def test_save_and_load_preserve_camera_settings(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        app_settings = settings_module.AppSettings()
        app_settings.cameras.source = "/srv/pi/recordings"
        app_settings.cameras.devices = [
            DeviceSettings(
                name="Pi Nord",
                ip="10.0.0.10",
                port=2222,
                username="pi",
                password="secret",
                ssh_key="/home/test/.ssh/pi_nord",
            )
        ]

        app_settings.save()
        loaded = settings_module.AppSettings.load()

        assert loaded.cameras.source == "/srv/pi/recordings"
        assert len(loaded.cameras.devices) == 1
        assert loaded.cameras.devices[0].name == "Pi Nord"
        assert loaded.cameras.devices[0].ip == "10.0.0.10"
        assert loaded.cameras.devices[0].port == 2222
        assert loaded.cameras.devices[0].ssh_key == "/home/test/.ssh/pi_nord"


class TestKaderblickSettingsDialog:
    def test_dialog_restores_bearer_selection(self):
        settings = settings_module.AppSettings()
        settings.kaderblick.auth_mode = "bearer"

        dlg = KaderblickSettingsDialog(None, settings)

        assert dlg._rb_bearer.isChecked() is True
        assert dlg._rb_jwt.isChecked() is False

    def test_dialog_defaults_to_jwt_when_auth_mode_blank(self):
        settings = settings_module.AppSettings()
        settings.kaderblick.auth_mode = ""

        dlg = KaderblickSettingsDialog(None, settings)

        assert dlg._rb_jwt.isChecked() is True
        assert dlg._rb_bearer.isChecked() is False

    def test_dialog_save_persists_bearer_selection(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        settings = settings_module.AppSettings()
        dlg = KaderblickSettingsDialog(None, settings)
        dlg._rb_bearer.setChecked(True)
        dlg.bearer_token_edit.setText("bearer-value")

        dlg._save()

        assert settings.kaderblick.auth_mode == "bearer"
        payload = json.loads(config_file.read_text(encoding="utf-8"))
        assert payload["kaderblick"]["auth_mode"] == "bearer"


class TestCameraSettingsDialog:
    def test_dialog_save_persists_camera_settings(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        settings = settings_module.AppSettings()
        settings.cameras.devices = [
            DeviceSettings(name="Pi Süd", ip="10.0.0.11", port=22, username="pi", password="pw")
        ]
        dlg = CameraSettingsDialog(None, settings)
        dlg._source_edit.setText("/srv/cameras")

        dlg._save()

        payload = json.loads(config_file.read_text(encoding="utf-8"))
        assert payload["cameras"]["source"] == "/srv/cameras"
        assert "destination" not in payload["cameras"]
        assert "delete_after_download" not in payload["cameras"]
        assert "auto_convert" not in payload["cameras"]
        assert payload["cameras"]["devices"][0]["name"] == "Pi Süd"
        assert payload["cameras"]["devices"][0]["ip"] == "10.0.0.11"


class TestGeneralSettingsDialog:
    def test_dialog_save_persists_global_workflow_output_root(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        settings = settings_module.AppSettings()
        dlg = GeneralSettingsDialog(None, settings)
        dlg.output_root_edit.setText("/srv/video/workflows")

        dlg._save()

        payload = json.loads(config_file.read_text(encoding="utf-8"))
        assert settings.workflow_output_root == "/srv/video/workflows"
        assert payload["workflow_output_root"] == "/srv/video/workflows"

    def test_dialog_shows_live_youtube_preview_for_global_match_data(self, monkeypatch):
        monkeypatch.setattr("src.ui.dialogs.general.load_memory", lambda: {})

        settings = settings_module.AppSettings(
            default_match_date="2026-03-22",
            default_match_competition="Pokal",
            default_match_home_team="FC Heim",
            default_match_away_team="FC Gast",
            default_match_location="Sportplatz Mitte",
        )
        dlg = GeneralSettingsDialog(None, settings)

        assert dlg.playlist_preview.text() == "22.03.2026 | Pokal | FC Heim vs FC Gast"
        assert dlg.title_preview.text() == "2026-03-22 | FC Heim vs FC Gast | 1. Halbzeit"
        assert "Sportplatz Mitte" in dlg.description_preview.toPlainText()

        dlg.match_competition_edit.setCurrentText("Kreispokal")
        dlg.match_location_edit.setCurrentText("Stadion Nord")

        assert dlg.playlist_preview.text() == "22.03.2026 | Kreispokal | FC Heim vs FC Gast"
        assert "Stadion Nord" in dlg.description_preview.toPlainText()

    def test_dialog_shows_central_match_values_as_real_input_content(self, monkeypatch):
        monkeypatch.setattr("src.ui.dialogs.general.load_memory", lambda: {})

        settings = settings_module.AppSettings(
            default_match_date="2026-03-22",
            default_match_competition="Pokal",
            default_match_home_team="FC Heim",
            default_match_away_team="FC Gast",
            default_match_location="Sportplatz Mitte",
            default_kaderblick_game_id="4711",
        )

        dlg = GeneralSettingsDialog(None, settings)

        assert dlg.match_competition_edit.currentText() == "Pokal"
        assert dlg.match_home_edit.currentText() == "FC Heim"
        assert dlg.match_away_edit.currentText() == "FC Gast"
        assert dlg.match_location_edit.currentText() == "Sportplatz Mitte"
        assert dlg.kb_game_id_edit.text() == "4711"
        assert dlg.match_competition_edit.lineEdit().placeholderText() == "z. B. Kreispokal"
        assert dlg.match_date_edit.text() == "22.03.2026"

    def test_dialog_can_clear_global_match_date_and_persist_blank_value(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        saved_memory = {}
        monkeypatch.setattr("src.ui.dialogs.general.load_memory", lambda: {})
        monkeypatch.setattr("src.ui.dialogs.general.save_memory", lambda data: saved_memory.update(data))

        settings = settings_module.AppSettings(default_match_date="2026-03-22")
        dlg = GeneralSettingsDialog(None, settings)

        dlg.match_date_edit.setText("")
        dlg._save()

        payload = json.loads(config_file.read_text(encoding="utf-8"))
        assert settings.default_match_date == ""
        assert payload["default_match_date"] == ""
        assert saved_memory["last_match"]["date_iso"] == ""

    def test_dialog_date_picker_sets_global_match_date(self, monkeypatch):
        monkeypatch.setattr("src.ui.dialogs.general.load_memory", lambda: {})

        dlg = GeneralSettingsDialog(None, settings_module.AppSettings())

        dlg.match_date_edit._on_calendar_date_selected(QDate(2026, 3, 22))

        assert dlg.match_date_edit.text() == "22.03.2026"
        assert dlg.match_date_edit.isoValue() == "2026-03-22"

    def test_dialog_uses_history_dropdowns_and_persists_match_memory(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)

        saved_memory = {}

        monkeypatch.setattr(
            "src.ui.dialogs.general.load_memory",
            lambda: {
                "last_match": {
                    "date_iso": "2026-03-20",
                    "competition": "Liga",
                    "home_team": "Heim Alt",
                    "away_team": "Gast Alt",
                    "location": "Altstadion",
                },
                "history_competition": ["Liga", "Pokal"],
                "history_home_team": ["Heim Alt"],
                "history_away_team": ["Gast Alt"],
                "history_location": ["Altstadion"],
            },
        )
        monkeypatch.setattr("src.ui.dialogs.general.save_memory", lambda data: saved_memory.update(data))

        settings = settings_module.AppSettings()
        settings.default_match_competition = "Zentral Liga"
        settings.default_match_home_team = "Zentral Heim"
        settings.default_match_away_team = "Zentral Gast"
        settings.default_match_location = "Zentraler Platz"
        settings.default_kaderblick_game_id = "314"
        dlg = GeneralSettingsDialog(None, settings)

        assert dlg.match_competition_edit.currentText() == "Zentral Liga"
        assert dlg.match_competition_edit.count() == 2
        assert dlg.match_home_edit.currentText() == "Zentral Heim"
        assert dlg.match_away_edit.currentText() == "Zentral Gast"
        assert dlg.match_location_edit.currentText() == "Zentraler Platz"
        assert dlg.kb_game_id_edit.text() == "314"
        assert dlg.match_date_edit.text() == date.today().strftime("%d.%m.%Y")

        dlg.match_competition_edit.setCurrentText("Kreispokal")
        dlg.match_home_edit.setCurrentText("FC Heim")
        dlg.match_away_edit.setCurrentText("FC Gast")
        dlg.match_location_edit.setCurrentText("Sportplatz Mitte")
        dlg.kb_game_id_edit.setText("99")
        dlg._save()

        payload = json.loads(config_file.read_text(encoding="utf-8"))
        assert payload["workflow_output_root"] == ""
        assert payload["default_match_competition"] == "Kreispokal"
        assert payload["default_match_home_team"] == "FC Heim"
        assert payload["default_match_away_team"] == "FC Gast"
        assert payload["default_match_location"] == "Sportplatz Mitte"
        assert payload["default_kaderblick_game_id"] == "99"
        assert saved_memory["last_match"]["date_iso"] == date.today().isoformat()
        assert saved_memory["last_match"]["competition"] == "Kreispokal"
        assert saved_memory["last_match"]["home_team"] == "FC Heim"
        assert saved_memory["last_match"]["away_team"] == "FC Gast"
        assert saved_memory["last_match"]["location"] == "Sportplatz Mitte"
        assert saved_memory["history_competition"][0] == "Kreispokal"
        assert saved_memory["history_home_team"][0] == "FC Heim"
        assert saved_memory["history_away_team"][0] == "FC Gast"
        assert saved_memory["history_location"][0] == "Sportplatz Mitte"

    def test_runtime_metadata_resolution_uses_global_defaults_when_job_fields_are_blank(self):
        settings = settings_module.AppSettings(
            default_match_date="2026-03-22",
            default_match_competition="Pokal",
            default_match_home_team="FC Heim",
            default_match_away_team="FC Gast",
            default_match_location="Sportplatz Mitte",
            default_kaderblick_game_id="4711",
        )
        job = WorkflowJob(
            youtube_segment_data={"camera": "Hauptkamera", "type_name": "1. Halbzeit", "half": 1},
        )

        metadata = ExecutorSupport.resolve_youtube_metadata(job, "/tmp/spiel.mp4", settings=settings)

        assert metadata["title"] == "2026-03-22 | FC Heim vs FC Gast | Hauptkamera | 1. Halbzeit"
        assert metadata["playlist"] == "22.03.2026 | Pokal | FC Heim vs FC Gast"
        assert "Sportplatz Mitte" in metadata["description"]
        assert ExecutorSupport.resolve_kaderblick_game_id(settings, job) == "4711"