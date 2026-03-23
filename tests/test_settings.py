import json

from PySide6.QtWidgets import QApplication

from src import settings as settings_module
from src.ui.dialogs import KaderblickSettingsDialog


_app = QApplication.instance() or QApplication([])


class TestSettingsPaths:
    def test_settings_file_is_stored_in_config_directory(self):
        assert settings_module.SETTINGS_FILE == settings_module._CONFIG_DIR / "settings.json"


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