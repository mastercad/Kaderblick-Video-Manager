import json

from src import settings as settings_module


class TestSettingsPaths:
    def test_settings_file_is_stored_in_config_directory(self):
        assert settings_module.SETTINGS_FILE == settings_module._CONFIG_DIR / "settings.json"
        assert settings_module.SESSION_FILE == settings_module._DATA_DIR / "session.json"


class TestAppSettingsLoad:
    def test_load_uses_legacy_data_settings_when_config_file_missing(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        legacy_file = tmp_path / "data" / "settings.json"
        legacy_file.parent.mkdir(parents=True, exist_ok=True)
        legacy_file.write_text(
            json.dumps(
                {
                    "video": {"fps": 30},
                    "restore_session": True,
                }
            ),
            encoding="utf-8",
        )

        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)
        monkeypatch.setattr(settings_module, "LEGACY_SETTINGS_FILE", legacy_file)

        loaded = settings_module.AppSettings.load()

        assert loaded.video.fps == 30
        assert loaded.restore_session is True

    def test_save_writes_to_config_settings_file(self, monkeypatch, tmp_path):
        config_file = tmp_path / "config" / "settings.json"
        legacy_file = tmp_path / "data" / "settings.json"
        monkeypatch.setattr(settings_module, "SETTINGS_FILE", config_file)
        monkeypatch.setattr(settings_module, "LEGACY_SETTINGS_FILE", legacy_file)

        app_settings = settings_module.AppSettings()
        app_settings.restore_session = True
        app_settings.save()

        assert config_file.exists()
        assert not legacy_file.exists()
        payload = json.loads(config_file.read_text(encoding="utf-8"))
        assert payload["restore_session"] is True