"""pytest-Konfiguration für das Kaderblick — Video Manager-Projekt."""

import sys
from pathlib import Path

import pytest

# Projekt-Root zum Import-Pfad hinzufügen, damit `from src.xxx import …` funktioniert
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def isolate_persistent_project_files(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    data_dir = tmp_path / "data"
    config_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    settings_file = config_dir / "settings.json"
    client_secret_file = config_dir / "client_secret.json"
    token_file = data_dir / "youtube_token.json"
    state_file = data_dir / "integration_state.json"
    last_workflow_file = data_dir / "last_workflow.json"

    import src.settings as settings_module
    import src.workflow.storage as workflow_storage
    import src.integrations.state_store as state_store
    import src.integrations.youtube_title_editor as youtube_title_editor
    import src.integrations.youtube as youtube_module
    import src.integrations.kaderblick as kaderblick_module

    monkeypatch.setattr(settings_module, "SETTINGS_FILE", settings_file)
    monkeypatch.setattr(settings_module, "CLIENT_SECRET_FILE", client_secret_file)
    monkeypatch.setattr(settings_module, "TOKEN_FILE", token_file)
    monkeypatch.setattr(workflow_storage, "LAST_WORKFLOW_FILE", last_workflow_file)
    monkeypatch.setattr(state_store, "INTEGRATION_STATE_FILE", state_file)
    monkeypatch.setattr(youtube_module, "CLIENT_SECRET_FILE", client_secret_file)
    monkeypatch.setattr(youtube_module, "TOKEN_FILE", token_file)

    def load_state(path=None):
        target = state_file if path is None else path
        return state_store._read_json(target) or {}

    def save_state(data, path=None):
        target = state_file if path is None else path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(__import__("json").dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def load_section(section, *, path=None):
        state = load_state(path)
        section_data = state.get(section)
        return section_data if isinstance(section_data, dict) else {}

    def save_section(section, data, *, path=None):
        state = load_state(path)
        state[section] = data
        save_state(state, path)

    monkeypatch.setattr(state_store, "load_state", load_state)
    monkeypatch.setattr(state_store, "save_state", save_state)
    monkeypatch.setattr(state_store, "load_section", load_section)
    monkeypatch.setattr(state_store, "save_section", save_section)

    monkeypatch.setattr(youtube_title_editor, "load_section", load_section)
    monkeypatch.setattr(youtube_title_editor, "save_section", save_section)
    monkeypatch.setattr(youtube_module, "load_section", load_section)
    monkeypatch.setattr(youtube_module, "save_section", save_section)
    monkeypatch.setattr(kaderblick_module, "load_section", load_section)
    monkeypatch.setattr(kaderblick_module, "save_section", save_section)

    monkeypatch.setattr(youtube_module, "_registry", youtube_module.UploadRegistry())
    monkeypatch.setattr(kaderblick_module, "_registry", None)
