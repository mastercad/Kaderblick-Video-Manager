from pathlib import Path

import src.runtime_paths as runtime_paths


def test_frozen_runtime_uses_executable_dir_for_writable_state(monkeypatch, tmp_path):
    internal_root = tmp_path / "_internal"
    executable = tmp_path / "kaderblick-video-manager.exe"
    internal_root.mkdir()
    executable.write_text("", encoding="utf-8")

    monkeypatch.setattr(runtime_paths.sys, "frozen", True, raising=False)
    monkeypatch.setattr(runtime_paths.sys, "_MEIPASS", str(internal_root), raising=False)
    monkeypatch.setattr(runtime_paths.sys, "executable", str(executable), raising=False)

    assert runtime_paths.is_frozen_app() is True
    assert runtime_paths.portable_root() == tmp_path
    assert runtime_paths.bundled_resource_root() == internal_root
    assert runtime_paths.asset_path("icon.svg") == internal_root / "assets" / "icon.svg"
    assert runtime_paths.config_dir() == tmp_path / "config"
    assert runtime_paths.data_dir() == tmp_path / "data"
    assert runtime_paths.workflows_dir() == tmp_path / "workflows"


def test_bundled_binary_path_prefers_environment_override(monkeypatch):
    monkeypatch.setenv("KADERBLICK_FFMPEG_BIN", "/custom/ffmpeg")

    assert runtime_paths.bundled_binary_path("KADERBLICK_FFMPEG_BIN", "ffmpeg") == "/custom/ffmpeg"


def test_bundled_binary_path_uses_embedded_binary_when_present(monkeypatch, tmp_path):
    internal_root = tmp_path / "_internal"
    embedded_bin = internal_root / "bin"
    embedded_bin.mkdir(parents=True)
    ffmpeg = embedded_bin / "ffmpeg.exe"
    ffmpeg.write_text("", encoding="utf-8")

    monkeypatch.setattr(runtime_paths.sys, "frozen", True, raising=False)
    monkeypatch.setattr(runtime_paths.sys, "_MEIPASS", str(internal_root), raising=False)
    monkeypatch.setattr(runtime_paths.sys, "executable", str(tmp_path / "app.exe"), raising=False)
    monkeypatch.setattr(runtime_paths.platform, "system", lambda: "Windows")
    monkeypatch.delenv("KADERBLICK_FFMPEG_BIN", raising=False)

    assert runtime_paths.bundled_binary_path("KADERBLICK_FFMPEG_BIN", "ffmpeg") == str(ffmpeg)


def test_shutdown_command_matches_supported_platforms(monkeypatch):
    monkeypatch.setattr(runtime_paths.platform, "system", lambda: "Linux")
    assert runtime_paths.shutdown_command() == ["shutdown", "now"]

    monkeypatch.setattr(runtime_paths.platform, "system", lambda: "Darwin")
    assert runtime_paths.shutdown_command() == ["sudo", "shutdown", "-h", "now"]

    monkeypatch.setattr(runtime_paths.platform, "system", lambda: "Windows")
    assert runtime_paths.shutdown_command() == ["shutdown", "/s", "/t", "0"]

    monkeypatch.setattr(runtime_paths.platform, "system", lambda: "Plan9")
    assert runtime_paths.shutdown_command() is None