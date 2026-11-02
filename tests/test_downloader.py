from pathlib import Path
import shlex
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.transfer.downloader import (
    _build_ssh_cmd,
    _can_use_rsync,
    _noninteractive_ssh_env,
    _ssh_connect_kwargs,
    download_device,
)


def _device() -> SimpleNamespace:
    return SimpleNamespace(
        name="Cam1",
        ip="1.2.3.4",
        port=22,
        username="pi",
        password="",
        ssh_key="",
    )


def _config() -> SimpleNamespace:
    return SimpleNamespace(source="/remote")


class _DummySftp:
    def listdir(self, _path):
        return ["take1.mjpg", "take1.wav"]

    def close(self):
        return None


class _DummyClient:
    def close(self):
        return None


def test_download_device_restart_ignores_existing_complete_local_files(tmp_path):
    dest_root = tmp_path / "downloads"
    dest_root.mkdir(parents=True, exist_ok=True)
    local_mjpg = dest_root / "take1.mjpg"
    local_wav = dest_root / "take1.wav"
    local_mjpg.write_bytes(b"old-mjpg")
    local_wav.write_bytes(b"old-wav")

    calls: list[tuple[str, str, bool]] = []

    def _fake_rsync(device, remote_path, local_path, total_size, allow_reuse_existing=True, **_kwargs):
        calls.append((remote_path, local_path, allow_reuse_existing))
        Path(local_path).write_bytes(b"x" * total_size)
        return True

    with patch("src.transfer.downloader._connect", return_value=(_DummyClient(), _DummySftp())), \
         patch("src.transfer.downloader._can_use_rsync", return_value=True), \
         patch("src.transfer.downloader._remote_size", side_effect=[10, 5]), \
         patch("src.transfer.downloader._rsync_download_file", side_effect=_fake_rsync):
        results = download_device(
            device=_device(),
            config=_config(),
            destination_override=str(dest_root),
            create_device_subdir=False,
            allow_reuse_existing=False,
        )

    assert len(results) == 1
    assert local_mjpg.stat().st_size == 10
    assert local_wav.stat().st_size == 5
    assert calls == [
        ("/remote/take1.mjpg", str(local_mjpg), False),
        ("/remote/take1.wav", str(local_wav), False),
    ]


def test_can_use_rsync_is_disabled_when_platform_support_is_missing():
    with patch("src.transfer.downloader.supports_rsync", return_value=False), \
         patch("src.transfer.downloader.shutil.which", return_value="/usr/bin/rsync"):
        assert _can_use_rsync(_device()) is False


def test_build_ssh_cmd_uses_runtime_null_device_path():
    with patch("src.transfer.downloader.null_device_path", return_value="NUL"):
        cmd = _build_ssh_cmd(_device())

    assert "UserKnownHostsFile=NUL" in cmd


def test_build_ssh_cmd_uses_only_password_auth_when_password_is_configured():
    device = _device()
    device.ssh_key = "/tmp/camera key"
    device.password = "secret"

    parts = shlex.split(_build_ssh_cmd(device))

    assert "-i" not in parts
    assert "PubkeyAuthentication=no" in parts
    assert "IdentityAgent=none" in parts
    assert "PreferredAuthentications=password" in parts
    assert "KbdInteractiveAuthentication=no" in parts
    assert "PasswordAuthentication=yes" in parts
    assert "NumberOfPasswordPrompts=1" in parts


def test_build_ssh_cmd_uses_only_configured_key_without_password():
    device = _device()
    device.ssh_key = "/tmp/camera key"

    parts = shlex.split(_build_ssh_cmd(device))

    assert ["-i", "/tmp/camera key"] == parts[parts.index("-i"):parts.index("-i") + 2]
    assert "IdentitiesOnly=yes" in parts
    assert "IdentityAgent=none" in parts
    assert "BatchMode=yes" in parts


def test_paramiko_connection_uses_only_password_when_password_is_configured():
    device = _device()
    device.ssh_key = "/tmp/camera key"
    device.password = "secret"

    kwargs = _ssh_connect_kwargs(device, timeout=30)

    assert kwargs["password"] == "secret"
    assert kwargs["look_for_keys"] is False
    assert kwargs["allow_agent"] is False
    assert "key_filename" not in kwargs


def test_noninteractive_ssh_env_disables_graphical_password_prompts():
    inherited = {
        "SSH_ASKPASS": "/usr/bin/ksshaskpass",
        "SSH_ASKPASS_REQUIRE": "prefer",
        "DISPLAY": ":0",
        "WAYLAND_DISPLAY": "wayland-0",
        "KEEP": "value",
    }

    with patch.dict("src.transfer.downloader.os.environ", inherited, clear=True):
        env = _noninteractive_ssh_env()

    assert "SSH_ASKPASS" not in env
    assert "DISPLAY" not in env
    assert "WAYLAND_DISPLAY" not in env
    assert env["SSH_ASKPASS_REQUIRE"] == "never"
    assert env["KEEP"] == "value"


def test_download_device_raises_when_selected_recording_transfer_fails(tmp_path):
    device = _device()

    with patch("src.transfer.downloader._connect", return_value=(_DummyClient(), _DummySftp())), \
         patch("src.transfer.downloader._can_use_rsync", return_value=True), \
         patch("src.transfer.downloader._remote_size", side_effect=[10, 5]), \
         patch("src.transfer.downloader._rsync_download_file", return_value=False):
        with pytest.raises(RuntimeError, match="unvollständig oder fehlgeschlagen: take1"):
            download_device(
                device=device,
                config=_config(),
                destination_override=str(tmp_path),
                create_device_subdir=False,
                selective_bases={"take1"},
                raise_on_failure=True,
            )
