from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.transfer.downloader import _build_ssh_cmd, _can_use_rsync, download_device


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