import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from PySide6.QtCore import QCoreApplication

from src.runtime.workflow_executor import WorkflowExecutor
from src.settings import AppSettings, DeviceSettings
from src.workflow import FileEntry, Workflow, WorkflowJob
from src.workflow_steps.pi_download_transfer_step import PiDownloadTransferStep


_app = QCoreApplication.instance() or QCoreApplication([])


class _Signal:
    def __init__(self):
        self.calls = []

    def emit(self, *args):
        self.calls.append(args)


def _executor(tmp_path: Path, download_func):
    device = SimpleNamespace(name="Kaderblick Kamera Rechts", ip="192.168.178.48")
    cameras = SimpleNamespace(devices=[device])
    settings = SimpleNamespace(
        cameras=cameras,
        workflow_raw_dir_for=lambda _job_name, _device_name: str(tmp_path),
    )
    return SimpleNamespace(
        _settings=settings,
        _download_func=download_func,
        _allow_reuse_existing=True,
        _cancel=threading.Event(),
        source_progress=_Signal(),
        file_progress=_Signal(),
        log_message=_Signal(),
        _set_step_status=lambda *_args: None,
        _set_step_detail=lambda *_args: None,
        _set_job_status=lambda *_args: None,
    )


def _job(tmp_path: Path, total_size: int = 15) -> WorkflowJob:
    return WorkflowJob(
        name="Kaderblick Kamera Rechts",
        source_mode="pi_download",
        device_name="Kaderblick Kamera Rechts",
        download_destination=str(tmp_path),
        files=[FileEntry(source_path=str(tmp_path / "take1.mjpg"), source_size_bytes=total_size)],
    )


def test_empty_download_result_is_a_transfer_error_instead_of_success(tmp_path):
    executor = _executor(tmp_path, lambda **_kwargs: [])

    with pytest.raises(RuntimeError, match="1 ausgewählte Aufnahme.*vollständig übertragen"):
        PiDownloadTransferStep().execute(executor, 0, _job(tmp_path))


def test_partial_local_recording_is_not_reused_after_download_failure(tmp_path):
    (tmp_path / "take1.mjpg").write_bytes(b"partial")
    (tmp_path / "take1.wav").write_bytes(b"audio")
    executor = _executor(tmp_path, lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("rsync failed")))

    with pytest.raises(RuntimeError, match="rsync failed"):
        PiDownloadTransferStep().execute(executor, 0, _job(tmp_path, total_size=100))


def test_complete_local_recording_can_be_reused_after_connection_failure(tmp_path):
    (tmp_path / "take1.mjpg").write_bytes(b"video-data")
    (tmp_path / "take1.wav").write_bytes(b"audio")
    total_size = (tmp_path / "take1.mjpg").stat().st_size + (tmp_path / "take1.wav").stat().st_size
    executor = _executor(tmp_path, lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("offline")))

    paths = PiDownloadTransferStep().execute(executor, 0, _job(tmp_path, total_size=total_size))

    assert paths == [str(tmp_path / "take1.mjpg")]


def test_pipeline_marks_empty_camera_download_as_error_not_finished(tmp_path):
    job = _job(tmp_path)
    workflow = Workflow(jobs=[job])
    settings = AppSettings(workflow_output_root=str(tmp_path))
    settings.cameras.devices = [DeviceSettings(
        name="Kaderblick Kamera Rechts",
        ip="192.168.178.48",
        username="kaderblick",
        password="kaderblick",
    )]
    executor = WorkflowExecutor(workflow, settings, active_indices={0})
    executor._download_func = lambda **_kwargs: []
    statuses = []
    executor.job_status.connect(lambda index, status: statuses.append((index, status)))

    ok, skipped, failed = executor._run_pipelined([(0, job)])

    assert (ok, skipped, failed) == (0, 0, 1)
    assert job.step_statuses["transfer"].startswith("error:")
    assert any(status.startswith("Fehler:") for _index, status in statuses)
    assert all(status != "Fertig" for _index, status in statuses)


def test_one_of_two_selected_recordings_does_not_mark_transfer_complete(tmp_path):
    job = _job(tmp_path)
    job.files.append(FileEntry(
        source_path=str(tmp_path / "take2.mjpg"),
        source_size_bytes=20,
    ))
    executor = _executor(
        tmp_path,
        lambda **_kwargs: [(str(tmp_path), "take1", str(tmp_path / "take1.mjpg"))],
    )

    with pytest.raises(RuntimeError, match="2 ausgewählte Aufnahme.*vollständig übertragen"):
        PiDownloadTransferStep().execute(executor, 0, job)
