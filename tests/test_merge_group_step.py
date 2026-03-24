from pathlib import Path
from types import SimpleNamespace

from src.media.converter import ConvertJob
from src.settings import AppSettings
from src.workflow import FileEntry, WorkflowJob
from src.workflow_steps.merge_group_step import MergeGroupStep
from src.workflow_steps.models import ConvertItem
from unittest.mock import patch


class _DummyEmitter:
    def __init__(self):
        self.values = []

    def emit(self, *args):
        self.values.append(args)


class _FakeExecutor:
    def __init__(self):
        self.log_message = _DummyEmitter()
        self.job_progress = _DummyEmitter()
        self._cancel = SimpleNamespace(is_set=lambda: False)
        self._concat_func = lambda *_args, **_kwargs: True
        self.status_updates = []

    @staticmethod
    def _build_job_settings(_job):
        return AppSettings()

    @staticmethod
    def _merge_precedes_convert(_job):
        return False

    @staticmethod
    def _set_step_status(job, step, status):
        if not isinstance(job.step_statuses, dict):
            job.step_statuses = {}
        job.step_statuses[step] = status

    @staticmethod
    def _set_step_detail(job, step, detail):
        if not isinstance(job.step_details, dict):
            job.step_details = {}
        job.step_details[step] = detail

    def _set_job_status(self, _orig_idx, status):
        self.status_updates.append(status)


def _merge_item(tmp_path, *, merge_title="Merge Titel", merge_playlist="Merge Playlist", merge_description="Merge Beschreibung"):
    source = tmp_path / "halbzeit1.mp4"
    source.write_text("video", encoding="utf-8")
    job = WorkflowJob(
        source_mode="files",
        files=[FileEntry(source_path=str(source), merge_group_id="g1")],
        merge_output_title=merge_title,
        merge_output_playlist=merge_playlist,
        merge_output_description=merge_description,
    )
    cv_job = ConvertJob(
        source_path=source,
        output_path=source,
        youtube_title="Alter Titel",
        youtube_playlist="Alte Playlist",
        youtube_description="Alte Beschreibung",
    )
    return job, ConvertItem(orig_idx=0, job=job, cv_job=cv_job)


def test_merge_group_step_reuses_merge_metadata_for_existing_merged_output(tmp_path):
    job, item = _merge_item(tmp_path)
    merged = Path(item.cv_job.output_path).with_stem("Merge Titel")
    merged.write_text("merged", encoding="utf-8")
    step = MergeGroupStep()

    with patch("src.workflow_steps.merge_group_step.validate_media_output", return_value=True):
        prepared, failures = step.execute(_FakeExecutor(), "g1", [item])

    assert failures == 0
    assert prepared is not None
    assert prepared.cv_job.output_path == merged
    assert prepared.cv_job.youtube_title == "Merge Titel"
    assert prepared.cv_job.youtube_playlist == "Merge Playlist"
    assert prepared.cv_job.youtube_description == "Merge Beschreibung"


def test_merge_group_step_uses_title_based_output_filename_for_merged_result(tmp_path):
    source_a = tmp_path / "halbzeit1.mp4"
    source_b = tmp_path / "halbzeit2.mp4"
    source_a.write_text("video-a", encoding="utf-8")
    source_b.write_text("video-b", encoding="utf-8")
    job = WorkflowJob(
        source_mode="files",
        files=[
            FileEntry(source_path=str(source_a), merge_group_id="g1"),
            FileEntry(source_path=str(source_b), merge_group_id="g1"),
        ],
        merge_output_title="2026-03-22 | Heim vs Gast | Kamera 1 | Links 1. Halbzeit",
    )
    item_a = ConvertItem(orig_idx=0, job=job, cv_job=ConvertJob(source_path=source_a, output_path=source_a))
    item_b = ConvertItem(orig_idx=0, job=job, cv_job=ConvertJob(source_path=source_b, output_path=source_b))
    executor = _FakeExecutor()

    def _concat(_sources, output, **_kwargs):
        output.write_text("merged", encoding="utf-8")
        return True

    executor._concat_func = _concat
    step = MergeGroupStep()

    prepared, failures = step.execute(executor, "g1", [item_a, item_b])

    assert failures == 0
    assert prepared is not None
    assert prepared.cv_job.output_path is not None
    assert prepared.cv_job.output_path.name == "2026-03-22 - Heim vs Gast - Kamera 1 - Links 1. Halbzeit.mp4"


def test_merge_group_step_reuses_merge_metadata_for_single_source_merge(tmp_path):
    job, item = _merge_item(tmp_path)
    step = MergeGroupStep()
    executor = _FakeExecutor()

    prepared, failures = step.execute(executor, "g1", [item])

    assert failures == 0
    assert prepared is not None
    assert prepared.cv_job.output_path == item.cv_job.source_path
    assert prepared.cv_job.youtube_title == "Merge Titel"
    assert prepared.cv_job.youtube_playlist == "Merge Playlist"
    assert prepared.cv_job.youtube_description == "Merge Beschreibung"
    assert job.step_statuses["merge"] == "reused-target"
    assert "Zusammenführen OK" in executor.status_updates


def test_merge_group_step_marks_successful_concat_as_done(tmp_path):
    source_a = tmp_path / "halbzeit1.mp4"
    source_b = tmp_path / "halbzeit2.mp4"
    source_a.write_text("video-a", encoding="utf-8")
    source_b.write_text("video-b", encoding="utf-8")
    job = WorkflowJob(
        source_mode="files",
        files=[
            FileEntry(source_path=str(source_a), merge_group_id="g1"),
            FileEntry(source_path=str(source_b), merge_group_id="g1"),
        ],
    )
    item_a = ConvertItem(orig_idx=0, job=job, cv_job=ConvertJob(source_path=source_a, output_path=source_a))
    item_b = ConvertItem(orig_idx=0, job=job, cv_job=ConvertJob(source_path=source_b, output_path=source_b))
    executor = _FakeExecutor()

    def _concat(_sources, output, **_kwargs):
        output.write_text("merged", encoding="utf-8")
        return True

    executor._concat_func = _concat
    step = MergeGroupStep()

    prepared, failures = step.execute(executor, "g1", [item_a, item_b])

    assert failures == 0
    assert prepared is not None
    assert job.step_statuses["merge"] == "done"
    assert executor.status_updates[-1] == "Zusammenführen OK"


def test_merge_group_step_uses_merge_specific_preset_and_no_bframes(tmp_path):
    source_a = tmp_path / "halbzeit1.mp4"
    source_b = tmp_path / "halbzeit2.mp4"
    source_a.write_text("video-a", encoding="utf-8")
    source_b.write_text("video-b", encoding="utf-8")
    job = WorkflowJob(
        source_mode="files",
        files=[
            FileEntry(source_path=str(source_a), merge_group_id="g1"),
            FileEntry(source_path=str(source_b), merge_group_id="g1"),
        ],
        merge_preset="slower",
        merge_no_bframes=False,
    )
    item_a = ConvertItem(orig_idx=0, job=job, cv_job=ConvertJob(source_path=source_a, output_path=source_a))
    item_b = ConvertItem(orig_idx=0, job=job, cv_job=ConvertJob(source_path=source_b, output_path=source_b))
    executor = _FakeExecutor()
    captured: dict[str, object] = {}

    def _concat(_sources, output, **kwargs):
        captured.update(kwargs)
        output.write_text("merged", encoding="utf-8")
        return True

    executor._concat_func = _concat
    step = MergeGroupStep()

    prepared, failures = step.execute(executor, "g1", [item_a, item_b])

    assert failures == 0
    assert prepared is not None
    assert captured["preset"] == "slower"
    assert captured["no_bframes"] is False
    assert captured["target_resolution"] == "source"


def test_merge_group_step_preserves_source_container_when_configured(tmp_path):
    source_a = tmp_path / "halbzeit1.avi"
    source_b = tmp_path / "halbzeit2.avi"
    source_a.write_text("video-a", encoding="utf-8")
    source_b.write_text("video-b", encoding="utf-8")
    job = WorkflowJob(
        source_mode="files",
        files=[
            FileEntry(source_path=str(source_a), merge_group_id="g1"),
            FileEntry(source_path=str(source_b), merge_group_id="g1"),
        ],
        merge_output_format="source",
    )
    item_a = ConvertItem(orig_idx=0, job=job, cv_job=ConvertJob(source_path=source_a, output_path=source_a))
    item_b = ConvertItem(orig_idx=0, job=job, cv_job=ConvertJob(source_path=source_b, output_path=source_b))
    executor = _FakeExecutor()

    def _concat(_sources, output, **_kwargs):
        output.write_text("merged", encoding="utf-8")
        return True

    executor._concat_func = _concat
    step = MergeGroupStep()

    prepared, failures = step.execute(executor, "g1", [item_a, item_b])

    assert failures == 0
    assert prepared is not None
    assert prepared.cv_job.output_path is not None
    assert prepared.cv_job.output_path.suffix == ".avi"


def test_merge_group_step_passes_selected_output_resolution(tmp_path):
    source_a = tmp_path / "halbzeit1.mp4"
    source_b = tmp_path / "halbzeit2.mp4"
    source_a.write_text("video-a", encoding="utf-8")
    source_b.write_text("video-b", encoding="utf-8")
    job = WorkflowJob(
        source_mode="files",
        files=[
            FileEntry(source_path=str(source_a), merge_group_id="g1"),
            FileEntry(source_path=str(source_b), merge_group_id="g1"),
        ],
        merge_output_resolution="1080p",
    )
    item_a = ConvertItem(orig_idx=0, job=job, cv_job=ConvertJob(source_path=source_a, output_path=source_a))
    item_b = ConvertItem(orig_idx=0, job=job, cv_job=ConvertJob(source_path=source_b, output_path=source_b))
    executor = _FakeExecutor()
    captured: dict[str, object] = {}

    def _concat(_sources, output, **kwargs):
        captured.update(kwargs)
        output.write_text("merged", encoding="utf-8")
        return True

    executor._concat_func = _concat
    step = MergeGroupStep()

    prepared, failures = step.execute(executor, "g1", [item_a, item_b])

    assert failures == 0
    assert prepared is not None
    assert captured["target_resolution"] == "1080p"