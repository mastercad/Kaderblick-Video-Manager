import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.media.converter import ConvertJob
from src.settings import AppSettings
from src.workflow import FileEntry, WorkflowJob
from src.workflow_steps import ExecutorSupport, OutputStepStack, PreparedOutput


class _DummyEmitter:
    def __init__(self):
        self.values = []

    def emit(self, value, *args):
        self.values.append((value, *args))


class _FakeExecutor:
    def __init__(self):
        self._cancel = threading.Event()
        self._concat_func = lambda *args, **kwargs: True
        self._youtube_convert_func = lambda *args, **kwargs: True
        self.log_message = _DummyEmitter()
        self.phase_changed = _DummyEmitter()
        self.job_progress = _DummyEmitter()
        self.status_updates = []
        self.step_details = []

    def _set_step_status(self, job, step, status):
        if not isinstance(job.step_statuses, dict):
            job.step_statuses = {}
        job.step_statuses[step] = status

    def _set_job_status(self, orig_idx, status):
        self.status_updates.append((orig_idx, status))

    def _set_step_detail(self, job, step, detail):
        if not isinstance(job.step_details, dict):
            job.step_details = {}
        job.step_details[step] = detail
        self.step_details.append((step, detail))

    @staticmethod
    def _find_file_entry(job, file_path):
        target = Path(file_path).name
        for entry in job.files:
            if Path(entry.source_path).name == target:
                return entry
        return None

    @staticmethod
    def _prepared_output_reaches_type(prepared, target_type):
        return ExecutorSupport.prepared_output_reaches_type(prepared, target_type)

    @staticmethod
    def _graph_node_id_for_type(job, node_type):
        return ExecutorSupport.graph_node_id_for_type(job, node_type)

    @staticmethod
    def _validation_branch_has_targets(prepared, node_type, branch):
        return ExecutorSupport.validation_branch_has_targets(prepared, node_type, branch)


def _prepared_output(tmp_path, *, title=False, yt_version=False, yt_upload=False, kaderblick=False):
    source = tmp_path / "clip-source.mp4"
    output = tmp_path / "clip-output.mp4"
    source.write_text("src", encoding="utf-8")
    output.write_text("out", encoding="utf-8")

    job = WorkflowJob(
        title_card_enabled=title,
        create_youtube_version=yt_version,
        upload_youtube=yt_upload,
        upload_kaderblick=kaderblick,
        default_kaderblick_game_id="game-1",
        default_kaderblick_video_type_id=4,
        default_kaderblick_camera_id=8,
        files=[FileEntry(source_path=str(source), kaderblick_game_id="game-1")],
    )
    cv_job = ConvertJob(
        source_path=source,
        output_path=output,
        job_type="convert",
        youtube_title="Titel",
    )
    return PreparedOutput(
        orig_idx=0,
        job=job,
        cv_job=cv_job,
        per_settings=AppSettings(),
        mark_finished=True,
    )


@pytest.mark.parametrize(
    "title,yt_version,yt_upload,kaderblick,expected_calls",
    [
        (False, False, False, False, []),
        (True, False, False, False, ["titlecard"]),
        (False, True, False, False, ["yt_version"]),
        (False, False, True, False, ["youtube_upload"]),
        (False, False, True, True, ["youtube_upload", "kaderblick"]),
        (True, True, False, False, ["titlecard", "yt_version"]),
        (True, False, True, True, ["titlecard", "youtube_upload", "kaderblick"]),
        (True, True, True, True, ["titlecard", "yt_version", "youtube_upload", "kaderblick"]),
        (False, True, False, True, ["yt_version"]),
    ],
)
def test_output_step_stack_combination_matrix(
    tmp_path,
    title,
    yt_version,
    yt_upload,
    kaderblick,
    expected_calls,
):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(
        tmp_path,
        title=title,
        yt_version=yt_version,
        yt_upload=yt_upload,
        kaderblick=kaderblick,
    )
    calls = []

    def _title_side_effect(_executor, _orig_idx, cv_job, _job, _settings):
        calls.append("titlecard")
        return cv_job.output_path, True

    def _yt_convert_side_effect(*_args, **_kwargs):
        calls.append("yt_version")
        return True

    def _upload_side_effect(*_args, **_kwargs):
        calls.append("youtube_upload")
        return True

    def _kb_side_effect(*_args, **_kwargs):
        calls.append("kaderblick")
        return True

    executor._youtube_convert_func = _yt_convert_side_effect

    with patch(
        "src.workflow_steps.title_card_step.TitleCardStep._prepend_title_card",
        side_effect=_title_side_effect,
    ), patch(
        "src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube",
        side_effect=_upload_side_effect,
    ), patch(
        "src.workflow_steps.kaderblick_post_step.KaderblickPostStep._post_to_kaderblick",
        side_effect=_kb_side_effect,
    ), patch(
        "src.workflow_steps.kaderblick_post_step.get_video_id_for_output",
        return_value="vid-123",
    ):
        failures = stack.execute(
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={("game-1", prepared.cv_job.source_path.name): 1},
        )

    assert failures == 0
    assert calls == expected_calls


def test_output_step_stack_skips_upload_and_kaderblick_without_yt_service(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, yt_upload=True, kaderblick=True)
    calls = []

    executor._youtube_convert_func = lambda *_args, **_kwargs: calls.append("yt_version") or True

    with patch(
        "src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube",
        side_effect=lambda *_args, **_kwargs: calls.append("youtube_upload") or True,
    ) as upload_mock, patch(
        "src.workflow_steps.kaderblick_post_step.kaderblick_post",
        side_effect=lambda *_args, **_kwargs: calls.append("kaderblick_post") or True,
    ) as kb_post_mock, patch(
        "src.workflow_steps.kaderblick_post_step.get_video_id_for_output",
        return_value=None,
    ):
        failures = stack.execute(
            executor,
            prepared,
            yt_service=None,
            kb_sort_index={("game-1", prepared.cv_job.source_path.name): 1},
        )

    assert failures == 0
    assert calls == []
    upload_mock.assert_not_called()
    kb_post_mock.assert_not_called()


def test_output_step_stack_stops_after_upload_failure(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=True, yt_version=True, yt_upload=True, kaderblick=True)
    calls = []

    def _title_side_effect(_executor, _orig_idx, cv_job, _job, _settings):
        calls.append("titlecard")
        return cv_job.output_path, True

    executor._youtube_convert_func = lambda *_args, **_kwargs: calls.append("yt_version") or True

    with patch(
        "src.workflow_steps.title_card_step.TitleCardStep._prepend_title_card",
        side_effect=_title_side_effect,
    ), patch(
        "src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube",
        side_effect=lambda *_args, **_kwargs: calls.append("youtube_upload") or False,
    ), patch(
        "src.workflow_steps.kaderblick_post_step.KaderblickPostStep._post_to_kaderblick",
        side_effect=lambda *_args, **_kwargs: calls.append("kaderblick") or True,
    ), patch(
        "src.workflow_steps.kaderblick_post_step.get_video_id_for_output",
        return_value="vid-123",
    ):
        failures = stack.execute(
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={("game-1", prepared.cv_job.source_path.name): 1},
        )

    assert failures == 1
    assert calls == ["titlecard", "yt_version", "youtube_upload"]


def test_output_steps_emit_progress_and_status_for_titlecard_and_ytversion(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=True, yt_version=True, yt_upload=False, kaderblick=False)

    def _title_side_effect(_executor, orig_idx, cv_job, _job, _settings):
        _executor._set_job_status(orig_idx, "Titelkarte zusammenfuhren ...")
        _executor.job_progress.emit(orig_idx, 25)
        _executor.job_progress.emit(orig_idx, 75)
        return cv_job.output_path, True

    def _yt_convert_side_effect(*_args, **kwargs):
        kwargs["progress_callback"](40)
        kwargs["progress_callback"](100)
        return True

    with patch(
        "src.workflow_steps.title_card_step.TitleCardStep._prepend_title_card",
        side_effect=_title_side_effect,
    ):
        executor._youtube_convert_func = _yt_convert_side_effect
        failures = stack.execute(
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={("game-1", prepared.cv_job.source_path.name): 1},
        )

    assert failures == 0
    assert executor.job_progress.values == [
        (0, 0),
        (0, 25),
        (0, 75),
        (0, 100),
        (0, 0),
        (0, 40),
        (0, 100),
        (0, 100),
    ]
    assert executor.status_updates == [
        (0, "Titelkarte erstellen …"),
        (0, "Titelkarte zusammenfuhren ..."),
        (0, "YT-Version erstellen …"),
        (0, "Fertig"),
    ]
    assert prepared.job.step_statuses["titlecard"] == "done"
    assert prepared.job.step_statuses["yt_version"] == "done"


def test_kaderblick_step_emits_progress_and_status(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=False, yt_version=False, yt_upload=True, kaderblick=True)

    with patch(
        "src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube",
        return_value=True,
    ), patch(
        "src.workflow_steps.kaderblick_post_step.KaderblickPostStep._post_to_kaderblick",
        return_value=True,
    ), patch(
        "src.workflow_steps.kaderblick_post_step.get_video_id_for_output",
        return_value="vid-123",
    ):
        failures = stack.execute(
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={("game-1", prepared.cv_job.source_path.name): 1},
        )

    assert failures == 0
    assert executor.status_updates == [
        (0, "YouTube-Upload …"),
        (0, "Kaderblick senden …"),
        (0, "Fertig"),
    ]
    assert executor.job_progress.values == [
        (0, 0),
        (0, 100),
        (0, 0),
        (0, 100),
    ]
    assert prepared.job.step_statuses["youtube_upload"] == "done"
    assert prepared.job.step_statuses["kaderblick"] == "done"


def test_output_step_stack_reuses_existing_titlecard_output(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=True, yt_version=False, yt_upload=False, kaderblick=False)
    titlecard = prepared.cv_job.output_path.with_stem(prepared.cv_job.output_path.stem + "_titlecard")
    titlecard.write_text("with-titlecard", encoding="utf-8")

    with patch(
        "src.workflow_steps.title_card_step.TitleCardStep._prepend_title_card",
        side_effect=AssertionError("titlecard should be reused, not regenerated"),
    ):
        failures = stack.execute(
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    assert prepared.cv_job.output_path == titlecard
    assert prepared.job.step_statuses["titlecard"] == "reused-target"


def test_output_step_stack_reuses_existing_titlecard_when_base_output_is_missing(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=True, yt_version=False, yt_upload=False, kaderblick=False)
    prepared.cv_job.output_path.unlink()
    titlecard = prepared.cv_job.output_path.with_stem(prepared.cv_job.output_path.stem + "_titlecard")
    titlecard.write_text("with-titlecard", encoding="utf-8")

    failures = stack.execute(
        executor,
        prepared,
        yt_service=object(),
        kb_sort_index={},
    )

    assert failures == 0
    assert prepared.cv_job.output_path == titlecard
    assert prepared.job.step_statuses["titlecard"] == "reused-target"


def test_output_step_stack_reuses_existing_youtube_version(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=False, yt_version=True, yt_upload=False, kaderblick=False)
    yt_version = prepared.cv_job.output_path.with_stem(prepared.cv_job.output_path.stem + "_youtube")
    yt_version.write_text("yt", encoding="utf-8")
    executor._youtube_convert_func = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("yt version should be reused, not regenerated")
    )

    with patch("src.workflow_steps.youtube_version_step.validate_media_output", return_value=True):
        failures = stack.execute(
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    assert prepared.job.step_statuses["yt_version"] == "reused-target"


def test_output_step_stack_reuses_existing_youtube_upload(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=False, yt_version=False, yt_upload=True, kaderblick=False)

    with patch(
        "src.workflow_steps.youtube_upload_step.get_video_id_for_output",
        return_value="existing-123",
    ), patch(
        "src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube",
        side_effect=AssertionError("upload should be reused, not rerun"),
    ):
        failures = stack.execute(
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    assert prepared.job.step_statuses["youtube_upload"] == "reused-target"


def test_output_step_stack_reuses_existing_youtube_upload_when_base_file_is_missing(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=False, yt_version=False, yt_upload=True, kaderblick=False)
    prepared.cv_job.output_path.unlink()

    with patch(
        "src.workflow_steps.youtube_upload_step.get_video_id_for_output",
        return_value="existing-456",
    ):
        failures = stack.execute(
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    assert prepared.job.step_statuses["youtube_upload"] == "reused-target"


def test_output_step_stack_reuses_existing_kaderblick_entry(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=False, yt_version=False, yt_upload=True, kaderblick=True)

    with patch(
        "src.workflow_steps.youtube_upload_step.get_video_id_for_output",
        return_value="video-123",
    ), patch(
        "src.workflow_steps.kaderblick_post_step.get_video_id_for_output",
        return_value="video-123",
    ), patch(
        "src.workflow_steps.kaderblick_post_step.get_recorded_kaderblick_id",
        return_value=99,
    ), patch(
        "src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube",
        side_effect=AssertionError("upload should be reused, not rerun"),
    ), patch(
        "src.workflow_steps.kaderblick_post_step.KaderblickPostStep._post_to_kaderblick",
        side_effect=AssertionError("kaderblick should be reused, not reposted"),
    ):
        failures = stack.execute(
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    assert prepared.job.step_statuses["youtube_upload"] == "reused-target"
    assert prepared.job.step_statuses["kaderblick"] == "reused-target"
