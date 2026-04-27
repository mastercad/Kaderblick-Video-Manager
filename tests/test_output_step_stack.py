import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.media.converter import ConvertJob
from src.settings import AppSettings
from src.workflow import FileEntry, WorkflowJob
from src.workflow_steps import ExecutorSupport, OutputStepStack, PreparedOutput


def _run_stack(stack, executor, prepared, yt_service=None, kb_sort_index=None):
    """Test-Helper: ersetzt das entfernte OutputStepStack.execute()."""
    failures = stack.execute_processing_steps(executor, prepared)
    if not failures and not executor._cancel.is_set():
        failures += stack.execute_delivery_steps(executor, prepared, yt_service, kb_sort_index or {})
    return failures


class _DummyEmitter:
    def __init__(self):
        self.values = []

    def emit(self, value, *args):
        self.values.append((value, *args))


class _FakeExecutor:
    def __init__(self):
        self._cancel = threading.Event()
        self._allow_reuse_existing = True
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
    def _is_job_cancelled(_orig_idx):
        return False

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
    def _advance_prepared_output_cursor(prepared, step_name):
        ExecutorSupport.advance_prepared_output_cursor(prepared, step_name)

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

    # Build a graph that only contains nodes for the requested steps so the
    # graph-walker in OutputStepStack visits exactly the right steps.
    nodes = [{"id": "src-1", "type": "source_files"}]
    edges: list[dict] = []
    prev = "src-1"
    if title:
        nodes.append({"id": "tc-1", "type": "titlecard"})
        edges.append({"source": prev, "target": "tc-1"})
        prev = "tc-1"
    if yt_version:
        nodes.append({"id": "ytv-1", "type": "yt_version"})
        edges.append({"source": prev, "target": "ytv-1"})
        prev = "ytv-1"
    if yt_upload:
        nodes.append({"id": "ytu-1", "type": "youtube_upload"})
        edges.append({"source": prev, "target": "ytu-1"})
        prev = "ytu-1"
        if kaderblick:
            nodes.append({"id": "kb-1", "type": "kaderblick"})
            edges.append({"source": "ytu-1", "target": "kb-1"})

    job = WorkflowJob(
        graph_nodes=nodes,
        graph_edges=edges,
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
        graph_origin_node_id="src-1",
        title_card_enabled_override=title,
        repair_enabled_override=False,
        youtube_version_enabled_override=yt_version,
        youtube_upload_enabled_override=yt_upload,
        kaderblick_enabled_override=(yt_upload and kaderblick),
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
        failures = _run_stack(stack, 
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
        failures = _run_stack(stack, 
            executor,
            prepared,
            yt_service=None,
            kb_sort_index={("game-1", prepared.cv_job.source_path.name): 1},
        )

    assert failures == 0
    assert calls == []
    upload_mock.assert_not_called()
    kb_post_mock.assert_not_called()


def test_output_step_stack_processes_terminal_titlecard_and_parallel_upload_branch(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    source = tmp_path / "graph-source.mp4"
    output = tmp_path / "graph-output.mp4"
    source.write_text("src", encoding="utf-8")
    output.write_text("out", encoding="utf-8")

    job = WorkflowJob(
        title_card_enabled=True,
        upload_youtube=True,
        files=[FileEntry(source_path=str(source), graph_source_id="source-1")],
        graph_nodes=[
            {"id": "source-1", "type": "source_files"},
            {"id": "title-1", "type": "titlecard"},
            {"id": "upload-1", "type": "youtube_upload"},
        ],
        graph_edges=[
            {"source": "source-1", "target": "title-1"},
            {"source": "source-1", "target": "upload-1"},
        ],
    )
    prepared = PreparedOutput(
        orig_idx=0,
        job=job,
        cv_job=ConvertJob(
            source_path=source,
            output_path=output,
            job_type="convert",
            youtube_title="Titel",
        ),
        per_settings=AppSettings(),
        graph_origin_node_id="source-1",
        mark_finished=True,
    )
    calls: list[str] = []

    def _title_side_effect(_executor, _orig_idx, cv_job, _job, _settings):
        calls.append("titlecard")
        return cv_job.output_path, True

    with patch(
        "src.workflow_steps.title_card_step.TitleCardStep._prepend_title_card",
        side_effect=_title_side_effect,
    ), patch(
        "src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube",
        side_effect=lambda *_args, **_kwargs: calls.append("youtube_upload") or True,
    ):
        failures = _run_stack(stack, 
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    assert calls == ["titlecard", "youtube_upload"]


@pytest.mark.parametrize(
    ("node_type", "step_attr"),
    [
        ("titlecard", "_title_card_step"),
        ("validate_surface", "_surface_validation_step"),
        ("validate_deep", "_deep_validation_step"),
        ("cleanup", "_cleanup_output_step"),
        ("repair", "_repair_output_step"),
        ("yt_version", "_youtube_version_step"),
        ("stop", "_stop_output_step"),
    ],
)
def test_dead_end_processing_nodes_still_allow_parallel_upload_branch(tmp_path, node_type, step_attr):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    source = tmp_path / f"{node_type}-source.mp4"
    output = tmp_path / f"{node_type}-output.mp4"
    source.write_text("src", encoding="utf-8")
    output.write_text("out", encoding="utf-8")

    job = WorkflowJob(
        title_card_enabled=(node_type == "titlecard"),
        create_youtube_version=(node_type == "yt_version"),
        upload_youtube=True,
        files=[FileEntry(source_path=str(source), graph_source_id="source-1")],
        graph_nodes=[
            {"id": "source-1", "type": "source_files"},
            {"id": "dead-1", "type": node_type},
            {"id": "upload-1", "type": "youtube_upload"},
        ],
        graph_edges=[
            {"source": "source-1", "target": "dead-1"},
            {"source": "source-1", "target": "upload-1"},
        ],
    )
    prepared = PreparedOutput(
        orig_idx=0,
        job=job,
        cv_job=ConvertJob(
            source_path=source,
            output_path=output,
            job_type="convert",
            youtube_title="Titel",
        ),
        per_settings=AppSettings(),
        graph_origin_node_id="source-1",
        mark_finished=True,
    )
    calls: list[str] = []
    step = getattr(stack, step_attr)

    with patch.object(
        step,
        "execute",
        side_effect=lambda *_args, **_kwargs: calls.append(node_type) or 0,
    ), patch.object(
        stack._youtube_upload_step,
        "execute",
        side_effect=lambda *_args, **_kwargs: calls.append("youtube_upload") or 0,
    ):
        failures = _run_stack(stack, 
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    assert calls == [node_type, "youtube_upload"]


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
        failures = _run_stack(stack, 
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
        failures = _run_stack(stack, 
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={("game-1", prepared.cv_job.source_path.name): 1},
        )

    assert failures == 0
    assert executor.job_progress.values == [
        (0, 0, "titlecard"),
        (0, 25),
        (0, 75),
        (0, 100, "titlecard"),
        (0, 0, "yt_version"),
        (0, 40, "yt_version"),
        (0, 100, "yt_version"),
        (0, 100, "yt_version"),
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
        failures = _run_stack(stack, 
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
        (0, 0, "youtube_upload"),
        (0, 100, "youtube_upload"),
        (0, 0, "kaderblick"),
        (0, 100, "kaderblick"),
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
        failures = _run_stack(stack, 
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    assert prepared.cv_job.output_path == titlecard
    assert prepared.job.step_statuses["titlecard"] == "reused-target"


def test_output_step_stack_restart_regenerates_titlecard_output(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=True, yt_version=False, yt_upload=False, kaderblick=False)
    prepared.per_settings.video.overwrite = True
    titlecard = prepared.cv_job.output_path.with_stem(prepared.cv_job.output_path.stem + "_titlecard")
    titlecard.write_text("with-titlecard", encoding="utf-8")

    with patch(
        "src.workflow_steps.title_card_step.TitleCardStep._prepend_title_card",
        return_value=(prepared.cv_job.output_path, True),
    ) as prepend_mock:
        failures = _run_stack(stack, 
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    prepend_mock.assert_called_once()
    assert prepared.job.step_statuses["titlecard"] == "done"


def test_output_step_stack_reuses_existing_titlecard_when_base_output_is_missing(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=True, yt_version=False, yt_upload=False, kaderblick=False)
    prepared.cv_job.output_path.unlink()
    titlecard = prepared.cv_job.output_path.with_stem(prepared.cv_job.output_path.stem + "_titlecard")
    titlecard.write_text("with-titlecard", encoding="utf-8")

    failures = _run_stack(stack, 
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
        failures = _run_stack(stack, 
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    assert prepared.job.step_statuses["yt_version"] == "reused-target"


def test_output_step_stack_restart_regenerates_existing_youtube_version(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=False, yt_version=True, yt_upload=False, kaderblick=False)
    prepared.per_settings.video.overwrite = True
    yt_version = prepared.cv_job.output_path.with_stem(prepared.cv_job.output_path.stem + "_youtube")
    yt_version.write_text("yt", encoding="utf-8")
    called = []
    executor._youtube_convert_func = lambda *_args, **_kwargs: called.append("yt") or True

    failures = _run_stack(stack, 
        executor,
        prepared,
        yt_service=object(),
        kb_sort_index={},
    )

    assert failures == 0
    assert called == ["yt"]
    assert prepared.job.step_statuses["yt_version"] == "done"


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
        failures = _run_stack(stack, 
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    assert prepared.job.step_statuses["youtube_upload"] == "reused-target"


def test_output_step_stack_restart_does_not_reuse_existing_youtube_upload(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    executor._allow_reuse_existing = False
    prepared = _prepared_output(tmp_path, title=False, yt_version=False, yt_upload=True, kaderblick=False)

    with patch(
        "src.workflow_steps.youtube_upload_step.get_video_id_for_output",
        return_value="existing-123",
    ), patch(
        "src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube",
        return_value=True,
    ) as upload_mock:
        failures = _run_stack(stack, 
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    upload_mock.assert_called_once()
    assert prepared.job.step_statuses["youtube_upload"] == "done"


def test_output_step_stack_reuses_existing_youtube_upload_when_base_file_is_missing(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    prepared = _prepared_output(tmp_path, title=False, yt_version=False, yt_upload=True, kaderblick=False)
    prepared.cv_job.output_path.unlink()

    with patch(
        "src.workflow_steps.youtube_upload_step.get_video_id_for_output",
        return_value="existing-456",
    ):
        failures = _run_stack(stack, 
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
        failures = _run_stack(stack, 
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    assert prepared.job.step_statuses["youtube_upload"] == "reused-target"
    assert prepared.job.step_statuses["kaderblick"] == "reused-target"


def test_output_step_stack_restart_does_not_reuse_existing_kaderblick_entry(tmp_path):
    stack = OutputStepStack()
    executor = _FakeExecutor()
    executor._allow_reuse_existing = False
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
        return_value=True,
    ) as upload_mock, patch(
        "src.workflow_steps.kaderblick_post_step.KaderblickPostStep._post_to_kaderblick",
        return_value=True,
    ) as kb_mock:
        failures = _run_stack(stack, 
            executor,
            prepared,
            yt_service=object(),
            kb_sort_index={},
        )

    assert failures == 0
    upload_mock.assert_called_once()
    kb_mock.assert_called_once()
    assert prepared.job.step_statuses["youtube_upload"] == "done"
    assert prepared.job.step_statuses["kaderblick"] == "done"


# ---------------------------------------------------------------------------
# Unit tests for static helper methods on individual step classes
# ---------------------------------------------------------------------------

from src.workflow_steps.title_card_step import TitleCardStep
from src.workflow_steps.youtube_version_step import YoutubeVersionStep


class TestFindExistingTitlecardOutput:
    """Branch-Abdeckung für TitleCardStep._find_existing_titlecard_output."""

    def test_returns_none_when_output_path_is_none(self, tmp_path):
        """L155: output_path is None → return None."""
        job = WorkflowJob()
        cv_job = ConvertJob(
            source_path=tmp_path / "s.mp4",
            output_path=None,  # type: ignore[arg-type]
            job_type="convert",
        )
        prepared = PreparedOutput(orig_idx=0, job=job, cv_job=cv_job, per_settings=AppSettings())
        result = TitleCardStep._find_existing_titlecard_output(prepared)
        assert result is None

    def test_returns_output_path_when_step_done_and_file_exists(self, tmp_path):
        """L167: step_statuses titlecard == 'done' and output_path exists → return output_path."""
        output = tmp_path / "clip.mp4"
        output.write_text("out")
        job = WorkflowJob(step_statuses={"titlecard": "done"})
        cv_job = ConvertJob(
            source_path=tmp_path / "s.mp4",
            output_path=output,
            job_type="convert",
        )
        prepared = PreparedOutput(orig_idx=0, job=job, cv_job=cv_job, per_settings=AppSettings())
        result = TitleCardStep._find_existing_titlecard_output(prepared)
        assert result == output


class TestExistingYoutubeVersion:
    """Branch-Abdeckung für YoutubeVersionStep._existing_youtube_version."""

    def test_returns_none_when_output_path_is_none(self, tmp_path):
        """L108: output_path is None → return None."""
        job = WorkflowJob()
        cv_job = ConvertJob(
            source_path=tmp_path / "s.mp4",
            output_path=None,  # type: ignore[arg-type]
            job_type="convert",
        )
        prepared = PreparedOutput(orig_idx=0, job=job, cv_job=cv_job, per_settings=AppSettings())
        result = YoutubeVersionStep._existing_youtube_version(prepared)
        assert result is None


class TestYoutubeVersionStepEarlyReturn:
    """L19: execute() gibt 0 zurück wenn youtube_version nicht aktiviert."""

    def test_returns_zero_when_not_enabled(self, tmp_path):
        step = YoutubeVersionStep()
        executor = _FakeExecutor()
        job = WorkflowJob()
        cv_job = ConvertJob(
            source_path=tmp_path / "s.mp4",
            output_path=tmp_path / "out.mp4",
            job_type="convert",
        )
        prepared = PreparedOutput(
            orig_idx=0,
            job=job,
            cv_job=cv_job,
            per_settings=AppSettings(),
            youtube_version_enabled_override=False,
        )
        result = step.execute(executor, prepared)
        assert result == 0


class TestExecutorSupportCancelFlag:
    """Branch-Abdeckung für ExecutorSupport.cancel_flag_for_job (L54)."""

    def test_returns_threading_event_when_executor_has_no_cancel(self):
        """L54: executor ohne _cancel_flag_for_job und ohne _cancel → threading.Event()."""
        result = ExecutorSupport.cancel_flag_for_job(object(), 0)
        import threading
        assert isinstance(result, threading.Event)

    def test_returns_cancel_attr_when_executor_has_cancel_event(self):
        """L52-53: executor hat _cancel als Event → gibt dieses zurück."""
        import threading
        ev = threading.Event()
        executor = SimpleNamespace(_cancel=ev)
        result = ExecutorSupport.cancel_flag_for_job(executor, 0)
        assert result is ev


class TestExecutorSupportOutputDir:
    """Branch-Abdeckung für ExecutorSupport.resolve_processed_destination (L90)."""

    def test_processed_stage_returns_source_dir(self, tmp_path):
        """L90: Quellpfad liegt in 'processed'-Ordner → gibt source_dir zurück."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()
        source = processed_dir / "clip.mp4"
        result = ExecutorSupport.resolve_processed_destination(source)
        assert result == processed_dir


class TestExecutorSupportFilesForSource:
    """Branch-Abdeckung für ExecutorSupport.files_for_source (L139-145)."""

    def test_returns_matching_entries_by_graph_source_id(self):
        """L141: graph_source_id stimmt überein → gibt passende Einträge zurück."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            files=[
                FileEntry(source_path="/tmp/a.mp4", graph_source_id="src-1"),
                FileEntry(source_path="/tmp/b.mp4", graph_source_id="src-2"),
            ],
            graph_nodes=[{"id": "src-1", "type": "source_files"}],
            graph_edges=[],
        )
        result = ExecutorSupport.files_for_source(job, "src-1")
        assert len(result) == 1
        assert result[0].source_path == "/tmp/a.mp4"

    def test_returns_all_files_when_single_source_node_matches(self):
        """L143-144: ein einziger Source-Node → gibt alle Dateien zurück."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            files=[
                FileEntry(source_path="/tmp/a.mp4", graph_source_id=""),
                FileEntry(source_path="/tmp/b.mp4", graph_source_id=""),
            ],
            graph_nodes=[{"id": "src-1", "type": "source_files"}],
            graph_edges=[],
        )
        result = ExecutorSupport.files_for_source(job, "src-1")
        assert len(result) == 2

    def test_returns_empty_when_source_node_not_found(self):
        """L145: Source-Node-ID nicht gefunden → leere Liste."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/a.mp4", graph_source_id="src-2")],
            graph_nodes=[{"id": "src-1", "type": "source_files"}],
            graph_edges=[],
        )
        result = ExecutorSupport.files_for_source(job, "unknown-id")
        assert result == []


class TestExecutorSupportHasGraph:
    """Branch-Abdeckung für ExecutorSupport._has_graph (L135)."""

    def test_returns_false_when_no_graph_nodes(self):
        """L135: Keine Graph-Nodes → False."""
        job = WorkflowJob(name="J", source_mode="files")
        assert ExecutorSupport._has_graph(job) is False

    def test_returns_true_when_graph_nodes_with_ids(self):
        """L135: Graph-Nodes vorhanden mit IDs → True."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            graph_nodes=[{"id": "src-1", "type": "source_files"}],
        )
        assert ExecutorSupport._has_graph(job) is True


class TestExecutorSupportYoutubeMetadataOverrides:
    """Branch-Abdeckung für resolve_youtube_metadata Zeilen 290 und 292."""

    def test_entry_youtube_playlist_overrides_default(self):
        """L290: entry.youtube_playlist gesetzt → wird als playlist genutzt."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/x.mp4", youtube_playlist="MyPlaylist")],
        )
        meta = ExecutorSupport.resolve_youtube_metadata(job, "/tmp/x.mp4")
        assert meta["playlist"] == "MyPlaylist"

    def test_entry_youtube_description_overrides_default(self):
        """L292: entry.youtube_description gesetzt → wird als description genutzt."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/x.mp4", youtube_description="My Desc")],
        )
        meta = ExecutorSupport.resolve_youtube_metadata(job, "/tmp/x.mp4")
        assert meta["description"] == "My Desc"


class TestExecutorSupportSourceReachesType:
    """Branch-Abdeckung für source_reaches_type (L346) und node_matches_or_reaches_type (L362) und direct_targets (L375)."""

    def test_source_reaches_type_with_source_node_id(self):
        """L346: source_node_id gefunden → graph_node_reaches_type genutzt."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/a.mp4", graph_source_id="src-1")],
            graph_nodes=[
                {"id": "src-1", "type": "source_files"},
                {"id": "conv-1", "type": "convert"},
            ],
            graph_edges=[{"source": "src-1", "target": "conv-1"}],
        )
        assert ExecutorSupport.source_reaches_type(job, "/tmp/a.mp4", "convert") is True

    def test_node_matches_or_reaches_type_without_start_node_id(self):
        """L362: kein start_node_id → graph_reachable_types genutzt."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            graph_nodes=[
                {"id": "src-1", "type": "source_files"},
                {"id": "tc-1", "type": "titlecard"},
            ],
            graph_edges=[{"source": "src-1", "target": "tc-1"}],
        )
        assert ExecutorSupport.node_matches_or_reaches_type(job, "", "titlecard") is True

    def test_direct_targets_returns_empty_without_node_id(self):
        """L375: kein node_id → leere Liste."""
        job = WorkflowJob(name="J", source_mode="files")
        assert ExecutorSupport.direct_targets(job, "") == []


class TestExecutorSupportPreparedOutputStartNodeId:
    """Branch-Abdeckung für _prepared_output_start_node_id (L459, L462)."""

    def _make_prepared(self, job, **attrs):
        from src.workflow_steps import PreparedOutput
        from src.media.converter import ConvertJob
        cv_job = ConvertJob(
            source_path=Path("/tmp/s.mp4"),
            output_path=Path("/tmp/out.mp4"),
            job_type="convert",
        )
        p = PreparedOutput(orig_idx=0, job=job, cv_job=cv_job, per_settings=AppSettings())
        for k, v in attrs.items():
            setattr(p, k, v)
        return p

    def test_returns_cursor_node_id_when_set(self):
        """L459: graph_cursor_node_id gesetzt → wird zurückgegeben."""
        job = WorkflowJob(name="J", source_mode="files")
        prepared = self._make_prepared(job, graph_cursor_node_id="node-42")
        result = ExecutorSupport._prepared_output_start_node_id(prepared)
        assert result == "node-42"

    def test_returns_origin_node_id_when_cursor_not_set(self):
        """L462: graph_origin_node_id gesetzt, kein cursor → wird zurückgegeben."""
        job = WorkflowJob(name="J", source_mode="files")
        prepared = self._make_prepared(job, graph_origin_node_id="orig-7")
        result = ExecutorSupport._prepared_output_start_node_id(prepared)
        assert result == "orig-7"


class TestExecutorSupportPreparedOutputReachesType:
    """Branch-Abdeckung für prepared_output_reaches_type (L439, L441)."""

    def _make_prepared(self, job, **attrs):
        from src.workflow_steps import PreparedOutput
        from src.media.converter import ConvertJob
        cv_job = ConvertJob(
            source_path=Path("/tmp/s.mp4"),
            output_path=Path("/tmp/out.mp4"),
            job_type="convert",
        )
        p = PreparedOutput(orig_idx=0, job=job, cv_job=cv_job, per_settings=AppSettings())
        for k, v in attrs.items():
            setattr(p, k, v)
        return p

    def test_uses_graph_node_when_start_node_id_set(self):
        """L439: start_node_id gesetzt → graph_node_reaches_type genutzt."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            graph_nodes=[
                {"id": "src-1", "type": "source_files"},
                {"id": "conv-1", "type": "convert"},
            ],
            graph_edges=[{"source": "src-1", "target": "conv-1"}],
        )
        prepared = self._make_prepared(job, graph_cursor_node_id="src-1")
        assert ExecutorSupport.prepared_output_reaches_type(prepared, "convert") is True

    def test_uses_merge_reaches_when_origin_kind_is_merge(self):
        """L441: graph_origin_kind='merge' → graph_merge_reaches_type genutzt."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            graph_nodes=[
                {"id": "merge-1", "type": "merge"},
                {"id": "tc-1", "type": "titlecard"},
            ],
            graph_edges=[{"source": "merge-1", "target": "tc-1"}],
        )
        prepared = self._make_prepared(job, graph_origin_kind="merge")
        assert ExecutorSupport.prepared_output_reaches_type(prepared, "titlecard") is True


class TestExecutorSupportAdvancePreparedOutputCursor:
    """Branch-Abdeckung für advance_prepared_output_cursor (L446-453)."""

    def _make_prepared(self, job, **attrs):
        from src.workflow_steps import PreparedOutput
        from src.media.converter import ConvertJob
        cv_job = ConvertJob(
            source_path=Path("/tmp/s.mp4"),
            output_path=Path("/tmp/out.mp4"),
            job_type="convert",
        )
        p = PreparedOutput(orig_idx=0, job=job, cv_job=cv_job, per_settings=AppSettings())
        for k, v in attrs.items():
            setattr(p, k, v)
        return p

    def test_advances_cursor_to_next_node(self):
        """L452-453: nächste Node gefunden → graph_cursor_node_id aktualisiert."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            graph_nodes=[
                {"id": "src-1", "type": "source_files"},
                {"id": "conv-1", "type": "convert"},
            ],
            graph_edges=[{"source": "src-1", "target": "conv-1"}],
        )
        prepared = self._make_prepared(job, graph_cursor_node_id="src-1")
        # advance_prepared_output_cursor finds next node of type "convert"
        ExecutorSupport.advance_prepared_output_cursor(prepared, "convert")
        assert prepared.graph_cursor_node_id == "conv-1"


class TestExecutorSupportValidationBranchHasTargets:
    """Branch-Abdeckung für validation_branch_has_targets (L475)."""

    def _make_prepared(self, job):
        from src.workflow_steps import PreparedOutput
        from src.media.converter import ConvertJob
        cv_job = ConvertJob(
            source_path=Path("/tmp/s.mp4"),
            output_path=Path("/tmp/out.mp4"),
            job_type="convert",
        )
        return PreparedOutput(orig_idx=0, job=job, cv_job=cv_job, per_settings=AppSettings())

    def test_returns_false_when_node_not_found(self):
        """L475: node_id nicht gefunden → False."""
        job = WorkflowJob(name="J", source_mode="files")
        prepared = self._make_prepared(job)
        assert ExecutorSupport.validation_branch_has_targets(prepared, "merge", "left") is False


class TestExecutorSupportTagsFromTitle:
    """Branch-Abdeckung für resolve_youtube_metadata (L295)."""

    def test_tags_from_title_used_when_default_tags_empty(self):
        """L295: tags ist leer → _tags_from_title genutzt."""
        from unittest.mock import patch
        job = WorkflowJob(name="J", source_mode="files")
        with patch.object(
            ExecutorSupport,
            "_default_youtube_metadata",
            return_value=("Heim | Gast", "", "", []),
        ):
            meta = ExecutorSupport.resolve_youtube_metadata(job, "/tmp/x.mp4")
        assert "Fußball" in meta["tags"]


class TestExecutorSupportSourceReachesTypeBeforeMerge:
    """Branch-Abdeckung für source_reaches_type_before_merge (L386)."""

    def test_returns_false_when_no_source_node(self):
        """L386: kein source_node_id → False."""
        job = WorkflowJob(name="J", source_mode="files", files=[])
        result = ExecutorSupport.source_reaches_type_before_merge(job, "/tmp/x.mp4", "titlecard")
        assert result is False


class TestExecutorSupportBuildJobSettings:
    """Branch-Abdeckung für build_job_settings (L420)."""

    def test_overwrite_forced_when_reuse_not_allowed(self):
        """L420: allow_reuse_existing=False → overwrite wird True."""
        job = WorkflowJob(name="J", source_mode="files", overwrite=False)
        executor = SimpleNamespace(
            _settings=AppSettings(),
            _allow_reuse_existing=False,
        )
        settings = ExecutorSupport.build_job_settings(executor, job)
        assert settings.video.overwrite is True


class TestExecutorSupportAdvancePreparedOutputCursorNoStart:
    """Branch-Abdeckung für advance_prepared_output_cursor früher return (L449)."""

    def test_returns_early_when_no_start_node_id(self):
        """L449: kein start_node_id → sofortiger return, kein Fehler."""
        from src.workflow_steps import PreparedOutput
        from src.media.converter import ConvertJob
        job = WorkflowJob(name="J", source_mode="files")
        cv_job = ConvertJob(
            source_path=Path("/tmp/s.mp4"),
            output_path=Path("/tmp/out.mp4"),
            job_type="convert",
        )
        prepared = PreparedOutput(orig_idx=0, job=job, cv_job=cv_job, per_settings=AppSettings())
        # no cursor or origin node set → start_node_id is ""
        ExecutorSupport.advance_prepared_output_cursor(prepared, "convert")
        # No crash, no cursor set


class TestFindFileEntryResolvedMatch:
    """Branch-Abdeckung für find_file_entry (L209-210, L216): resolved-path-Match."""

    def test_resolved_path_match_when_exact_string_differs(self):
        """L209-210, L216: entry_resolved == target_resolved, aber exact-string unterschiedlich."""
        real_path = "/tmp/s.mp4"
        search_path = "/tmp/./s.mp4"  # identisch nach resolve, aber anderer String
        job = WorkflowJob(
            name="J",
            source_mode="files",
            files=[FileEntry(source_path=real_path)],
        )
        result = ExecutorSupport.find_file_entry(job, search_path)
        assert result is not None
        assert result.source_path == real_path
