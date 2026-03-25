from pathlib import Path

from src.integrations import kaderblick as kaderblick_module
from src.integrations import youtube as youtube_module
from src.settings import AppSettings
from src.workflow import FileEntry, WorkflowJob, describe_reset_target, reset_job_for_rebuild
from src.workflow_steps.executor_support import ExecutorSupport


def _make_job(**overrides) -> WorkflowJob:
    payload = {
        "name": "Reset Workflow",
        "source_mode": "files",
        "files": [FileEntry(source_path="/tmp/source-a.mp4")],
        "convert_enabled": True,
    }
    payload.update(overrides)
    return WorkflowJob(**payload)


def _write_file(path: Path, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestWorkflowReset:
    def test_full_reset_clears_runtime_artifacts_and_delivery_state(self, tmp_path):
        settings = AppSettings(workflow_output_root=str(tmp_path / "workflow-root"))
        source_path = tmp_path / "imports" / "clip.mp4"
        _write_file(source_path)

        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            upload_youtube=True,
            upload_kaderblick=True,
            resume_status="YouTube-Upload …",
            step_statuses={
                "transfer": "done",
                "convert": "done",
                "youtube_upload": "done",
                "kaderblick": "done",
            },
            step_details={
                "convert": "ok",
                "youtube_upload": "vid-123",
            },
        )

        raw_target = Path(settings.workflow_raw_dir_for(job.name)) / source_path.name
        _write_file(raw_target)

        convert_job = ExecutorSupport.build_convert_job(None, job, str(raw_target))
        output_path = convert_job.output_path
        assert output_path is not None
        _write_file(output_path)
        youtube_variant = output_path.with_name(f"{output_path.stem}_youtube{output_path.suffix}")
        _write_file(youtube_variant)

        youtube_module._registry.record_done(youtube_variant, "vid-123", "Titel")
        kaderblick_module._get_registry().record("vid-123", 77, "game-1", "Titel")

        result = reset_job_for_rebuild(job, settings)

        assert result.effective_node_type == "transfer"
        assert not raw_target.exists()
        assert not output_path.exists()
        assert not youtube_variant.exists()
        assert youtube_module.get_video_id_for_output(output_path) is None
        assert kaderblick_module.get_recorded_kaderblick_id("vid-123") is None
        assert job.resume_status == ""
        assert job.step_statuses == {}
        assert job.step_details == {}

    def test_partial_reset_from_youtube_upload_keeps_upstream_steps(self, tmp_path):
        settings = AppSettings()
        source_path = tmp_path / "imports" / "clip.mp4"
        _write_file(source_path)

        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            upload_youtube=True,
            upload_kaderblick=True,
            step_statuses={
                "transfer": "done",
                "convert": "done",
                "youtube_upload": "done",
                "kaderblick": "done",
            },
        )

        convert_job = ExecutorSupport.build_convert_job(None, job, str(source_path))
        output_path = convert_job.output_path
        assert output_path is not None
        _write_file(output_path)
        youtube_variant = output_path.with_name(f"{output_path.stem}_youtube{output_path.suffix}")
        _write_file(youtube_variant)

        youtube_module._registry.record_done(youtube_variant, "vid-456", "Titel")
        kaderblick_module._get_registry().record("vid-456", 88, "game-2", "Titel")

        result = reset_job_for_rebuild(job, settings, node_type="youtube_upload")

        assert result.effective_node_type == "youtube_upload"
        assert job.step_statuses["transfer"] == "done"
        assert job.step_statuses["convert"] == "done"
        assert "youtube_upload" not in job.step_statuses
        assert "kaderblick" not in job.step_statuses
        assert youtube_module.get_video_id_for_output(output_path) is None
        assert kaderblick_module.get_recorded_kaderblick_id("vid-456") is None
        assert output_path.exists()

    def test_titlecard_reset_rewinds_to_merge_in_classic_merge_flow(self):
        job = _make_job(
            title_card_enabled=True,
            files=[
                FileEntry(source_path="/tmp/a.mp4", merge_group_id="g1"),
                FileEntry(source_path="/tmp/b.mp4", merge_group_id="g1"),
            ],
        )

        label, note = describe_reset_target(job, "titlecard")

        assert label == "Titelkarte"
        assert "Merge" in note