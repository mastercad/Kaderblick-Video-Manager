from pathlib import Path

from src.integrations import kaderblick as kaderblick_module
from src.integrations import youtube as youtube_module
from src.settings import AppSettings
from src.workflow import FileEntry, WorkflowJob, describe_reset_target, describe_reset_warning, reset_job_for_rebuild
from src.workflow.reset import (
    _affected_steps,
    _clear_runtime_state,
    _delete_file,
    _job_source_paths,
    _label_for_node_type,
    _local_transfer_targets_are_disposable,
    _move_changes_source_location,
    _path_exists,
    _paths_match,
    _planned_steps,
    _step_sort_key,
    _transfer_targets,
    _transfer_targets_for_reset,
)
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
            graph_nodes=[
                {"id": "src-1", "type": "source_files"},
                {"id": "conv-1", "type": "convert"},
                {"id": "ytu-1", "type": "youtube_upload"},
                {"id": "kb-1", "type": "kaderblick"},
            ],
            graph_edges=[
                {"source": "src-1", "target": "conv-1"},
                {"source": "conv-1", "target": "ytu-1"},
                {"source": "ytu-1", "target": "kb-1"},
            ],
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
            graph_nodes=[
                {"id": "src-1", "type": "source_files"},
                {"id": "conv-1", "type": "convert"},
                {"id": "ytu-1", "type": "youtube_upload"},
                {"id": "kb-1", "type": "kaderblick"},
            ],
            graph_edges=[
                {"source": "src-1", "target": "conv-1"},
                {"source": "conv-1", "target": "ytu-1"},
                {"source": "ytu-1", "target": "kb-1"},
            ],
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
            graph_nodes=[
                {"id": "src-1", "type": "source_files"},
                {"id": "conv-1", "type": "convert"},
                {"id": "merge-1", "type": "merge"},
                {"id": "title-1", "type": "titlecard"},
            ],
            graph_edges=[
                {"source": "src-1", "target": "conv-1"},
                {"source": "conv-1", "target": "merge-1"},
                {"source": "merge-1", "target": "title-1"},
            ],
        )

        label, note = describe_reset_target(job, "titlecard")

        assert label == "Titelkarte"
        assert "Merge" in note

    def test_full_reset_warning_only_when_moved_sources_can_be_deleted(self, tmp_path):
        source_path = tmp_path / "imports" / "clip.mp4"
        target_dir = tmp_path / "raw"
        target_path = target_dir / source_path.name
        _write_file(target_path)

        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "done"},
        )

        warning = describe_reset_warning(job, settings)

        assert warning == ""

    def test_full_reset_keeps_successfully_moved_file_targets(self, tmp_path):
        source_path = tmp_path / "imports" / "clip.mp4"
        target_dir = tmp_path / "raw"
        target_path = target_dir / source_path.name
        _write_file(target_path)

        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "done", "convert": "done"},
        )

        output_path = target_dir / "clip_converted.mp4"
        _write_file(output_path)

        result = reset_job_for_rebuild(job, settings)

        assert target_path.exists()
        assert str(target_path) not in result.deleted_paths

    def test_full_reset_keeps_folder_scan_targets_after_move(self, tmp_path):
        source_dir = tmp_path / "eingang"
        target_dir = tmp_path / "raw"
        target_path = target_dir / "clip.mp4"
        _write_file(target_path)

        settings = AppSettings()
        job = _make_job(
            source_mode="folder_scan",
            files=[],
            source_folder=str(source_dir),
            file_pattern="*.mp4",
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "done"},
        )

        result = reset_job_for_rebuild(job, settings)

        assert target_path.exists()
        assert str(target_path) not in result.deleted_paths

    def test_full_reset_keeps_same_directory_file_targets(self, tmp_path):
        source_dir = tmp_path / "raw"
        source_path = source_dir / "clip.mp4"
        _write_file(source_path)

        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            move_files=False,
            copy_destination=str(source_dir),
            step_statuses={"transfer": "done"},
        )

        result = reset_job_for_rebuild(job, settings)

        assert source_path.exists()
        assert str(source_path) not in result.deleted_paths

    def test_full_reset_deletes_copied_transfer_targets(self, tmp_path):
        source_path = tmp_path / "imports" / "clip.mp4"
        target_dir = tmp_path / "raw"
        target_path = target_dir / source_path.name
        _write_file(source_path)
        _write_file(target_path)

        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            move_files=False,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "done"},
        )

        result = reset_job_for_rebuild(job, settings)

        assert not target_path.exists()
        assert str(target_path) in result.deleted_paths

    def test_full_reset_warning_stays_hidden_without_moved_source_risk(self, tmp_path):
        source_path = tmp_path / "imports" / "clip.mp4"
        target_dir = tmp_path / "raw"
        target_path = target_dir / source_path.name
        _write_file(source_path)
        _write_file(target_path)

        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            move_files=False,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "done"},
        )

        assert describe_reset_warning(job, settings) == ""

    def test_full_reset_warning_stays_hidden_when_partial_reset_requested(self, tmp_path):
        source_path = tmp_path / "imports" / "clip.mp4"
        target_dir = tmp_path / "raw"
        _write_file(target_dir / source_path.name)

        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "done"},
        )

        assert describe_reset_warning(job, settings, node_type="convert") == ""

    def test_full_reset_warning_stays_hidden_without_target_directory(self, tmp_path):
        source_path = tmp_path / "imports" / "clip.mp4"
        _write_file(source_path)

        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            move_files=True,
            copy_destination="",
            step_statuses={"transfer": "done"},
        )

        assert describe_reset_warning(job, settings) == ""

    def test_full_reset_warning_stays_hidden_when_move_keeps_same_directory(self, tmp_path):
        source_dir = tmp_path / "raw"
        source_path = source_dir / "clip.mp4"
        _write_file(source_path)

        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            move_files=True,
            copy_destination=str(source_dir),
            step_statuses={"transfer": "done"},
        )

        assert describe_reset_warning(job, settings) == ""

    def test_full_reset_warning_stays_hidden_without_existing_targets(self, tmp_path):
        source_path = tmp_path / "imports" / "clip.mp4"
        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            move_files=True,
            copy_destination=str(tmp_path / "raw"),
            step_statuses={"transfer": "done"},
        )

        assert describe_reset_warning(job, settings) == ""

    def test_full_reset_warning_detects_reused_target_state(self, tmp_path):
        source_path = tmp_path / "imports" / "clip.mp4"
        target_dir = tmp_path / "raw"
        _write_file(target_dir / source_path.name)

        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "reused-target"},
        )

        warning = describe_reset_warning(job, settings)

        assert warning == ""

    def test_full_reset_warning_stays_hidden_when_source_file_still_exists(self, tmp_path):
        source_path = tmp_path / "imports" / "clip.mp4"
        target_dir = tmp_path / "raw"
        _write_file(source_path)
        _write_file(target_dir / source_path.name)

        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path=str(source_path))],
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "pending"},
        )

        assert describe_reset_warning(job, settings) == ""

    def test_full_reset_warning_ignores_blank_file_entries(self, tmp_path):
        target_dir = tmp_path / "raw"
        _write_file(target_dir / "clip.mp4")

        settings = AppSettings()
        job = _make_job(
            files=[FileEntry(source_path="")],
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "pending"},
        )

        assert describe_reset_warning(job, settings) == ""

    def test_full_reset_warning_mentions_ordner_for_folder_scan_when_source_is_missing(self, tmp_path):
        source_dir = tmp_path / "eingang"
        target_dir = tmp_path / "raw"
        _write_file(target_dir / "clip.mp4")

        settings = AppSettings()
        job = _make_job(
            source_mode="folder_scan",
            files=[],
            source_folder=str(source_dir),
            file_pattern="*.mp4",
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "pending"},
        )

        warning = describe_reset_warning(job, settings)

        assert warning == ""

    def test_full_reset_warning_stays_hidden_for_folder_scan_when_source_exists(self, tmp_path):
        source_dir = tmp_path / "eingang"
        target_dir = tmp_path / "raw"
        source_dir.mkdir(parents=True, exist_ok=True)
        _write_file(target_dir / "clip.mp4")

        settings = AppSettings()
        job = _make_job(
            source_mode="folder_scan",
            files=[],
            source_folder=str(source_dir),
            file_pattern="*.mp4",
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "pending"},
        )

        assert describe_reset_warning(job, settings) == ""

    def test_full_reset_warning_stays_hidden_for_folder_scan_without_source_folder(self, tmp_path):
        target_dir = tmp_path / "raw"
        _write_file(target_dir / "clip.mp4")

        settings = AppSettings()
        job = _make_job(
            source_mode="folder_scan",
            files=[],
            source_folder="",
            file_pattern="*.mp4",
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "pending"},
        )

        assert describe_reset_warning(job, settings) == ""


class TestDescribeResetTargetNodeTypeVariants:
    """Branch-Abdeckung für _resolve_effective_node_type (L95, L99, L103-105)."""

    def test_source_files_node_type_returns_rebuild_note(self):
        """node_type='source_files' → L95: 'Der Branch wird ab der Quelle neu aufgebaut.'"""
        job = _make_job(
            graph_nodes=[
                {"id": "src", "type": "source_files"},
                {"id": "conv", "type": "convert"},
            ],
            graph_edges=[{"source": "src", "target": "conv"}],
        )
        label, note = describe_reset_target(job, "source_files")
        assert "ab der Quelle" in note

    def test_merge_after_convert_resolves_to_convert(self):
        """merge nach convert → L99: effektiver Typ = convert."""
        job = _make_job(
            files=[
                FileEntry(source_path="/tmp/a.mp4", merge_group_id="g1"),
                FileEntry(source_path="/tmp/b.mp4", merge_group_id="g1"),
            ],
            graph_nodes=[
                {"id": "src", "type": "source_files"},
                {"id": "conv", "type": "convert"},
                {"id": "merge", "type": "merge"},
            ],
            graph_edges=[
                {"source": "src", "target": "conv"},
                {"source": "conv", "target": "merge"},
            ],
        )
        label, note = describe_reset_target(job, "merge")
        assert "Konvertierung" in note

    def test_titlecard_after_merge_resolves_to_merge(self):
        """Titelkarte folgt Merge → L103: effektiver Typ = merge."""
        job = _make_job(
            files=[
                FileEntry(source_path="/tmp/a.mp4", merge_group_id="g1"),
                FileEntry(source_path="/tmp/b.mp4", merge_group_id="g1"),
            ],
            graph_nodes=[
                {"id": "src", "type": "source_files"},
                {"id": "merge", "type": "merge"},
                {"id": "tc", "type": "titlecard"},
            ],
            graph_edges=[
                {"source": "src", "target": "merge"},
                {"source": "merge", "target": "tc"},
            ],
        )
        label, note = describe_reset_target(job, "titlecard")
        assert "Merge" in note

    def test_titlecard_with_convert_but_no_titlecard_in_graph_resolves_to_convert(self):
        """node_type='titlecard' ohne Titelkarte im Graph (aber mit Convert) → L104: effektiver Typ = convert."""
        # graph_has_post_merge_titlecard gibt False zurück, weil kein Titelkarte-Knoten im Graph.
        # has_convert ist True → L104: 'Titelkarte setzt hier auf die Konvertierung zurück...'
        job = _make_job(
            graph_nodes=[
                {"id": "src", "type": "source_files"},
                {"id": "conv", "type": "convert"},
            ],
            graph_edges=[
                {"source": "src", "target": "conv"},
            ],
        )
        label, note = describe_reset_target(job, "titlecard")
        assert "Konvertierung" in note

    def test_titlecard_without_convert_or_graph_resolves_to_transfer(self):
        """node_type='titlecard' ohne Graph und ohne Convert → L105: effektiver Typ = transfer."""
        # Kein Graph, kein Convert → graph_has_post_merge_titlecard=False, has_convert=False → L105
        job = _make_job(graph_nodes=[], graph_edges=[])
        label, note = describe_reset_target(job, "titlecard")
        assert "Transfer" in note


class TestAffectedStepsBranchCoverage:
    """Branch-Abdeckung für _affected_steps (L124, L132, L136, L142-148)."""

    def test_source_node_type_with_graph_adds_transfer_to_affected(self):
        """source_files-Knoten im BFS → L136: 'transfer' in affected."""
        job = _make_job(
            graph_nodes=[
                {"id": "src", "type": "source_files"},
                {"id": "conv", "type": "convert"},
            ],
            graph_edges=[{"source": "src", "target": "conv"}],
        )
        affected = _affected_steps(job, "source_files")
        assert "transfer" in affected

    def test_start_node_not_in_graph_returns_all_planned(self):
        """effective_node_type nicht im Graph → L124: alle geplanten Schritte zurückgeben."""
        job = _make_job(
            graph_nodes=[
                {"id": "src", "type": "source_files"},
                {"id": "conv", "type": "convert"},
            ],
            graph_edges=[{"source": "src", "target": "conv"}],
        )
        # 'titlecard' ist nicht im Graph → _affected_steps fällt auf _planned_steps zurück
        affected = _affected_steps(job, "titlecard")
        assert "transfer" in affected
        assert "convert" in affected

    def test_bfs_cycle_does_not_cause_infinite_loop(self):
        """Zyklus im Graph → L132: bereits besuchte Knoten werden übersprungen."""
        job = _make_job(
            graph_nodes=[
                {"id": "src", "type": "source_files"},
                {"id": "conv", "type": "convert"},
            ],
            graph_edges=[
                {"source": "src", "target": "conv"},
                {"source": "conv", "target": "src"},  # künstlicher Zyklus
            ],
        )
        # Darf nicht hängen oder abstürzen
        affected = _affected_steps(job, "convert")
        assert "convert" in affected

    def test_source_node_type_without_graph_returns_all_planned(self):
        """Kein Graph, source_files als effective_node_type → L142-148: alle geplanten Schritte."""
        job = _make_job(convert_enabled=True)
        affected = _affected_steps(job, "source_files")
        assert "transfer" in affected

    def test_non_source_step_without_graph_returns_from_that_step(self):
        """Kein Graph, 'convert' als effective_node_type → ab Convert bis Ende."""
        job = WorkflowJob(
            name="Job",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/a.mp4")],
            graph_nodes=[
                {"id": "src", "type": "source_files"},
                {"id": "conv", "type": "convert"},
            ],
            graph_edges=[{"source": "src", "target": "conv"}],
        )
        affected = _affected_steps(job, "convert")
        assert "convert" in affected
        assert "transfer" not in affected

    def test_non_source_step_not_in_planned_without_graph_returns_all_planned(self):
        """L145: effective_node_type nicht in planned, kein Graph → alle Schritte zurück."""
        job = _make_job(convert_enabled=True)
        # With empty graph_nodes, planned = ["transfer"] and "yt_version" is not in it
        job.graph_nodes = []
        job.graph_edges = []
        affected = _affected_steps(job, "yt_version")
        # Returns all planned steps (just "transfer" here)
        assert "transfer" in affected


class TestClearRuntimeStateNonDict:
    """Branch-Abdeckung für _clear_runtime_state (L153, L155) mit nicht-dict Eingaben."""

    def test_non_dict_step_statuses_replaced_with_empty_dict(self):
        """step_statuses ist kein Dict → L153: durch {} ersetzen."""
        job = _make_job()
        job.step_statuses = None  # type: ignore[assignment]
        job.step_details = {}

        _clear_runtime_state(job, {"transfer"})

        assert isinstance(job.step_statuses, dict)

    def test_non_dict_step_details_replaced_with_empty_dict(self):
        """step_details ist kein Dict → L155: durch {} ersetzen."""
        job = _make_job()
        job.step_statuses = {}
        job.step_details = "ungültig"  # type: ignore[assignment]

        _clear_runtime_state(job, {"transfer"})

        assert isinstance(job.step_details, dict)


class TestLocalTransferDisposableFolderScan:
    """Branch-Abdeckung für _local_transfer_targets_are_disposable Ordner-Scan-Pfad (L291-294)."""

    def test_folder_scan_with_different_source_and_target_dir_is_disposable(self, tmp_path):
        source_dir = tmp_path / "source"
        target_dir = tmp_path / "target"
        source_dir.mkdir()
        target_dir.mkdir()

        settings = AppSettings(workflow_output_root=AppSettings.stage_root_for(str(target_dir)))
        job = _make_job(
            source_mode="folder_scan",
            files=[],
            source_folder=str(source_dir),
            move_files=False,
            copy_destination=str(target_dir),
        )

        result = _local_transfer_targets_are_disposable(job, settings)

        assert result is True

    def test_folder_scan_same_source_and_target_dir_is_not_disposable(self, tmp_path):
        shared_dir = tmp_path / "shared"
        shared_dir.mkdir()

        settings = AppSettings(workflow_output_root=AppSettings.stage_root_for(str(shared_dir)))
        job = _make_job(
            source_mode="folder_scan",
            files=[],
            source_folder=str(shared_dir),
            move_files=False,
            copy_destination=str(shared_dir),
        )

        result = _local_transfer_targets_are_disposable(job, settings)

        assert result is False

    def test_folder_scan_without_source_folder_returns_false(self, tmp_path):
        target_dir = tmp_path / "target"
        target_dir.mkdir()

        settings = AppSettings(workflow_output_root=AppSettings.stage_root_for(str(target_dir)))
        job = _make_job(
            source_mode="folder_scan",
            files=[],
            source_folder="",
            move_files=False,
            copy_destination=str(target_dir),
        )

        result = _local_transfer_targets_are_disposable(job, settings)

        assert result is False


class TestClearArtifactsMergeTitlecardRepairYtVersion:
    """Coverage für reset.py L198, L200, L202, L204: merge/titlecard/repair/yt_version-Zweige."""

    def test_full_reset_traverses_all_artifact_families(self, tmp_path):
        """L198/200/202/204: full reset mit merge/titlecard/repair/yt_version in cleared_steps."""
        settings = AppSettings(workflow_output_root=str(tmp_path / "output"))
        source = tmp_path / "s.mp4"
        source.write_text("x")

        job = _make_job(
            files=[FileEntry(source_path=str(source))],
            graph_nodes=[
                {"id": "src-1", "type": "source_files"},
                {"id": "merge-1", "type": "merge"},
                {"id": "title-1", "type": "titlecard"},
                {"id": "repair-1", "type": "repair"},
                {"id": "yt-1", "type": "yt_version"},
                {"id": "ytu-1", "type": "youtube_upload"},
            ],
            graph_edges=[
                {"source": "src-1", "target": "merge-1"},
                {"source": "merge-1", "target": "title-1"},
                {"source": "title-1", "target": "repair-1"},
                {"source": "repair-1", "target": "yt-1"},
                {"source": "yt-1", "target": "ytu-1"},
            ],
        )

        result = reset_job_for_rebuild(job, settings)

        # All four step types must be in cleared_steps
        cleared = set(result.cleared_steps)
        assert "merge" in cleared
        assert "titlecard" in cleared
        assert "repair" in cleared
        assert "yt_version" in cleared


class TestStepSortKeyAndLabelForNodeType:
    """Branch-Abdeckung für _step_sort_key (L505) und _label_for_node_type (L514-515)."""

    def test_unknown_step_returns_len_of_sequence(self):
        """Unbekannter Schritt → L505: ValueError → gibt len(_STEP_SEQUENCE) zurück."""
        result = _step_sort_key("unbekannt_xyz")
        assert isinstance(result, int)
        assert result > 0

    def test_known_step_returns_index(self):
        """Bekannter Schritt → gibt korrekten Index zurück."""
        transfer_idx = _step_sort_key("transfer")
        convert_idx = _step_sort_key("convert")
        assert transfer_idx < convert_idx

    def test_label_for_source_files(self):
        assert _label_for_node_type("source_files") == "Dateiquelle"

    def test_label_for_source_folder_scan(self):
        assert _label_for_node_type("source_folder_scan") == "Ordner-Scan"

    def test_label_for_source_pi_download(self):
        assert _label_for_node_type("source_pi_download") == "Pi-Download"

    def test_label_for_unknown_node_type_returns_type_itself(self):
        result = _label_for_node_type("unbekannt")
        assert result == "unbekannt"


class TestLocalTransferDisposableTargetDirNone:
    """Branch-Abdeckung für _local_transfer_targets_are_disposable (L277, L284)."""

    def test_folder_scan_without_copy_destination_returns_false(self):
        """L277: resolve_copy_destination → None → False."""
        job = _make_job(
            source_mode="folder_scan",
            files=[],
            source_folder="/tmp/src",
            move_files=False,
            copy_destination="",  # kein explizites Ziel → None
        )
        settings = AppSettings()  # kein workflow_output_root → resolve_copy_destination returns None
        result = _local_transfer_targets_are_disposable(job, settings)
        assert result is False

    def test_files_mode_empty_source_path_skipped(self):
        """L284: leerer source_path-Eintrag wird übersprungen → saw_source bleibt False → False."""
        job = _make_job(
            source_mode="files",
            files=[FileEntry(source_path="")],
            move_files=False,
            copy_destination="/tmp/target",
        )
        settings = AppSettings()
        result = _local_transfer_targets_are_disposable(job, settings)
        assert result is False


class TestTransferTargetsBranchCoverage:
    """Branch-Abdeckung für _transfer_targets (L415-418, L430-435)."""

    def test_unknown_source_mode_returns_empty(self):
        """L418: unbekannter source_mode → target_dir=None → leeres Set."""
        job = WorkflowJob(
            name="J",
            source_mode="unknown_mode",
            files=[FileEntry(source_path="/tmp/a.mp4")],
        )
        settings = AppSettings()
        result = _transfer_targets(job, settings)
        assert result == set()

    def test_pi_download_no_files_globs_mjpg(self, tmp_path):
        """L430-435: pi_download ohne files → glob *.mjpg."""
        download_dir = tmp_path / "dl"
        download_dir.mkdir()
        mjpg = download_dir / "clip.mjpg"
        mjpg.write_text("x")

        job = WorkflowJob(
            name="J",
            source_mode="pi_download",
            files=[],
            download_destination=str(download_dir),
        )
        settings = AppSettings()
        result = _transfer_targets(job, settings)
        assert mjpg in result

    def test_pi_download_mode_not_in_files_folder_returns_targets(self, tmp_path):
        """L265: pi_download → source_mode not in {files, folder_scan} → return targets."""
        download_dir = tmp_path / "dl"
        download_dir.mkdir()
        mjpg = download_dir / "clip.mjpg"
        mjpg.write_text("x")

        job = WorkflowJob(
            name="J",
            source_mode="pi_download",
            files=[],
            download_destination=str(download_dir),
        )
        settings = AppSettings()
        result = _transfer_targets_for_reset(job, settings)
        assert mjpg in result


class TestJobSourcePathsBranchCoverage:
    """Branch-Abdeckung für _job_source_paths (L448-449): pi_download-Pfad."""

    def test_pi_download_uses_download_destination(self, tmp_path):
        """L448-449: pi_download → runtime_target_dir = resolve_download_destination."""
        download_dir = tmp_path / "dl"
        download_dir.mkdir()

        job = WorkflowJob(
            name="J",
            source_mode="pi_download",
            files=[FileEntry(source_path="/camera/clip.mjpg")],
            download_destination=str(download_dir),
        )
        settings = AppSettings()
        result = _job_source_paths(job, settings)
        assert result == [download_dir / "clip.mjpg"]


class TestPlannedStepsBranchCoverage:
    """Branch-Abdeckung für _planned_steps (L480-482, L496)."""

    def test_merge_precedes_convert_in_graph(self):
        """L480-482: merge→convert graph → merge kommt vor convert in planned steps."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/a.mp4")],
            graph_nodes=[
                {"id": "src-1", "type": "source_files"},
                {"id": "merge-1", "type": "merge"},
                {"id": "conv-1", "type": "convert"},
            ],
            graph_edges=[
                {"source": "src-1", "target": "merge-1"},
                {"source": "merge-1", "target": "conv-1"},
            ],
        )
        steps = _planned_steps(job)
        assert "merge" in steps
        assert "convert" in steps
        assert steps.index("merge") < steps.index("convert")

    def test_stop_node_included_in_planned(self):
        """L496: stop-Knoten im Graph → 'stop' in planned steps."""
        job = WorkflowJob(
            name="J",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/a.mp4")],
            graph_nodes=[
                {"id": "src-1", "type": "source_files"},
                {"id": "stop-1", "type": "stop"},
            ],
            graph_edges=[
                {"source": "src-1", "target": "stop-1"},
            ],
        )
        steps = _planned_steps(job)
        assert "stop" in steps


class TestMoveChangesSourceLocation:
    """Branch-Abdeckung für _move_changes_source_location (L298-310)."""

    def test_files_mode_source_in_different_dir_returns_true(self, tmp_path):
        """L298-304: files-Modus, Quelldatei in anderem Verzeichnis → True."""
        target_dir = tmp_path / "target"
        job = _make_job(
            source_mode="files",
            files=[FileEntry(source_path=str(tmp_path / "source" / "clip.mp4"))],
        )
        result = _move_changes_source_location(job, target_dir)
        assert result is True

    def test_files_mode_source_in_same_dir_returns_false(self, tmp_path):
        """L305: files-Modus, Quelldatei bereits im Zielverzeichnis → False."""
        target_dir = tmp_path / "target"
        target_dir.mkdir()
        job = _make_job(
            source_mode="files",
            files=[FileEntry(source_path=str(target_dir / "clip.mp4"))],
        )
        result = _move_changes_source_location(job, target_dir)
        assert result is False

    def test_files_mode_empty_source_path_returns_false(self):
        """L301-302: leere source_path → überspringen → False."""
        job = _make_job(
            source_mode="files",
            files=[FileEntry(source_path="")],
        )
        result = _move_changes_source_location(job, Path("/tmp/target"))
        assert result is False

    def test_folder_scan_empty_source_folder_returns_false(self):
        """L308-309: folder_scan ohne source_folder → False."""
        job = _make_job(source_mode="folder_scan", files=[], source_folder="")
        result = _move_changes_source_location(job, Path("/tmp/target"))
        assert result is False

    def test_folder_scan_different_source_and_target_returns_true(self, tmp_path):
        """L310: folder_scan, Quellordner != Zielordner → True."""
        source_dir = tmp_path / "source"
        target_dir = tmp_path / "target"
        job = _make_job(
            source_mode="folder_scan",
            files=[],
            source_folder=str(source_dir),
        )
        result = _move_changes_source_location(job, target_dir)
        assert result is True


class TestPathExistsAndDeleteFile:
    """Branch-Abdeckung für _path_exists (L320-324) und _delete_file (L244-250)."""

    def test_path_exists_returns_true_for_existing_file(self, tmp_path):
        """L321-322: Datei existiert → True."""
        f = tmp_path / "x.txt"
        f.write_text("x")
        assert _path_exists(f) is True

    def test_path_exists_returns_false_for_nonexistent(self, tmp_path):
        """L322: Datei existiert nicht → False."""
        assert _path_exists(tmp_path / "nope.txt") is False

    def test_path_exists_returns_false_on_oserror(self, tmp_path):
        """L323-324: OSError beim exists-Check → False."""
        from unittest.mock import patch
        with patch("pathlib.Path.exists", side_effect=OSError("perm")):
            assert _path_exists(tmp_path / "x.txt") is False

    def test_delete_file_returns_false_on_exists_oserror(self, tmp_path):
        """L244-245: OSError beim exists-Check → False."""
        from unittest.mock import patch
        f = tmp_path / "x.txt"
        f.write_text("x")
        with patch("pathlib.Path.exists", side_effect=OSError("perm")):
            result = _delete_file(f)
        assert result is False

    def test_delete_file_returns_false_on_unlink_oserror(self, tmp_path):
        """L249-250: OSError beim unlink → False."""
        from unittest.mock import patch
        f = tmp_path / "x.txt"
        f.write_text("x")
        with patch("pathlib.Path.unlink", side_effect=OSError("busy")):
            result = _delete_file(f)
        assert result is False


class TestPathsMatchOSError:
    """Branch-Abdeckung für _paths_match (L316-317): OSError → Fallback-Vergleich."""

    def test_paths_match_oserror_falls_back_to_eq(self, tmp_path):
        """L316-317: OSError bei resolve → direkter Pfadvergleich."""
        from unittest.mock import patch
        p = tmp_path / "a.txt"
        with patch("pathlib.Path.resolve", side_effect=OSError("err")):
            result = _paths_match(p, p)
        assert result is True


class TestTransferTargetsOSError:
    """Branch-Abdeckung für _transfer_targets OSError-Handler (L426-427, L433-434)."""

    def test_folder_scan_oserror_on_glob_returns_empty(self, tmp_path):
        """L426-427: folder_scan glob → OSError → leeres Set."""
        from unittest.mock import patch
        target_dir = tmp_path / "target"
        target_dir.mkdir()
        job = _make_job(
            source_mode="folder_scan",
            files=[],
            copy_destination=str(target_dir),
        )
        settings = AppSettings()
        with patch("pathlib.Path.glob", side_effect=OSError("perm")):
            result = _transfer_targets(job, settings)
        assert result == set()

    def test_pi_download_oserror_on_glob_returns_empty(self, tmp_path):
        """L433-434: pi_download glob → OSError → leeres Set."""
        from unittest.mock import patch
        download_dir = tmp_path / "dl"
        download_dir.mkdir()
        job = WorkflowJob(
            name="J",
            source_mode="pi_download",
            files=[],
            download_destination=str(download_dir),
        )
        settings = AppSettings()
        with patch("pathlib.Path.glob", side_effect=OSError("perm")):
            result = _transfer_targets(job, settings)
        assert result == set()
