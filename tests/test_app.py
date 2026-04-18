"""Tests für Resume-Status und Last-Workflow-Verhalten in der Haupt-GUI."""

import sys
from unittest.mock import MagicMock, patch

from PySide6.QtCore import QItemSelectionModel, Qt
from PySide6.QtWidgets import QApplication, QMessageBox

_app = QApplication.instance() or QApplication(sys.argv)

from src.app import (
    ConverterApp,
    _compute_job_overall_progress,
    _format_resume_tooltip,
    _job_has_source_config,
    _normalize_cancelled_resume_state,
    _planned_job_steps,
    _repair_restored_workflow,
)
from src.app.execution import (
    _build_source_move_conflict_warning,
    _job_effectively_moves_sources,
    _job_source_paths,
)
from src.settings import AppSettings
from src.workflow.storage import save_workflow
from src.workflow import FileEntry, Workflow, WorkflowJob


class _DummySignal:
    def __init__(self):
        self._callbacks = []

    def connect(self, callback):
        self._callbacks.append(callback)


class _DummyThread:
    def __init__(self, parent=None):
        self.parent = parent
        self.started = _DummySignal()

    def isRunning(self):
        return False

    def start(self):
        return None

    def quit(self):
        return None

    def wait(self, timeout=None):
        return True


class _DummyExecutor:
    def __init__(self, workflow, settings, *, active_indices=None, allow_reuse_existing=True):
        self.workflow = workflow
        self.settings = settings
        self.active_indices = set(active_indices or set())
        self.allow_reuse_existing = allow_reuse_existing
        self.log_message = _DummySignal()
        self.job_status = _DummySignal()
        self.job_progress = _DummySignal()
        self.file_progress = _DummySignal()
        self.overall_progress = _DummySignal()
        self.phase_changed = _DummySignal()
        self.finished = _DummySignal()
        self.source_status = _DummySignal()
        self.source_progress = _DummySignal()
        self.cancel_calls = []

    def moveToThread(self, thread):
        self.thread = thread

    def run(self):
        return None

    def cancel(self, active_indices=None):
        self.cancel_calls.append(None if active_indices is None else set(active_indices))
        return None


class _DummyWorkflowDialog:
    instances = []

    def __init__(self, parent, job, allow_edit=False, settings=None, allow_wizard_shortcut=True):
        self.parent = parent
        self.job = job
        self.allow_edit = allow_edit
        self.settings = settings
        self.allow_wizard_shortcut = allow_wizard_shortcut
        self.edit_requested = False
        self.changed = False
        _DummyWorkflowDialog.instances.append(self)

    def exec(self):
        return True


def _new_app() -> ConverterApp:
    settings = AppSettings()
    settings.restore_last_workflow = False
    with patch("src.app.AppSettings.load", return_value=settings):
        window = ConverterApp()
    return window


def _rich_file_entry() -> FileEntry:
    return FileEntry(
        source_path="/tmp/halbzeit1.mp4",
        output_filename="halbzeit1_export.mp4",
        youtube_title="FC Heim - FC Gast | 1. Halbzeit",
        youtube_description="Spieltag 23",
        youtube_playlist="Saison 2025/2026",
        kaderblick_game_id="4711",
        kaderblick_game_start=120,
        kaderblick_video_type_id=7,
        kaderblick_camera_id=3,
        merge_group_id="merge-a",
        title_card_subtitle="1. Halbzeit",
        graph_source_id="source-files-1",
        title_card_before_merge=True,
    )


def _rich_job(**overrides) -> WorkflowJob:
    job = WorkflowJob(
        name="Gespeicherter Workflow",
        source_mode="files",
        files=[_rich_file_entry()],
        convert_enabled=True,
        encoder="libx264",
        crf=21,
        preset="slow",
        fps=50,
        output_format="mov",
        overwrite=True,
        merge_audio=True,
        amplify_audio=True,
        amplify_db=8.5,
        audio_sync=True,
        create_youtube_version=True,
        upload_youtube=True,
        default_youtube_title="Liga | FC Heim - FC Gast",
        default_youtube_playlist="Playlist A",
        default_youtube_competition="Pokal",
        upload_kaderblick=True,
        default_kaderblick_game_id="9001",
        default_kaderblick_video_type_id=11,
        default_kaderblick_camera_id=5,
        title_card_enabled=True,
        title_card_logo_path="/tmp/logo.png",
        title_card_duration=4.0,
        title_card_bg_color="#112233",
        title_card_fg_color="#F8FAFC",
        title_card_home_team="FC Heim",
        title_card_away_team="FC Gast",
        title_card_date="2026-03-22",
        graph_nodes=[
            {"id": "source-files-1", "type": "source_files", "x": 80.0, "y": 100.0},
            {"id": "convert-1", "type": "convert", "x": 360.0, "y": 100.0},
            {"id": "merge-1", "type": "merge", "x": 360.0, "y": 220.0},
            {"id": "title-1", "type": "titlecard", "x": 360.0, "y": 340.0},
            {"id": "yt-1", "type": "yt_version", "x": 360.0, "y": 460.0},
            {"id": "upload-1", "type": "youtube_upload", "x": 680.0, "y": 220.0},
            {"id": "kb-1", "type": "kaderblick", "x": 680.0, "y": 340.0},
        ],
        graph_edges=[
            {"source": "source-files-1", "target": "convert-1"},
            {"source": "convert-1", "target": "merge-1"},
            {"source": "merge-1", "target": "title-1"},
            {"source": "title-1", "target": "yt-1"},
            {"source": "yt-1", "target": "upload-1"},
            {"source": "upload-1", "target": "kb-1"},
        ],
        resume_status="YT-Version erstellen …",
        step_statuses={
            "transfer": "done",
            "convert": "done",
            "merge": "done",
            "titlecard": "done",
            "yt_version": "running",
        },
        progress_pct=44,
        overall_progress_pct=78,
        current_step_key="yt_version",
    )
    for key, value in overrides.items():
        setattr(job, key, value)
    return job


def _roundtrip_restored_workflow(tmp_path, workflow: Workflow) -> ConverterApp:
    last_workflow_file = tmp_path / "last_workflow.json"
    settings = AppSettings()
    settings.restore_last_workflow = False

    with patch("src.app.AppSettings.load", return_value=settings), patch("src.workflow.storage.LAST_WORKFLOW_FILE", last_workflow_file):
        first = ConverterApp()
        try:
            first._workflow = workflow
            first._save_last_workflow()
        finally:
            first.close()

    restart_settings = AppSettings()
    restart_settings.restore_last_workflow = True
    with patch("src.app.AppSettings.load", return_value=restart_settings), patch("src.workflow.storage.LAST_WORKFLOW_FILE", last_workflow_file):
        return ConverterApp()


class TestResumeTooltip:
    def test_format_resume_tooltip_lists_known_steps_in_order(self):
        job = WorkflowJob(
            resume_status="Transfer OK",
            step_statuses={
                "youtube_upload": "done",
                "transfer": "done",
                "convert": "running",
            },
        )

        tooltip = _format_resume_tooltip(job)

        assert tooltip == (
            "Letzter Status: Transfer OK\n"
            "Transfer: done\n"
            "Konvertierung: running\n"
            "YouTube-Upload: done"
        )

    def test_format_resume_tooltip_uses_resume_status_without_steps(self):
        job = WorkflowJob(resume_status="Kaderblick …")
        assert _format_resume_tooltip(job) == "Kaderblick …"

    def test_format_resume_tooltip_includes_step_details(self):
        job = WorkflowJob(
            resume_status="Zusammenführen OK",
            step_statuses={"merge": "done"},
            step_details={"merge": "Quellen: a.mp4, b.mp4 | Datei: merged.mp4 | Dauer: 12:34 | Größe: 1.20 GB"},
        )

        tooltip = _format_resume_tooltip(job)

        assert tooltip == (
            "Letzter Status: Zusammenführen OK\n"
            "Zusammenführen: done\n"
            "  Quellen: a.mp4, b.mp4 | Datei: merged.mp4 | Dauer: 12:34 | Größe: 1.20 GB"
        )

    def test_format_resume_tooltip_includes_elapsed_runtime_when_present(self):
        job = WorkflowJob(resume_status="Kaderblick …", run_elapsed_seconds=125)

        assert _format_resume_tooltip(job) == "Kaderblick …\nLaufzeit: 2min 05s"


class TestJobOverallProgress:
    def test_job_has_source_config_requires_real_source_definition(self):
        assert _job_has_source_config(WorkflowJob(source_mode="files", files=[])) is False
        assert _job_has_source_config(WorkflowJob(source_mode="folder_scan", source_folder="")) is False
        assert _job_has_source_config(WorkflowJob(source_mode="pi_download", device_name="")) is False
        assert _job_has_source_config(
            WorkflowJob(source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")])
        ) is True

    def test_planned_steps_include_current_pipeline(self):
        job = WorkflowJob(
            convert_enabled=True,
            title_card_enabled=True,
            create_youtube_version=True,
            upload_youtube=True,
            upload_kaderblick=True,
        )
        job.files = []

        assert _planned_job_steps(job) == [
            "transfer",
            "convert",
            "titlecard",
            "yt_version",
            "youtube_upload",
            "kaderblick",
        ]

    def test_planned_steps_skip_unreachable_output_steps_without_processing(self):
        job = WorkflowJob(
            convert_enabled=False,
            title_card_enabled=True,
            create_youtube_version=True,
            upload_youtube=False,
        )

        assert _planned_job_steps(job) == ["transfer"]

    def test_compute_job_overall_progress_uses_current_step_progress(self):
        job = WorkflowJob(
            convert_enabled=True,
            upload_youtube=True,
        )
        job.step_statuses = {"transfer": "done", "convert": "running"}
        job.current_step_key = "convert"

        assert _compute_job_overall_progress(job, "Konvertiere …", 50) == 50

    def test_compute_job_overall_progress_reaches_done_state(self):
        job = WorkflowJob(convert_enabled=False, upload_youtube=True)
        job.step_statuses = {"transfer": "done", "youtube_upload": "done"}
        job.current_step_key = "youtube_upload"

        assert _compute_job_overall_progress(job, "Fertig", 100) == 100

    def test_compute_job_overall_progress_keeps_transfer_progress_when_graph_merge_starts(self):
        job = WorkflowJob(
            convert_enabled=False,
            graph_nodes=[
                {"id": "source-1", "type": "source_files"},
                {"id": "merge-1", "type": "merge"},
                {"id": "yt-1", "type": "youtube_upload"},
            ],
            graph_edges=[
                {"source": "source-1", "target": "merge-1"},
                {"source": "merge-1", "target": "yt-1"},
            ],
            upload_youtube=True,
        )
        job.step_statuses = {"transfer": "done", "merge": "running"}
        job.current_step_key = "merge"

        assert _planned_job_steps(job) == ["transfer", "merge", "youtube_upload"]
        assert _compute_job_overall_progress(job, "Zusammenführen …", 0) == 33


class TestSelectedWorkflowStart:
    def test_job_source_paths_collects_folder_scan_matches(self, tmp_path):
        source_dir = tmp_path / "input"
        source_dir.mkdir()
        first = source_dir / "a.mp4"
        second = source_dir / "b.mp4"
        ignored = source_dir / "c.mov"
        first.write_text("a")
        second.write_text("b")
        ignored.write_text("c")

        job = WorkflowJob(
            source_mode="folder_scan",
            source_folder=str(source_dir),
            file_pattern="*.mp4",
            move_files=True,
            copy_destination=str(tmp_path / "target"),
        )

        assert _job_source_paths(job) == {first, second}

    def test_job_source_paths_ignores_non_file_graph_entries_for_multi_source_jobs(self, tmp_path):
        local_file = tmp_path / "lokal.mp4"
        download_file = tmp_path / "download.mp4"
        local_file.write_text("a")
        download_file.write_text("b")

        job = WorkflowJob(
            source_mode="files",
            files=[
                FileEntry(source_path=str(local_file), graph_source_id="source-files"),
                FileEntry(source_path=str(download_file), graph_source_id="source-pi"),
            ],
            graph_nodes=[
                {"id": "source-files", "type": "source_files"},
                {"id": "source-pi", "type": "source_pi_download"},
            ],
        )

        assert _job_source_paths(job) == {local_file}

    def test_job_effectively_moves_sources_is_false_for_in_place_target(self, tmp_path):
        source_dir = tmp_path / "input"
        source_dir.mkdir()
        source_file = source_dir / "halbzeit.mp4"
        source_file.write_text("video")
        job = WorkflowJob(
            source_mode="files",
            files=[FileEntry(source_path=str(source_file))],
            move_files=True,
            copy_destination=str(source_dir),
        )

        assert _job_effectively_moves_sources(AppSettings(), job) is False

    def test_conflict_warning_requires_move_before_later_access(self, tmp_path):
        source_file = tmp_path / "halbzeit.mp4"
        source_file.write_text("video")
        move_target = tmp_path / "ziel"
        earlier_reader = WorkflowJob(
            name="Leser",
            source_mode="files",
            files=[FileEntry(source_path=str(source_file))],
            move_files=False,
        )
        later_mover = WorkflowJob(
            name="Mover",
            source_mode="files",
            files=[FileEntry(source_path=str(source_file))],
            move_files=True,
            copy_destination=str(move_target),
        )

        warning = _build_source_move_conflict_warning(
            AppSettings(),
            [(0, earlier_reader), (1, later_mover)],
        )

        assert warning == ""

    def test_conflict_warning_limits_output_and_reports_remaining_count(self, tmp_path):
        settings = AppSettings()
        job_entries = []
        for index in range(9):
            source_file = tmp_path / f"datei-{index}.mp4"
            source_file.write_text("video")
            mover = WorkflowJob(
                name=f"Mover {index}",
                source_mode="files",
                files=[FileEntry(source_path=str(source_file))],
                move_files=True,
                copy_destination=str(tmp_path / f"ziel-{index}"),
            )
            reader = WorkflowJob(
                name=f"Reader {index}",
                source_mode="files",
                files=[FileEntry(source_path=str(source_file))],
                move_files=False,
            )
            job_entries.extend([(index * 2, mover), (index * 2 + 1, reader)])

        warning = _build_source_move_conflict_warning(settings, job_entries)

        assert warning.count("greift spaeter erneut darauf zu") == 8
        assert "1 weitere Konflikt(e)" in warning

    def test_start_selected_workflows_passes_selected_indices_to_executor(self):
        window = _new_app()
        window._workflow.jobs = [
            WorkflowJob(name="A", source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")]),
            WorkflowJob(name="B", source_mode="files", files=[FileEntry(source_path="/tmp/b.mp4")]),
            WorkflowJob(name="C", source_mode="files", files=[FileEntry(source_path="/tmp/c.mp4")]),
        ]
        window._refresh_table()
        selection_model = window.table.selectionModel()
        selection_model.select(window.table.model().index(0, 0), QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows)
        selection_model.select(window.table.model().index(2, 0), QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows)

        with patch("src.app.QThread", _DummyThread), patch("src.app.WorkflowExecutor", _DummyExecutor):
            window._start_selected_workflows()

        assert window._wf_executor is not None
        assert window._wf_executor.active_indices == {0, 2}

    def test_start_selected_workflows_starts_all_enabled_when_nothing_selected(self):
        window = _new_app()
        window._workflow.jobs = [
            WorkflowJob(name="A", source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")]),
            WorkflowJob(name="B", source_mode="files", files=[FileEntry(source_path="/tmp/b.mp4")]),
        ]
        window._refresh_table()
        window.table.clearSelection()

        with patch("src.app.QThread", _DummyThread), patch("src.app.WorkflowExecutor", _DummyExecutor):
            window._start_selected_workflows()

        assert window._wf_executor is not None
        assert window._wf_executor.active_indices == {0, 1}

    def test_start_selected_workflows_does_not_prompt_when_nothing_selected(self):
        window = _new_app()
        window._workflow.jobs = [
            WorkflowJob(name="A", source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")]),
            WorkflowJob(name="B", source_mode="files", files=[FileEntry(source_path="/tmp/b.mp4")]),
        ]
        window._refresh_table()
        window.table.clearSelection()

        with patch("src.app.QThread", _DummyThread), patch("src.app.WorkflowExecutor", _DummyExecutor), patch(
            "src.app.QMessageBox.question"
        ) as question_mock:
            window._start_selected_workflows()

        question_mock.assert_not_called()
        assert window._wf_executor is not None
        assert window._wf_executor.active_indices == {0, 1}

    def test_start_workflow_warns_when_two_active_jobs_move_same_source(self, tmp_path):
        window = _new_app()
        source_file = tmp_path / "halbzeit1.mp4"
        source_file.write_text("video")
        target_a = tmp_path / "ziel-a"
        target_b = tmp_path / "ziel-b"
        window._workflow.jobs = [
            WorkflowJob(
                name="A",
                source_mode="files",
                files=[FileEntry(source_path=str(source_file))],
                copy_destination=str(target_a),
                move_files=True,
            ),
            WorkflowJob(
                name="B",
                source_mode="files",
                files=[FileEntry(source_path=str(source_file))],
                copy_destination=str(target_b),
                move_files=True,
            ),
        ]
        window._refresh_table()

        with patch("src.app.QMessageBox.warning") as warning_mock, patch("src.app.QThread", _DummyThread), patch(
            "src.app.WorkflowExecutor", _DummyExecutor
        ):
            window._start_workflow(active_indices={0, 1})

        warning_mock.assert_called_once()
        assert "wollen dieselbe Quelldatei verschieben" in warning_mock.call_args.args[2]
        assert window._wf_executor is None

    def test_start_workflow_warns_when_earlier_job_moves_source_before_later_reuse(self, tmp_path):
        window = _new_app()
        source_file = tmp_path / "halbzeit2.mp4"
        source_file.write_text("video")
        target_dir = tmp_path / "ziel"
        window._workflow.jobs = [
            WorkflowJob(
                name="Frueher",
                source_mode="files",
                files=[FileEntry(source_path=str(source_file))],
                copy_destination=str(target_dir),
                move_files=True,
            ),
            WorkflowJob(
                name="Spaeter",
                source_mode="files",
                files=[FileEntry(source_path=str(source_file))],
                move_files=False,
            ),
        ]
        window._refresh_table()

        with patch("src.app.QMessageBox.warning") as warning_mock, patch("src.app.QThread", _DummyThread), patch(
            "src.app.WorkflowExecutor", _DummyExecutor
        ):
            window._start_workflow(active_indices={0, 1})

        warning_mock.assert_called_once()
        assert "verschiebt die Quelle" in warning_mock.call_args.args[2]
        assert "greift spaeter erneut darauf zu" in warning_mock.call_args.args[2]
        assert window._wf_executor is None

    def test_start_workflow_allows_shared_source_when_nobody_moves_it(self, tmp_path):
        window = _new_app()
        source_file = tmp_path / "halbzeit3.mp4"
        source_file.write_text("video")
        window._workflow.jobs = [
            WorkflowJob(
                name="A",
                source_mode="files",
                files=[FileEntry(source_path=str(source_file))],
                move_files=False,
            ),
            WorkflowJob(
                name="B",
                source_mode="files",
                files=[FileEntry(source_path=str(source_file))],
                move_files=False,
            ),
        ]
        window._refresh_table()

        with patch("src.app.QMessageBox.warning") as warning_mock, patch("src.app.QThread", _DummyThread), patch(
            "src.app.WorkflowExecutor", _DummyExecutor
        ):
            window._start_workflow(active_indices={0, 1})

        warning_mock.assert_not_called()
        assert window._wf_executor is not None
        assert window._wf_executor.active_indices == {0, 1}

    def test_main_window_has_no_separate_start_all_action(self):
        window = _new_app()

        assert hasattr(window, "act_start")
        assert not hasattr(window, "act_start_all")

    def test_start_workflow_disables_reuse_when_restart_is_selected(self):
        window = _new_app()
        window._workflow.jobs = [
            WorkflowJob(
                name="A",
                source_mode="files",
                files=[FileEntry(source_path="/tmp/a.mp4")],
                resume_status="Transfer OK",
                step_statuses={"transfer": "done"},
            )
        ]
        window._refresh_table()

        with patch("src.app.QThread", _DummyThread), patch("src.app.WorkflowExecutor", _DummyExecutor), patch.object(
            window,
            "_ask_resume_behavior",
            return_value=QMessageBox.StandardButton.No,
        ):
            window._start_selected_workflows()

        assert window._wf_executor is not None
        assert window._wf_executor.allow_reuse_existing is False


class TestSelectedWorkflowCancel:
    def test_cancel_workflow_aborts_only_selected_jobs_after_confirmation(self):
        window = _new_app()
        try:
            window._workflow.jobs = [
                WorkflowJob(name="A", source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")]),
                WorkflowJob(name="B", source_mode="files", files=[FileEntry(source_path="/tmp/b.mp4")]),
                WorkflowJob(name="C", source_mode="files", files=[FileEntry(source_path="/tmp/c.mp4")]),
            ]
            window._refresh_table()
            selection_model = window.table.selectionModel()
            selection_model.select(window.table.model().index(1, 0), QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows)

            with patch("src.app.QThread", _DummyThread), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow(active_indices={0, 1, 2})

            assert window._wf_executor is not None

            with patch("src.app.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes) as question_mock:
                window._cancel_workflow()

            assert window._wf_executor.cancel_calls == [{1}]
            question_mock.assert_called_once()
            assert "ausgewählte" in question_mock.call_args.args[2]
        finally:
            window.close()

    def test_cancel_workflow_aborts_all_jobs_when_nothing_is_selected(self):
        window = _new_app()
        try:
            window._workflow.jobs = [
                WorkflowJob(name="A", source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")]),
                WorkflowJob(name="B", source_mode="files", files=[FileEntry(source_path="/tmp/b.mp4")]),
            ]
            window._refresh_table()
            window.table.clearSelection()

            with patch("src.app.QThread", _DummyThread), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow(active_indices={0, 1})

            assert window._wf_executor is not None

            with patch("src.app.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes) as question_mock:
                window._cancel_workflow()

            assert window._wf_executor.cancel_calls == [{0, 1}]
            question_mock.assert_called_once()
            assert "alle laufenden Jobs" in question_mock.call_args.args[2]
        finally:
            window.close()

    def test_cancel_workflow_stops_when_confirmation_is_declined(self):
        window = _new_app()
        try:
            window._workflow.jobs = [
                WorkflowJob(name="A", source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")]),
            ]
            window._refresh_table()

            with patch("src.app.QThread", _DummyThread), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow(active_indices={0})

            assert window._wf_executor is not None

            with patch("src.app.QMessageBox.question", return_value=QMessageBox.StandardButton.No):
                window._cancel_workflow()

            assert window._wf_executor.cancel_calls == []
        finally:
            window.close()


class TestSessionRepair:
    def test_repair_restored_placeholder_workflow_does_not_restore_last_workflow_without_resume_state(self):
        restored = Workflow(job=WorkflowJob(name="Job 1", source_mode="files", files=[]))
        fallback = Workflow(
            name="Spieltag 23",
            job=WorkflowJob(
                name="Gespeicherter Job",
                source_mode="files",
                files=[FileEntry(source_path="/tmp/halbzeit1.mp4")],
                upload_youtube=True,
            ),
            shutdown_after=True,
        )

        repaired, repaired_count, dropped = _repair_restored_workflow(restored, fallback)

        assert repaired_count == 0
        assert dropped == 0
        assert repaired.name == ""
        assert repaired.shutdown_after is False
        assert repaired.job is not None
        assert repaired.job.name == "Job 1"
        assert repaired.job.files == []

    def test_repair_restored_workflow_reuses_last_workflow_config(self):
        restored = Workflow(jobs=[
            WorkflowJob(
                name="Job 1",
                source_mode="files",
                files=[],
                resume_status="Transfer OK",
                step_statuses={"transfer": "done", "convert": "running"},
            )
        ])
        fallback = Workflow(jobs=[
            WorkflowJob(
                name="Job 1",
                source_mode="files",
                files=[FileEntry(source_path="/tmp/halbzeit1.mp4")],
                upload_youtube=True,
            )
        ])

        repaired, repaired_count, dropped = _repair_restored_workflow(restored, fallback)

        assert repaired_count == 1
        assert dropped == 0
        assert repaired.jobs[0].files[0].source_path == "/tmp/halbzeit1.mp4"
        assert repaired.jobs[0].resume_status == "Transfer OK"
        assert repaired.jobs[0].step_statuses == {"transfer": "done", "convert": "running"}

    def test_repair_restored_workflow_drops_resume_for_unrepairable_job(self):
        restored = Workflow(jobs=[
            WorkflowJob(
                name="Job 1",
                source_mode="files",
                files=[],
                resume_status="Transfer OK",
                step_statuses={"transfer": "done"},
            )
        ])

        repaired, repaired_count, dropped = _repair_restored_workflow(restored, None)

        assert repaired_count == 0
        assert dropped == 1
        assert repaired.jobs[0].resume_status == ""
        assert repaired.jobs[0].step_statuses == {}

    def test_repair_restored_workflow_keeps_placeholder_only_session_without_resume_state(self):
        restored = Workflow(jobs=[WorkflowJob(name="Job 1", source_mode="files", files=[])])
        fallback = Workflow(
            name="Spieltag (2)",
            jobs=[
                WorkflowJob(name="Spieltag", source_mode="files", files=[]),
                WorkflowJob(name="Spieltag (2)", source_mode="files", files=[FileEntry(source_path="/tmp/c.mp4")]),
            ],
        )

        repaired, repaired_count, dropped = _repair_restored_workflow(restored, fallback)

        assert repaired_count == 0
        assert dropped == 0
        assert repaired.name == ""
        assert len(repaired.jobs) == 1
        assert repaired.jobs[0].name == "Job 1"
        assert repaired.jobs[0].files == []


class TestConverterAppResumeState:
    def test_normalize_cancelled_resume_state_turns_aborted_job_into_resumable_step(self):
        job = WorkflowJob(
            name="Job 1",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/a.mp4")],
            resume_status="Konvertierung abgebrochen",
            current_step_key="convert",
            step_statuses={"transfer": "done", "convert": "cancelled"},
            step_details={"convert": "Durch Benutzer abgebrochen"},
        )

        changed = _normalize_cancelled_resume_state(job)

        assert changed is True
        assert job.resume_status == "Konvertiere …"
        assert job.current_step_key == "convert"
        assert job.step_statuses == {"transfer": "done"}
        assert job.step_details == {}

    def test_save_last_workflow_persists_complete_workflow_configuration(self, tmp_path):
        last_workflow_file = tmp_path / "last_workflow.json"
        workflow = Workflow(name="Spieltag 23", job=_rich_job(), shutdown_after=True)

        window = _new_app()
        try:
            window._workflow = workflow
            with patch("src.workflow.storage.LAST_WORKFLOW_FILE", last_workflow_file):
                window._save_last_workflow()

            assert last_workflow_file.exists() is True

            with patch("src.workflow.storage.LAST_WORKFLOW_FILE", last_workflow_file):
                restored = Workflow.load_last()

            assert restored is not None
            assert restored.to_dict() == workflow.to_dict()
            assert restored.job is not None
            assert restored.job.to_dict() == workflow.job.to_dict()
            assert restored.job.resume_status == "YT-Version erstellen …"
            assert restored.job.step_statuses == {
                "transfer": "done",
                "convert": "done",
                "merge": "done",
                "titlecard": "done",
                "yt_version": "running",
            }
            assert restored.job.graph_nodes == workflow.job.graph_nodes
            assert restored.job.graph_edges == workflow.job.graph_edges
        finally:
            window.close()

    def test_save_last_workflow_persists_resume_state_but_not_transient_error_fields(self, tmp_path):
        last_workflow_file = tmp_path / "last_workflow.json"
        runtime_job = _rich_job()
        runtime_job.status = "Läuft"
        runtime_job.transfer_status = "Transfer 1/3"
        runtime_job.transfer_progress_pct = 66
        runtime_job.progress_pct = 44
        runtime_job.overall_progress_pct = 78
        runtime_job.current_step_key = "yt_version"
        runtime_job.error_msg = ""

        window = _new_app()
        try:
            window._workflow = Workflow(name="Spieltag 23", job=runtime_job, shutdown_after=True)
            with patch("src.workflow.storage.LAST_WORKFLOW_FILE", last_workflow_file):
                window._save_last_workflow()

            payload = last_workflow_file.read_text(encoding="utf-8")

            assert '"resume_status": "YT-Version erstellen …"' in payload
            assert '"step_statuses"' in payload
            assert '"progress_pct": 44' in payload
            assert '"overall_progress_pct": 78' in payload
            assert '"current_step_key": "yt_version"' in payload
            assert '"transfer_status": "Transfer 1/3"' in payload
            assert '"transfer_progress_pct": 66' in payload
            assert '"graph_nodes"' in payload
            assert '"graph_edges"' in payload
            assert '"status"' not in payload
            assert '"error_msg"' not in payload
        finally:
            window.close()

    def test_app_restart_restores_full_last_workflow_configuration(self, tmp_path):
        expected = Workflow(name="Spieltag 23", job=_rich_job(), shutdown_after=True)

        window = _roundtrip_restored_workflow(tmp_path, expected)

        try:
            assert window.table.rowCount() == 1
            assert window._workflow.to_dict() == expected.to_dict()
            assert window._workflow.job is not None
            assert window._workflow.job.to_dict() == expected.job.to_dict()
            assert window.table.item(0, 1).text() == "Gespeicherter Workflow"
            assert window.table.item(0, 4).text() == "YT-Version erstellen …"
            assert window.table.item(0, 5).text() == "78%"
        finally:
            window.close()

    def test_app_start_drops_resume_state_from_unrepairable_last_workflow(self, tmp_path):
        settings = AppSettings()
        settings.restore_last_workflow = True
        last_workflow_file = tmp_path / "last_workflow.json"
        broken = Workflow(
            name="Spieltag 23",
            job=WorkflowJob(
                name="Gespeicherter Job",
                source_mode="files",
                files=[],
                resume_status="YT-Version erstellen …",
                step_statuses={"yt_version": "running"},
            ),
        )
        broken.save(last_workflow_file)

        with patch("src.app.AppSettings.load", return_value=settings), \
             patch("src.workflow.storage.LAST_WORKFLOW_FILE", last_workflow_file):
            window = ConverterApp()

        try:
            assert window.table.rowCount() == 1
            assert window._workflow.job is not None
            assert window._workflow.job.resume_status == ""
            assert window._workflow.job.step_statuses == {}
            assert window._workflow.job.files == []
        finally:
            window.close()

    def test_app_start_normalizes_cancelled_last_workflow_state(self, tmp_path):
        settings = AppSettings()
        settings.restore_last_workflow = True
        last_workflow_file = tmp_path / "last_workflow.json"
        restored = Workflow(
            jobs=[
                WorkflowJob(
                    name="Gespeicherter Job",
                    source_mode="files",
                    files=[FileEntry(source_path="/tmp/a.mp4")],
                    resume_status="Konvertierung abgebrochen",
                    current_step_key="convert",
                    step_statuses={"transfer": "done", "convert": "cancelled"},
                    step_details={"convert": "Durch Benutzer abgebrochen"},
                )
            ]
        )
        save_workflow(restored, last_workflow_file, include_runtime=True)

        with patch("src.app.AppSettings.load", return_value=settings), \
             patch("src.workflow.storage.LAST_WORKFLOW_FILE", last_workflow_file):
            window = ConverterApp()

        try:
            assert window.table.rowCount() == 1
            assert window._workflow.job is not None
            assert window._workflow.job.resume_status == "Konvertiere …"
            assert window._workflow.job.step_statuses == {"transfer": "done"}
            assert window._workflow.job.step_details == {}
            assert window.table.item(0, 4).text() == "Konvertiere …"
        finally:
            window.close()

    def test_app_start_normalizes_cancelled_state_for_all_restored_jobs(self, tmp_path):
        settings = AppSettings()
        settings.restore_last_workflow = True
        last_workflow_file = tmp_path / "last_workflow.json"
        restored = Workflow(
            jobs=[
                WorkflowJob(
                    name="Gespeicherter Job A",
                    source_mode="files",
                    files=[FileEntry(source_path="/tmp/a.mp4")],
                    resume_status="Konvertierung abgebrochen",
                    current_step_key="convert",
                    step_statuses={"transfer": "done", "convert": "cancelled"},
                    step_details={"convert": "Durch Benutzer abgebrochen"},
                ),
                WorkflowJob(
                    name="Gespeicherter Job B",
                    source_mode="files",
                    files=[FileEntry(source_path="/tmp/b.mp4")],
                    create_youtube_version=True,
                    upload_youtube=True,
                    resume_status="YouTube-Upload abgebrochen",
                    current_step_key="youtube_upload",
                    step_statuses={
                        "transfer": "done",
                        "convert": "done",
                        "yt_version": "done",
                        "youtube_upload": "cancelled",
                    },
                    step_details={"youtube_upload": "Durch Benutzer abgebrochen"},
                ),
            ]
        )
        save_workflow(restored, last_workflow_file, include_runtime=True)

        with patch("src.app.AppSettings.load", return_value=settings), \
             patch("src.workflow.storage.LAST_WORKFLOW_FILE", last_workflow_file):
            window = ConverterApp()

        try:
            assert window.table.rowCount() == 2
            assert window._workflow.jobs[0].resume_status == "Konvertiere …"
            assert window._workflow.jobs[0].step_statuses == {"transfer": "done"}
            assert window._workflow.jobs[1].resume_status == "YouTube-Upload …"
            assert window._workflow.jobs[1].step_statuses == {
                "transfer": "done",
                "convert": "done",
                "yt_version": "done",
            }
            assert window.table.item(0, 4).text() == "Konvertiere …"
            assert window.table.item(1, 4).text() == "YouTube-Upload …"
        finally:
            window.close()

    def test_app_start_restores_last_workflow_when_enabled(self, tmp_path):
        settings = AppSettings()
        settings.restore_last_workflow = True
        restored = Workflow(jobs=[
            WorkflowJob(
                name="Gespeicherter Job",
                source_mode="files",
                files=[FileEntry(source_path="/tmp/a.mp4")],
            )
        ])
        last_workflow_file = tmp_path / "last_workflow.json"
        restored.save(last_workflow_file)

        with patch("src.app.AppSettings.load", return_value=settings), \
             patch("src.workflow.storage.LAST_WORKFLOW_FILE", last_workflow_file):
            window = ConverterApp()

        try:
            assert window.table.rowCount() == 1
            assert window.table.item(0, 1).text() == "Gespeicherter Job"
        finally:
            window.close()

    def test_app_start_keeps_placeholder_last_workflow_without_inventing_data(self, tmp_path):
        settings = AppSettings()
        settings.restore_last_workflow = True
        last_workflow_file = tmp_path / "last_workflow.json"
        placeholder = Workflow(job=WorkflowJob(name="Job 1", source_mode="files", files=[]))
        placeholder.save(last_workflow_file)

        with patch("src.app.AppSettings.load", return_value=settings), \
             patch("src.workflow.storage.LAST_WORKFLOW_FILE", last_workflow_file):
            window = ConverterApp()

        try:
            assert window.table.rowCount() == 1
            assert window._workflow.name == ""
            assert window._workflow.job is not None
            assert window._workflow.job.files == []
            assert window.table.item(0, 1).text() == "Job 1"
        finally:
            window.close()

    def test_new_workflow_uses_saved_app_settings_defaults(self):
        settings = AppSettings()
        settings.video.encoder = "hevc_nvenc"
        settings.video.crf = 21
        settings.video.preset = "slow"
        settings.video.fps = 50
        settings.video.output_format = "mov"
        settings.audio.amplify_audio = True
        settings.audio.amplify_db = 9.5

        with patch("src.app.AppSettings.load", return_value=settings):
            window = ConverterApp()

        try:
            _DummyWorkflowDialog.instances.clear()

            def _make_dialog(parent, job, allow_edit=False, settings=None, allow_wizard_shortcut=True):
                dialog = _DummyWorkflowDialog(
                    parent,
                    job,
                    allow_edit=allow_edit,
                    settings=settings,
                    allow_wizard_shortcut=allow_wizard_shortcut,
                )
                dialog.changed = False
                return dialog

            with patch("src.app.JobWorkflowDialog", side_effect=_make_dialog):
                window._new_workflow()

            assert len(_DummyWorkflowDialog.instances) == 1
            job = _DummyWorkflowDialog.instances[0].job
            assert job.name == "Neuer Workflow"
            assert job.encoder == "hevc_nvenc"
            assert job.crf == 21
            assert job.preset == "slow"
            assert job.fps == 50
            assert job.output_format == "mov"
            assert job.amplify_audio is True
            assert job.amplify_db == 9.5
        finally:
            window.close()

    def test_new_workflow_opens_workflow_editor_directly_and_applies_result(self):
        window = _new_app()
        try:
            _DummyWorkflowDialog.instances.clear()

            def _make_dialog(parent, job, allow_edit=False, settings=None, allow_wizard_shortcut=True):
                dialog = _DummyWorkflowDialog(
                    parent,
                    job,
                    allow_edit=allow_edit,
                    settings=settings,
                    allow_wizard_shortcut=allow_wizard_shortcut,
                )
                dialog.changed = True
                job.name = "Neuer Workflow"
                job.files = [FileEntry(source_path="/tmp/a.mp4")]
                return dialog

            with patch("src.app.JobWorkflowDialog", side_effect=_make_dialog), patch.object(window, "_save_last_workflow") as save_last_workflow:
                window._new_workflow()

            assert len(_DummyWorkflowDialog.instances) == 1
            assert _DummyWorkflowDialog.instances[0].allow_wizard_shortcut is False
            assert window._workflow.job is not None
            assert window._workflow.job.name == "Neuer Workflow"
            assert window._workflow.name == "Neuer Workflow"
            assert window.table.rowCount() == 1
            assert window.table.item(0, 1).text() == "Neuer Workflow"
            save_last_workflow.assert_called_once()
        finally:
            window.close()

    def test_new_workflow_appends_instead_of_overwriting_existing_job(self):
        window = _new_app()
        try:
            window._workflow = Workflow(jobs=[WorkflowJob(name="Bestehend")])
            _DummyWorkflowDialog.instances.clear()

            def _make_dialog(parent, job, allow_edit=False, settings=None, allow_wizard_shortcut=True):
                dialog = _DummyWorkflowDialog(
                    parent,
                    job,
                    allow_edit=allow_edit,
                    settings=settings,
                    allow_wizard_shortcut=allow_wizard_shortcut,
                )
                dialog.changed = True
                job.name = "Neu"
                job.files = [FileEntry(source_path="/tmp/b.mp4")]
                return dialog

            with patch("src.app.JobWorkflowDialog", side_effect=_make_dialog), patch.object(window, "_save_last_workflow") as save_last_workflow:
                window._new_workflow()

            assert [job.name for job in window._workflow.jobs] == ["Bestehend", "Neu"]
            assert window.table.rowCount() == 2
            assert window.table.item(0, 1).text() == "Bestehend"
            assert window.table.item(1, 1).text() == "Neu"
            save_last_workflow.assert_called_once()
        finally:
            window.close()

    def test_new_workflow_duplicate_name_can_increment(self):
        window = _new_app()
        try:
            window._workflow = Workflow(jobs=[WorkflowJob(name="Spieltag")])
            _DummyWorkflowDialog.instances.clear()

            def _make_dialog(parent, job, allow_edit=False, settings=None, allow_wizard_shortcut=True):
                dialog = _DummyWorkflowDialog(
                    parent,
                    job,
                    allow_edit=allow_edit,
                    settings=settings,
                    allow_wizard_shortcut=allow_wizard_shortcut,
                )
                dialog.changed = True
                job.name = "Spieltag"
                job.files = [FileEntry(source_path="/tmp/c.mp4")]
                return dialog

            class _IncrementMessageBox:
                class Icon:
                    Warning = QMessageBox.Icon.Warning

                class ButtonRole:
                    AcceptRole = QMessageBox.ButtonRole.AcceptRole
                    ActionRole = QMessageBox.ButtonRole.ActionRole
                    RejectRole = QMessageBox.ButtonRole.RejectRole

                def __init__(self, parent=None):
                    self._clicked = None
                    self._buttons = []

                def setIcon(self, *_args, **_kwargs):
                    return None

                def setWindowTitle(self, *_args, **_kwargs):
                    return None

                def setText(self, *_args, **_kwargs):
                    return None

                def setInformativeText(self, *_args, **_kwargs):
                    return None

                def addButton(self, text, _role):
                    button = object()
                    self._buttons.append((text, button))
                    if text == "Inkrementieren":
                        self._increment_button = button
                    return button

                def setDefaultButton(self, *_args, **_kwargs):
                    return None

                def exec(self):
                    self._clicked = self._increment_button
                    return 0

                def clickedButton(self):
                    return self._clicked

            with patch("src.app.JobWorkflowDialog", side_effect=_make_dialog), patch("src.app.QMessageBox", _IncrementMessageBox), patch.object(window, "_save_last_workflow") as save_last_workflow:
                window._new_workflow()

            assert [job.name for job in window._workflow.jobs] == ["Spieltag", "Spieltag (2)"]
            assert window.table.rowCount() == 2
            assert window.table.item(1, 1).text() == "Spieltag (2)"
            save_last_workflow.assert_called_once()
        finally:
            window.close()

    def test_duplicate_job_inserts_copy_below_selection_and_selects_it(self):
        window = _new_app()
        try:
            original = _rich_job(name="Spieltag")
            second = WorkflowJob(name="Anderer Job", source_mode="files", files=[FileEntry(source_path="/tmp/b.mp4")])
            window._workflow = Workflow(jobs=[original, second])
            window._refresh_table()
            window.table.selectRow(0)

            with patch.object(window, "_save_last_workflow") as save_last_workflow:
                window._duplicate_job()

            assert [job.name for job in window._workflow.jobs] == ["Spieltag", "Spieltag (2)", "Anderer Job"]
            assert window._workflow.jobs[1].id != window._workflow.jobs[0].id
            assert window._workflow.jobs[1].to_dict() == {
                **window._workflow.jobs[0].to_dict(),
                "id": window._workflow.jobs[1].id,
                "name": "Spieltag (2)",
            }
            assert window.table.currentRow() == 1
            assert window.table.item(1, 1).text() == "Spieltag (2)"
            save_last_workflow.assert_called_once()
        finally:
            window.close()

    def test_duplicate_action_in_menu_triggers_job_copy(self):
        window = _new_app()
        try:
            window._workflow = Workflow(jobs=[WorkflowJob(name="Spieltag", source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")])])
            window._refresh_table()
            window.table.selectRow(0)

            with patch.object(window, "_duplicate_job") as duplicate_job:
                window.act_duplicate.trigger()

            duplicate_job.assert_called_once_with()
        finally:
            window.close()

    def test_refresh_table_uses_restored_resume_status_and_tooltip(self):
        window = _new_app()
        try:
            window._workflow = Workflow(jobs=[
                WorkflowJob(
                    name="Job 1",
                    resume_status="Transfer OK",
                    step_statuses={"transfer": "done", "convert": "running"},
                )
            ])

            window._refresh_table()

            item = window.table.item(0, 4)
            assert item.text() == "Transfer OK"
            assert item.toolTip() == (
                "Letzter Status: Transfer OK\n"
                "Transfer: done\n"
                "Konvertierung: running"
            )
        finally:
            window.close()

    def test_refresh_table_shows_persisted_job_and_workflow_durations(self):
        window = _new_app()
        try:
            window._workflow = Workflow(
                jobs=[
                    WorkflowJob(
                        name="Job 1",
                        resume_status="Fertig",
                        run_elapsed_seconds=125,
                    )
                ],
                last_run_elapsed_seconds=3665,
            )

            window._refresh_table()

            assert window.table.item(0, 6).text() == "2min 05s"
            assert window.duration_label.text() == "Gesamtdauer: 1h 01min"
        finally:
            window.close()

    def test_roundtrip_restores_duration_metadata(self, tmp_path):
        workflow = Workflow(
            jobs=[
                WorkflowJob(
                    name="Job 1",
                    source_mode="files",
                    files=[FileEntry(source_path="/tmp/a.mp4")],
                    run_started_at="2026-03-23T10:00:00",
                    run_finished_at="2026-03-23T10:03:05",
                    run_elapsed_seconds=185,
                )
            ],
            last_run_started_at="2026-03-23T10:00:00",
            last_run_finished_at="2026-03-23T10:04:10",
            last_run_elapsed_seconds=250,
        )

        restored = _roundtrip_restored_workflow(tmp_path, workflow)
        try:
            job = restored._workflow.jobs[0]
            assert job.run_started_at == "2026-03-23T10:00:00"
            assert job.run_finished_at == "2026-03-23T10:03:05"
            assert job.run_elapsed_seconds == 185
            assert restored._workflow.last_run_elapsed_seconds == 250
            assert restored.duration_label.text() == "Gesamtdauer: 4min 10s"
        finally:
            restored.close()

    def test_refresh_table_overwrites_stale_cells_after_workflow_replace(self):
        window = _new_app()
        try:
            window._workflow = Workflow(jobs=[WorkflowJob(name="Alt")])
            window._refresh_table()
            window.table.item(0, 4).setText("Veralteter Status")
            window.table.item(0, 5).setText("91%")

            window._workflow = Workflow(jobs=[
                WorkflowJob(
                    name="Neu",
                    resume_status="Transfer OK",
                    step_statuses={"transfer": "done"},
                )
            ])
            window._refresh_table()

            assert window.table.item(0, 1).text() == "Neu"
            assert window.table.item(0, 4).text() == "Transfer OK"
            assert window.table.item(0, 5).text() == "0%"
        finally:
            window.close()

    def test_set_row_status_marks_merge_as_running(self):
        window = _new_app()
        try:
            window._workflow = Workflow(jobs=[WorkflowJob(name="Job 1")])
            window._refresh_table()

            window._set_row_status(0, "Zusammenführen …")

            item = window.table.item(0, 4)
            assert item.text() == "Zusammenführen …"
            assert item.foreground().color() == Qt.blue
        finally:
            window.close()

    def test_set_row_status_marks_detailed_step_statuses_as_running(self):
        window = _new_app()
        try:
            window._workflow = Workflow(jobs=[WorkflowJob(name="Job 1")])
            window._refresh_table()

            for status in (
                "Transfer 1/3: clip.mp4 …",
                "Titelkarte erstellen …",
                "YT-Version erstellen …",
                "Kaderblick senden …",
            ):
                window._set_row_status(0, status)
                item = window.table.item(0, 4)
                assert item.text() == status
                assert item.foreground().color() == Qt.blue
        finally:
            window.close()

    def test_on_job_status_updates_resume_status_and_saves_last_workflow(self):
        window = _new_app()
        try:
            job = WorkflowJob(
                name="Job 1",
                step_statuses={"transfer": "done", "convert": "running"},
            )
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()
            window._save_last_workflow = MagicMock()

            window._on_job_status(0, "Konvertiere …")

            item = window.table.item(0, 4)
            overall_item = window.table.item(0, 5)
            assert job.resume_status == "Konvertiere …"
            assert item.text() == "Konvertiere …"
            assert overall_item.text() == "50%"
            assert item.toolTip() == (
                "Letzter Status: Konvertiere …\n"
                "Transfer: done\n"
                "Konvertierung: running"
            )
            window._save_last_workflow.assert_called_once()
        finally:
            window.close()

    def test_on_job_progress_updates_step_and_overall_columns(self):
        window = _new_app()
        try:
            job = WorkflowJob(name="Job 1", convert_enabled=True, upload_youtube=True)
            job.resume_status = "Konvertiere …"
            job.step_statuses = {"transfer": "done", "convert": "running"}
            job.current_step_key = "convert"
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()

            window._on_job_progress(0, 50)

            assert window.table.item(0, 4).data(int(Qt.ItemDataRole.UserRole)) == 50
            assert window.table.item(0, 5).text() == "50%"
            assert window.table.item(0, 5).data(int(Qt.ItemDataRole.UserRole) + 1) == 50
        finally:
            window.close()

    def test_source_progress_does_not_override_running_convert_progress(self):
        window = _new_app()
        try:
            job = WorkflowJob(name="Job 1", convert_enabled=True, upload_youtube=True)
            job.resume_status = "Konvertiere …"
            job.step_statuses = {"transfer": "running", "convert": "running"}
            job.current_step_key = "convert"
            job.progress_pct = 44
            job.transfer_progress_pct = 12
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()

            window._on_source_progress(0, 73)

            assert job.progress_pct == 44
            assert job.transfer_progress_pct == 73
            assert window.table.item(0, 4).data(int(Qt.ItemDataRole.UserRole)) == 44
        finally:
            window.close()

    def test_start_workflow_clears_persisted_resume_state_before_run(self):
        window = _new_app()
        try:
            job = WorkflowJob(
                name="Job 1",
                resume_status="Alter Status",
                step_statuses={"transfer": "done"},
            )
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()
            window._save_last_workflow = MagicMock()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.No), patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            assert job.resume_status == ""
            assert job.step_statuses == {}
            assert job.progress_pct == 0
            assert job.overall_progress_pct == 0
            assert job.current_step_key == ""
            assert window.table.item(0, 4).text() == "Wartend"
            assert window.table.item(0, 5).text() == "0%"
            window._save_last_workflow.assert_called_once()
        finally:
            window.close()

    def test_start_workflow_does_not_start_job_duration_for_waiting_jobs(self):
        window = _new_app()
        try:
            first = WorkflowJob(name="Job 1", source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")])
            second = WorkflowJob(name="Job 2", source_mode="files", files=[FileEntry(source_path="/tmp/b.mp4")])
            window._workflow = Workflow(jobs=[first, second])
            window._refresh_table()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.No), patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor), patch("src.app.execution.time.monotonic", return_value=10_000.0):
                window._start_workflow()

            with patch("src.app.execution.time.monotonic", return_value=10_600.0):
                window._refresh_runtime_durations()

            assert first.run_elapsed_seconds == 0.0
            assert second.run_elapsed_seconds == 0.0
            assert window.table.item(0, 6).text() == "–"
            assert window.table.item(1, 6).text() == "–"
        finally:
            window.close()

    def test_restart_mode_clears_stale_waiting_job_duration(self):
        window = _new_app()
        try:
            job = WorkflowJob(
                name="Job 1",
                source_mode="files",
                files=[FileEntry(source_path="/tmp/a.mp4")],
                resume_status="Alt",
                step_statuses={"transfer": "done"},
                run_started_at="2026-03-23T10:00:00",
                run_finished_at="2026-03-23T18:00:00",
                run_elapsed_seconds=8 * 3600,
            )
            window._workflow = Workflow(jobs=[job], last_run_elapsed_seconds=8 * 3600)
            window._refresh_table()
            window._save_last_workflow = MagicMock()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.No), patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            assert job.run_started_at == ""
            assert job.run_finished_at == ""
            assert job.run_elapsed_seconds == 0.0
            assert window.table.item(0, 6).text() == "–"
            assert window.duration_label.text() == "Gesamtdauer: –"
        finally:
            window.close()

    def test_finished_job_duration_does_not_continue_after_late_events(self):
        window = _new_app()
        try:
            job = WorkflowJob(
                name="Job 1",
                source_mode="files",
                files=[FileEntry(source_path="/tmp/a.mp4")],
                run_started_at="2026-03-23T10:00:00",
                run_finished_at="2026-03-23T10:05:00",
                run_elapsed_seconds=300,
                resume_status="Fertig",
                step_statuses={"transfer": "done", "convert": "done"},
            )
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()
            window._job_run_elapsed_base_seconds = {job.id: 300.0}
            window._job_run_started_monotonic = {}

            window._on_job_progress(0, 100)
            window._on_job_status(0, "Transfer 2/2: a.mp4 …")

            assert job.run_elapsed_seconds == 300
            assert job.run_finished_at == "2026-03-23T10:05:00"
            assert window.table.item(0, 6).text() == "5min 00s"
        finally:
            window.close()

    def test_job_duration_pauses_while_job_waits_for_pipeline_slot(self):
        window = _new_app()
        try:
            job = WorkflowJob(
                name="Job 2",
                source_mode="files",
                files=[FileEntry(source_path="/tmp/b.mp4")],
            )
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.No), patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor), patch("src.app.execution.time.monotonic", return_value=1_000.0):
                window._start_workflow(active_indices={0})

            with patch("src.app.execution.time.monotonic", return_value=1_000.0):
                window._on_job_status(0, "Transfer 1/1: b.mp4 …")
            with patch("src.app.execution.time.monotonic", return_value=1_120.0):
                window._refresh_runtime_durations()

            assert job.run_elapsed_seconds == 120.0

            with patch("src.app.execution.time.monotonic", return_value=1_120.0):
                window._on_job_status(0, "Transfer OK")
            with patch("src.app.execution.time.monotonic", return_value=1_400.0):
                window._refresh_runtime_durations()

            assert job.run_elapsed_seconds == 120.0
            assert window.table.item(0, 6).text() == "2min 00s"

            with patch("src.app.execution.time.monotonic", return_value=1_400.0):
                window._on_job_status(0, "Zusammenführen …")
            with patch("src.app.execution.time.monotonic", return_value=1_460.0):
                window._refresh_runtime_durations()

            assert job.run_elapsed_seconds == 180.0
            assert window.table.item(0, 6).text() == "3min 00s"
        finally:
            window.close()

    def test_job_durations_only_advance_for_the_job_that_is_currently_working(self):
        window = _new_app()
        try:
            first = WorkflowJob(name="Job 1", source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")])
            second = WorkflowJob(name="Job 2", source_mode="files", files=[FileEntry(source_path="/tmp/b.mp4")])
            third = WorkflowJob(name="Job 3", source_mode="files", files=[FileEntry(source_path="/tmp/c.mp4")])
            window._workflow = Workflow(jobs=[first, second, third])
            window._refresh_table()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.No), patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor), patch("src.app.execution.time.monotonic", return_value=2_000.0):
                window._start_workflow(active_indices={0, 1, 2})

            with patch("src.app.execution.time.monotonic", return_value=2_000.0):
                window._on_job_status(0, "Transfer 1/1: a.mp4 …")
            with patch("src.app.execution.time.monotonic", return_value=2_120.0):
                window._refresh_runtime_durations()

            assert first.run_elapsed_seconds == 120.0
            assert second.run_elapsed_seconds == 0.0
            assert third.run_elapsed_seconds == 0.0

            with patch("src.app.execution.time.monotonic", return_value=2_120.0):
                window._on_job_status(0, "Transfer OK")
                window._on_job_status(1, "Transfer 1/1: b.mp4 …")
            with patch("src.app.execution.time.monotonic", return_value=2_300.0):
                window._refresh_runtime_durations()

            assert first.run_elapsed_seconds == 120.0
            assert second.run_elapsed_seconds == 180.0
            assert third.run_elapsed_seconds == 0.0

            with patch("src.app.execution.time.monotonic", return_value=2_300.0):
                window._on_job_status(1, "Transfer OK")
                window._on_job_status(2, "Transfer 1/1: c.mp4 …")
            with patch("src.app.execution.time.monotonic", return_value=2_420.0):
                window._refresh_runtime_durations()

            assert first.run_elapsed_seconds == 120.0
            assert second.run_elapsed_seconds == 180.0
            assert third.run_elapsed_seconds == 120.0
            assert window.table.item(0, 6).text() == "2min 00s"
            assert window.table.item(1, 6).text() == "3min 00s"
            assert window.table.item(2, 6).text() == "2min 00s"
        finally:
            window.close()

    def test_start_workflow_firststart_without_resume_does_not_prompt(self):
        window = _new_app()
        try:
            job = WorkflowJob(name="Job 1")
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()
            window._save_last_workflow = MagicMock()

            with patch.object(window, "_ask_resume_behavior") as question, patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            question.assert_not_called()
            assert job.resume_status == ""
            assert job.step_statuses == {}
            window._save_last_workflow.assert_called_once()
        finally:
            window.close()

    def test_start_workflow_prompts_once_when_any_job_is_resumeable(self):
        window = _new_app()
        try:
            job = WorkflowJob(
                name="Job 1",
                files=[FileEntry(source_path="/tmp/a.mp4")],
                resume_status="Transfer OK",
                step_statuses={"transfer": "done"},
            )
            window._workflow = Workflow(job=job)
            window._refresh_table()
            window._save_last_workflow = MagicMock()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.Yes) as question, patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            question.assert_called_once()
        finally:
            window.close()

    def test_start_workflow_does_not_prompt_for_invalid_resume_metadata(self):
        window = _new_app()
        try:
            job = WorkflowJob(name="Job 1", source_mode="files", files=[], resume_status="Transfer OK")
            window._workflow = Workflow(job=job)
            window._refresh_table()
            window._save_last_workflow = MagicMock()

            with patch.object(window, "_ask_resume_behavior") as question, patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            question.assert_not_called()
        finally:
            window.close()

    def test_start_workflow_resume_keeps_persisted_resume_state(self):
        window = _new_app()
        try:
            job = WorkflowJob(
                name="Job 1",
                files=[FileEntry(source_path="/tmp/a.mp4")],
                resume_status="Transfer OK",
                step_statuses={"transfer": "reused-target", "convert": "done"},
            )
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()
            window._save_last_workflow = MagicMock()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.Yes), patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            assert job.resume_status == "Transfer OK"
            assert job.step_statuses == {"transfer": "reused-target", "convert": "done"}
            assert window.table.item(0, 4).text() == "Transfer OK"
            window._save_last_workflow.assert_not_called()
        finally:
            window.close()

    def test_start_workflow_resume_keeps_all_normalized_restored_jobs(self):
        window = _new_app()
        try:
            window._workflow = Workflow(
                jobs=[
                    WorkflowJob(
                        name="Job 1",
                        source_mode="files",
                        files=[FileEntry(source_path="/tmp/a.mp4")],
                        resume_status="Konvertiere …",
                        current_step_key="convert",
                        step_statuses={"transfer": "done"},
                    ),
                    WorkflowJob(
                        name="Job 2",
                        source_mode="files",
                        files=[FileEntry(source_path="/tmp/b.mp4")],
                        create_youtube_version=True,
                        upload_youtube=True,
                        resume_status="YouTube-Upload …",
                        current_step_key="youtube_upload",
                        step_statuses={"transfer": "done", "convert": "done", "yt_version": "done"},
                    ),
                ]
            )
            window._refresh_table()
            window._save_last_workflow = MagicMock()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.Yes), patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            assert window._wf_executor is not None
            assert window._wf_executor.allow_reuse_existing is True
            assert window._workflow.jobs[0].resume_status == "Konvertiere …"
            assert window._workflow.jobs[0].step_statuses == {"transfer": "done"}
            assert window._workflow.jobs[1].resume_status == "YouTube-Upload …"
            assert window._workflow.jobs[1].step_statuses == {"transfer": "done", "convert": "done", "yt_version": "done"}
            window._save_last_workflow.assert_not_called()
        finally:
            window.close()

    def test_start_workflow_restart_clears_all_normalized_restored_jobs(self):
        window = _new_app()
        try:
            window._workflow = Workflow(
                jobs=[
                    WorkflowJob(
                        name="Job 1",
                        source_mode="files",
                        files=[FileEntry(source_path="/tmp/a.mp4")],
                        resume_status="Konvertiere …",
                        current_step_key="convert",
                        step_statuses={"transfer": "done"},
                    ),
                    WorkflowJob(
                        name="Job 2",
                        source_mode="files",
                        files=[FileEntry(source_path="/tmp/b.mp4")],
                        create_youtube_version=True,
                        upload_youtube=True,
                        resume_status="YouTube-Upload …",
                        current_step_key="youtube_upload",
                        step_statuses={"transfer": "done", "convert": "done", "yt_version": "done"},
                    ),
                ]
            )
            window._refresh_table()
            window._save_last_workflow = MagicMock()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.No), patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            assert window._wf_executor is not None
            assert window._wf_executor.allow_reuse_existing is False
            assert window._workflow.jobs[0].resume_status == ""
            assert window._workflow.jobs[0].step_statuses == {}
            assert window._workflow.jobs[1].resume_status == ""
            assert window._workflow.jobs[1].step_statuses == {}
            assert window.table.item(0, 4).text() == "Wartend"
            assert window.table.item(1, 4).text() == "Wartend"
            window._save_last_workflow.assert_called_once()
        finally:
            window.close()

    def test_start_workflow_resume_cancel_does_not_start(self):
        window = _new_app()
        try:
            job = WorkflowJob(
                name="Job 1",
                files=[FileEntry(source_path="/tmp/a.mp4")],
                resume_status="Transfer OK",
                step_statuses={"transfer": "done"},
            )
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()
            window._save_last_workflow = MagicMock()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.Cancel), patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            assert window._wf_thread is None
            assert window._wf_executor is None
            assert job.resume_status == "Transfer OK"
            window._save_last_workflow.assert_not_called()
        finally:
            window.close()

    def test_open_job_workflow_uses_selected_job_and_allows_edit_mode(self):
        window = _new_app()
        try:
            _DummyWorkflowDialog.instances.clear()
            job = WorkflowJob(name="Job 1")
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()
            window.table.selectRow(0)

            with patch("src.app.JobWorkflowDialog", _DummyWorkflowDialog):
                window._open_job_workflow()

            assert len(_DummyWorkflowDialog.instances) == 1
            assert _DummyWorkflowDialog.instances[0].job is job
            assert _DummyWorkflowDialog.instances[0].allow_edit is True
        finally:
            window.close()

    def test_open_job_workflow_refreshes_table_when_dialog_changed_job(self):
        window = _new_app()
        try:
            _DummyWorkflowDialog.instances.clear()
            job = WorkflowJob(name="Job 1")
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()
            window.table.selectRow(0)

            def _make_dialog(parent, selected_job, allow_edit=False, settings=None):
                dialog = _DummyWorkflowDialog(parent, selected_job, allow_edit=allow_edit, settings=settings)
                dialog.changed = True
                selected_job.name = "Geändert"
                return dialog

            with patch("src.app.JobWorkflowDialog", side_effect=_make_dialog), patch.object(window, "_save_last_workflow") as save_last_workflow:
                window._open_job_workflow()

            assert window.table.item(0, 1).text() == "Geändert"
            save_last_workflow.assert_called_once()
        finally:
            window.close()

    def test_open_job_workflow_uses_only_job_without_explicit_selection(self):
        window = _new_app()
        try:
            _DummyWorkflowDialog.instances.clear()
            job = WorkflowJob(name="Job 1")
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()
            window.table.clearSelection()

            with patch("src.app.JobWorkflowDialog", _DummyWorkflowDialog):
                window._open_job_workflow()

            assert len(_DummyWorkflowDialog.instances) == 1
            assert _DummyWorkflowDialog.instances[0].job is job
        finally:
            window.close()

    def test_open_job_workflow_shows_hint_when_multiple_jobs_and_none_selected(self):
        window = _new_app()
        try:
            window._workflow = Workflow()
            window._refresh_table()
            window.table.clearSelection()

            with patch("src.app.QMessageBox.information") as info, patch("src.app.JobWorkflowDialog") as dialog:
                window._open_job_workflow()

            info.assert_not_called()
            dialog.assert_not_called()
        finally:
            window.close()

    def test_table_double_click_opens_workflow_for_pipeline_columns(self):
        window = _new_app()
        try:
            window._workflow = Workflow(jobs=[WorkflowJob(name="Job 1")])
            window._refresh_table()
            index = window.table.model().index(0, 4)

            with patch.object(window, "_open_job_workflow") as open_workflow, patch.object(window, "_edit_job") as edit_job:
                window._handle_table_double_click(index)

            open_workflow.assert_called_once_with(0)
            edit_job.assert_not_called()
        finally:
            window.close()

    def test_table_double_click_opens_workflow_for_name_column(self):
        window = _new_app()
        try:
            window._workflow = Workflow(jobs=[WorkflowJob(name="Job 1")])
            window._refresh_table()
            index = window.table.model().index(0, 1)

            with patch.object(window, "_open_job_workflow") as open_workflow, patch.object(window, "_edit_job") as edit_job:
                window._handle_table_double_click(index)

            open_workflow.assert_called_once_with(0)
            edit_job.assert_not_called()
        finally:
            window.close()