"""Tests für Resume-Status und Session-Verhalten in der Haupt-GUI."""

import sys
from unittest.mock import MagicMock, patch

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QMessageBox

_app = QApplication.instance() or QApplication(sys.argv)

from src.app import (
    ConverterApp,
    _compute_job_overall_progress,
    _format_resume_tooltip,
    _job_has_source_config,
    _planned_job_steps,
    _repair_restored_workflow,
)
from src.settings import AppSettings
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
    def __init__(self, workflow, settings):
        self.workflow = workflow
        self.settings = settings
        self.log_message = _DummySignal()
        self.job_status = _DummySignal()
        self.job_progress = _DummySignal()
        self.file_progress = _DummySignal()
        self.overall_progress = _DummySignal()
        self.phase_changed = _DummySignal()
        self.finished = _DummySignal()

    def moveToThread(self, thread):
        self.thread = thread

    def run(self):
        return None

    def cancel(self):
        return None


def _new_app() -> ConverterApp:
    with patch("src.app.AppSettings.load", return_value=AppSettings()):
        window = ConverterApp()
    return window


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


class TestSessionRepair:
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


class TestConverterAppResumeState:
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

    def test_on_job_status_updates_resume_status_and_saves_session(self):
        window = _new_app()
        try:
            job = WorkflowJob(
                name="Job 1",
                step_statuses={"transfer": "done", "convert": "running"},
            )
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()
            window._save_session = MagicMock()

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
            window._save_session.assert_called_once()
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
            window._save_session = MagicMock()

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
            window._save_session.assert_called_once()
        finally:
            window.close()

    def test_start_workflow_firststart_without_resume_does_not_prompt(self):
        window = _new_app()
        try:
            job = WorkflowJob(name="Job 1")
            window._workflow = Workflow(jobs=[job])
            window._refresh_table()
            window._save_session = MagicMock()

            with patch.object(window, "_ask_resume_behavior") as question, patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            question.assert_not_called()
            assert job.resume_status == ""
            assert job.step_statuses == {}
            window._save_session.assert_called_once()
        finally:
            window.close()

    def test_start_workflow_prompts_once_when_any_job_is_resumeable(self):
        window = _new_app()
        try:
            jobs = [
                WorkflowJob(name="Job 1", files=[FileEntry(source_path="/tmp/a.mp4")]),
                WorkflowJob(
                    name="Job 2",
                    files=[FileEntry(source_path="/tmp/b.mp4")],
                    resume_status="Transfer OK",
                    step_statuses={"transfer": "done"},
                ),
            ]
            window._workflow = Workflow(jobs=jobs)
            window._refresh_table()
            window._save_session = MagicMock()

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
            jobs = [
                WorkflowJob(name="Job 1", source_mode="files", files=[], resume_status="Transfer OK"),
            ]
            window._workflow = Workflow(jobs=jobs)
            window._refresh_table()
            window._save_session = MagicMock()

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
            window._save_session = MagicMock()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.Yes), patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            assert job.resume_status == "Transfer OK"
            assert job.step_statuses == {"transfer": "reused-target", "convert": "done"}
            assert window.table.item(0, 4).text() == "Transfer OK"
            window._save_session.assert_not_called()
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
            window._save_session = MagicMock()

            with patch.object(window, "_ask_resume_behavior", return_value=QMessageBox.Cancel), patch(
                "src.app.QThread", _DummyThread
            ), patch("src.app.WorkflowExecutor", _DummyExecutor):
                window._start_workflow()

            assert window._wf_thread is None
            assert window._wf_executor is None
            assert job.resume_status == "Transfer OK"
            window._save_session.assert_not_called()
        finally:
            window.close()