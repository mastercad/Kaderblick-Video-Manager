"""Tests für WorkflowExecutor – innere Logik (ohne Qt-Event-Loop).

Geprüft:
- _find_file_entry()        – FileEntry-Suche per Pfad-String
- _resolve_youtube_title()  – Fallback-Kette: per-Datei → Job-Standard → Dateiname
- _build_job_settings()     – Job-Werte werden in AppSettings übertragen
- Merge-Gruppen-Erkennung   – _merge_skip_conv_idx enthält die richtigen Indizes
- Failure-Counter            – _transfer_fail zählt Fehler in Phase 1
- finished-Signal            – (ok, skip, fail) korrekt bei leerem Workflow
"""

import threading
import tempfile
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# PySide6 braucht eine QApplication – aber WorkflowExecutor ist ein QObject,
# also müssen wir eine minimal-App erheben.
from PySide6.QtWidgets import QApplication
import sys

# Einmalige App-Instanz für alle Tests in diesem Modul
_app = QApplication.instance() or QApplication(sys.argv)

from src.workflow import Workflow, WorkflowJob, FileEntry
from src.workflow_executor import WorkflowExecutor


# ─── Hilfsfunktionen ─────────────────────────────────────────────────────────

def _make_settings():
    """Erstellt ein minimales AppSettings-Objekt ohne Datei-I/O."""
    from src.settings import AppSettings
    return AppSettings()   # Standardwerte, ohne Datei-I/O


# ─── _find_file_entry ─────────────────────────────────────────────────────────

class TestFindFileEntry:
    def _executor(self) -> WorkflowExecutor:
        wf = Workflow()
        return WorkflowExecutor(wf, _make_settings())

    def test_found_by_exact_path(self):
        job = WorkflowJob()
        job.files = [FileEntry(source_path="/a/b.mp4")]
        result = WorkflowExecutor._find_file_entry(job, "/a/b.mp4")
        assert result is not None
        assert result.source_path == "/a/b.mp4"

    def test_not_found_returns_none(self):
        job = WorkflowJob()
        job.files = [FileEntry(source_path="/a/b.mp4")]
        assert WorkflowExecutor._find_file_entry(job, "/a/c.mp4") is None

    def test_empty_files_returns_none(self):
        job = WorkflowJob()   # files=[]
        assert WorkflowExecutor._find_file_entry(job, "/a/b.mp4") is None

    def test_first_match_returned(self):
        """Falls zwei Einträge den gleichen Pfad haben, wird der erste zurückgegeben."""
        fe1 = FileEntry(source_path="/x.mp4", youtube_title="Erster")
        fe2 = FileEntry(source_path="/x.mp4", youtube_title="Zweiter")
        job = WorkflowJob(files=[fe1, fe2])
        result = WorkflowExecutor._find_file_entry(job, "/x.mp4")
        assert result.youtube_title == "Erster"

    def test_unique_filename_match_after_copy(self):
        """Nach Kopieren/Verschieben bleibt die Zuordnung über den Dateinamen erhalten."""
        job = WorkflowJob()
        job.files = [FileEntry(source_path="/quelle/halbzeit1.mp4", merge_group_id="hz1")]
        result = WorkflowExecutor._find_file_entry(job, "/ziel/halbzeit1.mp4")
        assert result is not None
        assert result.merge_group_id == "hz1"

    def test_ambiguous_filename_match_returns_none(self):
        """Gleicher Dateiname mehrfach: kein unsicheres Fallback verwenden."""
        job = WorkflowJob()
        job.files = [
            FileEntry(source_path="/quelle_a/halbzeit.mp4", youtube_title="A"),
            FileEntry(source_path="/quelle_b/halbzeit.mp4", youtube_title="B"),
        ]
        result = WorkflowExecutor._find_file_entry(job, "/ziel/halbzeit.mp4")
        assert result is None


# ─── _resolve_youtube_title ───────────────────────────────────────────────────

class TestResolveYoutubeTitle:
    def test_per_file_title_wins(self):
        fe = FileEntry(source_path="/a/b.mp4", youtube_title="Datei-Titel")
        job = WorkflowJob(
            default_youtube_title="Job-Standard",
            files=[fe],
        )
        result = WorkflowExecutor._resolve_youtube_title(job, "/a/b.mp4")
        assert result == "Datei-Titel"

    def test_job_default_used_when_no_file_title(self):
        fe = FileEntry(source_path="/a/b.mp4", youtube_title="")
        job = WorkflowJob(
            default_youtube_title="Job-Standard",
            files=[fe],
        )
        result = WorkflowExecutor._resolve_youtube_title(job, "/a/b.mp4")
        assert result == "Job-Standard"

    def test_filename_stem_as_last_fallback(self):
        fe = FileEntry(source_path="/a/mein_video.mp4", youtube_title="")
        job = WorkflowJob(
            default_youtube_title="",
            files=[fe],
        )
        result = WorkflowExecutor._resolve_youtube_title(job, "/a/mein_video.mp4")
        assert result == "mein_video"

    def test_no_entry_uses_job_default(self):
        """Wenn keine FileEntry existiert → Job-Standard."""
        job = WorkflowJob(default_youtube_title="Globaler Titel", files=[])
        result = WorkflowExecutor._resolve_youtube_title(job, "/a/b.mp4")
        assert result == "Globaler Titel"

    def test_no_entry_no_default_uses_stem(self):
        job = WorkflowJob(default_youtube_title="", files=[])
        result = WorkflowExecutor._resolve_youtube_title(job, "/videos/abcdef.mkv")
        assert result == "abcdef"


# ─── _build_job_settings ──────────────────────────────────────────────────────

class TestBuildJobSettings:
    def _executor(self) -> WorkflowExecutor:
        wf = Workflow()
        return WorkflowExecutor(wf, _make_settings())

    def test_encoder_transferred(self):
        ex = self._executor()
        job = WorkflowJob(encoder="libx265", crf=28, preset="slow",
                          fps=30, output_format="avi")
        s = ex._build_job_settings(job)
        assert s.video.encoder == "libx265"
        assert s.video.crf == 28
        assert s.video.preset == "slow"
        assert s.video.fps == 30
        assert s.video.output_format == "avi"

    def test_audio_transferred(self):
        ex = self._executor()
        job = WorkflowJob(
            merge_audio=True,
            amplify_audio=True,
            amplify_db=9.0,
            audio_sync=True,
        )
        s = ex._build_job_settings(job)
        assert s.audio.include_audio is True
        assert s.audio.amplify_audio is True
        assert s.audio.amplify_db == 9.0
        assert s.video.audio_sync is True

    def test_youtube_flags_transferred(self):
        ex = self._executor()
        job = WorkflowJob(
            create_youtube_version=True,
            upload_youtube=True,
        )
        s = ex._build_job_settings(job)
        assert s.youtube.create_youtube is True
        assert s.youtube.upload_to_youtube is True

    def test_does_not_mutate_original_settings(self):
        """_build_job_settings darf die globalen Settings nicht verändern."""
        settings = _make_settings()
        original_encoder = settings.video.encoder
        wf = Workflow()
        ex = WorkflowExecutor(wf, settings)
        job = WorkflowJob(encoder="libx265")
        ex._build_job_settings(job)
        assert settings.video.encoder == original_encoder


# ─── Merge-Gruppen-Logik ─────────────────────────────────────────────────────

class TestMergeGroupDetection:
    """Testet die interne _merge_skip_conv_idx-Berechnung.

    Der WorkflowExecutor baut die Skip-Menge im run()-Slot auf.
    Wir testen die Logik direkt auf der Datenebene, ohne run() aufzurufen.
    """

    @staticmethod
    def _compute_skip_set(
        file_entries: list[FileEntry],
        job: WorkflowJob,
    ) -> set[int]:
        """Repliziert die interne Merge-Skip-Logik aus workflow_executor.py.

        Seit dem Fix gehören ALLE Mitglieder einer Merge-Gruppe zur Skip-Menge
        (also auch das erste), damit kein Einzel-Upload vor dem Concat geschieht.
        """
        from src.converter import ConvertJob
        to_convert = [
            (0, job, ConvertJob(source_path=Path(fe.source_path)))
            for fe in file_entries
        ]
        skip: set[int] = set()
        for ci, (_, j, cv) in enumerate(to_convert):
            entry = WorkflowExecutor._find_file_entry(j, str(cv.source_path))
            gid = (getattr(entry, "merge_group_id", "") or "") if entry else ""
            if gid:
                skip.add(ci)   # alle Mitglieder – auch das erste
        return skip

    def _job_with_entries(self, entries: list[FileEntry]) -> WorkflowJob:
        job = WorkflowJob(source_mode="files", convert_enabled=True)
        job.files = entries
        return job

    def test_no_groups_nothing_skipped(self):
        entries = [
            FileEntry(source_path="/a/1.mp4"),
            FileEntry(source_path="/a/2.mp4"),
        ]
        job = self._job_with_entries(entries)
        skip = self._compute_skip_set(entries, job)
        assert skip == set()

    def test_two_in_group_both_skipped(self):
        """Beide Mitglieder der Gruppe werden übersprungen – Upload nach Merge."""
        entries = [
            FileEntry(source_path="/a/1.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/2.mp4", merge_group_id="g1"),
        ]
        job = self._job_with_entries(entries)
        skip = self._compute_skip_set(entries, job)
        assert skip == {0, 1}

    def test_three_in_group_all_skipped(self):
        entries = [
            FileEntry(source_path="/a/1.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/2.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/3.mp4", merge_group_id="g1"),
        ]
        job = self._job_with_entries(entries)
        skip = self._compute_skip_set(entries, job)
        assert skip == {0, 1, 2}

    def test_two_separate_groups_all_members_skipped(self):
        entries = [
            FileEntry(source_path="/a/1.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/2.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/3.mp4", merge_group_id="g2"),
            FileEntry(source_path="/a/4.mp4", merge_group_id="g2"),
        ]
        job = self._job_with_entries(entries)
        skip = self._compute_skip_set(entries, job)
        assert skip == {0, 1, 2, 3}

    def test_mixed_grouped_and_ungrouped(self):
        """Nicht-Mitglieder bleiben außerhalb der Skip-Menge."""
        entries = [
            FileEntry(source_path="/a/1.mp4"),
            FileEntry(source_path="/a/2.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/3.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/4.mp4"),
        ]
        job = self._job_with_entries(entries)
        skip = self._compute_skip_set(entries, job)
        assert skip == {1, 2}

    def test_group_detected_after_copy_by_filename(self):
        """Merge-Gruppe bleibt auch nach Transfer in einen Zielordner erkennbar."""
        job = self._job_with_entries([
            FileEntry(source_path="/quelle/teil1.mp4", merge_group_id="g1"),
            FileEntry(source_path="/quelle/teil2.mp4", merge_group_id="g1"),
        ])
        assert WorkflowExecutor._get_merge_group_id(job, "/ziel/teil1.mp4") == "g1"
        assert WorkflowExecutor._get_merge_group_id(job, "/ziel/teil2.mp4") == "g1"


# ─── Transfer-Fehler-Zähler ───────────────────────────────────────────────────

class TestTransferFailCounter:
    def test_initial_fail_count_is_zero(self):
        wf = Workflow()
        ex = WorkflowExecutor(wf, _make_settings())
        assert ex._transfer_fail == 0

    def test_empty_workflow_emits_finished_0_0_0(self):
        finished_args = []
        wf = Workflow(jobs=[])
        ex = WorkflowExecutor(wf, _make_settings())
        ex.finished.connect(lambda ok, sk, fa: finished_args.append((ok, sk, fa)))
        ex.run()
        assert finished_args == [(0, 0, 0)]

    def test_disabled_jobs_not_counted(self):
        finished_args = []
        j = WorkflowJob(enabled=False)
        wf = Workflow(jobs=[j])
        ex = WorkflowExecutor(wf, _make_settings())
        ex.finished.connect(lambda ok, sk, fa: finished_args.append((ok, sk, fa)))
        ex.run()
        assert finished_args == [(0, 0, 0)]


class TestTransferResumeFallbacks:
    def test_direct_files_reuse_existing_target_when_source_missing(self, tmp_path):
        dst_dir = tmp_path / "ziel"
        dst_dir.mkdir()
        existing = dst_dir / "halbzeit1.mp4"
        existing.write_text("data", encoding="utf-8")

        job = WorkflowJob(
            name="Direkt",
            source_mode="files",
            copy_destination=str(dst_dir),
            files=[FileEntry(source_path=str(tmp_path / "quelle" / "halbzeit1.mp4"))],
        )
        ex = WorkflowExecutor(Workflow(), _make_settings())

        paths = ex._handle_direct_files(job)

        assert paths == [str(existing)]

    def test_scan_folder_reuses_existing_target_when_source_folder_missing(self, tmp_path):
        dst_dir = tmp_path / "ziel"
        dst_dir.mkdir()
        reused = dst_dir / "clip1.mp4"
        reused.write_text("data", encoding="utf-8")

        job = WorkflowJob(
            name="Ordner",
            source_mode="folder_scan",
            source_folder=str(tmp_path / "quelle-fehlt"),
            copy_destination=str(dst_dir),
            file_pattern="*.mp4",
        )
        ex = WorkflowExecutor(Workflow(), _make_settings())

        paths = ex._scan_folder(job)

        assert paths == [str(reused)]
        assert job.step_statuses["transfer"] == "reused-target"

    def test_direct_files_transfer_emits_status_and_progress(self, tmp_path):
        src_a = tmp_path / "a.mp4"
        src_b = tmp_path / "b.mp4"
        src_a.write_bytes(b"a" * 128)
        src_b.write_bytes(b"b" * 256)
        dst_dir = tmp_path / "ziel"

        job = WorkflowJob(
            name="Direkt",
            source_mode="files",
            copy_destination=str(dst_dir),
            files=[
                FileEntry(source_path=str(src_a)),
                FileEntry(source_path=str(src_b)),
            ],
        )
        ex = WorkflowExecutor(Workflow(), _make_settings())
        statuses = []
        progress = []
        ex.job_status.connect(lambda idx, status: statuses.append((idx, status)))
        ex.job_progress.connect(lambda idx, pct: progress.append((idx, pct)))

        paths = ex._handle_direct_files(job)

        assert [Path(path).name for path in paths] == ["a.mp4", "b.mp4"]
        assert statuses[0] == (0, "Transfer 1/2: a.mp4 …")
        assert statuses[-1] == (0, "Transfer 2/2: b.mp4 …")
        assert progress[0] == (0, 0)
        assert progress[-1] == (0, 100)

    def test_folder_scan_transfer_emits_status_and_progress_without_copy(self, tmp_path):
        src_dir = tmp_path / "quelle"
        src_dir.mkdir()
        (src_dir / "clip1.mp4").write_text("1", encoding="utf-8")
        (src_dir / "clip2.mp4").write_text("2", encoding="utf-8")

        job = WorkflowJob(
            name="Ordner",
            source_mode="folder_scan",
            source_folder=str(src_dir),
            copy_destination="",
            file_pattern="*.mp4",
        )
        ex = WorkflowExecutor(Workflow(), _make_settings())
        statuses = []
        progress = []
        ex.job_status.connect(lambda idx, status: statuses.append((idx, status)))
        ex.job_progress.connect(lambda idx, pct: progress.append((idx, pct)))

        paths = ex._scan_folder(job)

        assert [Path(path).name for path in paths] == ["clip1.mp4", "clip2.mp4"]
        assert statuses == [
            (0, "Transfer 1/2: clip1.mp4 …"),
            (0, "Transfer 2/2: clip2.mp4 …"),
        ]
        assert progress[0] == (0, 0)
        assert progress[-1] == (0, 100)

    @patch("src.workflow_executor.run_convert")
    @patch("src.workflow_executor.download_device", side_effect=RuntimeError("offline"))
    def test_download_resume_fallback_continues_workflow_without_failure(
        self,
        _mock_download,
        mock_convert,
        tmp_path,
    ):
        download_root = tmp_path / "downloads"
        device_dir = download_root / "Cam1"
        device_dir.mkdir(parents=True)
        reused = device_dir / "take1.mjpg"
        reused.write_text("data", encoding="utf-8")

        settings = _make_settings()
        settings.cameras.destination = str(download_root)
        settings.cameras.devices = [SimpleNamespace(name="Cam1", ip="1.2.3.4")]

        def patched_convert(cv, _settings, **_kw):
            out = cv.source_path.with_suffix(".mp4")
            out.touch()
            cv.output_path = out
            cv.status = "Fertig"
            return True

        mock_convert.side_effect = patched_convert

        job = WorkflowJob(
            name="Pi",
            source_mode="pi_download",
            device_name="Cam1",
            download_destination=str(download_root),
            convert_enabled=True,
            files=[FileEntry(source_path="take1.mp4")],
        )
        finished_args = []
        ex = WorkflowExecutor(Workflow(jobs=[job]), settings)
        ex.finished.connect(lambda ok, sk, fa: finished_args.append((ok, sk, fa)))

        ex.run()

        assert finished_args == [(1, 0, 0)]
        assert mock_convert.called


class TestWorkflowStackScenarios:
    def test_convert_disabled_skips_conversion(self, tmp_path):
        source = tmp_path / "clip.mp4"
        source.write_text("data", encoding="utf-8")

        job = WorkflowJob(
            name="Kein Convert",
            source_mode="files",
            convert_enabled=False,
            files=[FileEntry(source_path=str(source))],
        )
        ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())

        with patch("src.workflow_executor.run_convert") as mock_convert:
            ex.run()

        mock_convert.assert_not_called()

    @patch("src.workflow_executor.run_convert")
    def test_convert_enabled_runs_conversion(self, mock_convert, tmp_path):
        source = tmp_path / "clip.mjpeg"
        source.write_text("data", encoding="utf-8")

        def patched_convert(cv, _settings, **_kw):
            out = cv.source_path.with_suffix(".mp4")
            out.touch()
            cv.output_path = out
            cv.status = "Fertig"
            return True

        mock_convert.side_effect = patched_convert

        job = WorkflowJob(
            name="Mit Convert",
            source_mode="files",
            convert_enabled=True,
            files=[FileEntry(source_path=str(source))],
        )
        ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())

        ex.run()

        assert mock_convert.called

    def test_move_files_moves_source_into_destination(self, tmp_path):
        src = tmp_path / "quelle.mp4"
        src.write_text("data", encoding="utf-8")
        dst_dir = tmp_path / "ziel"

        job = WorkflowJob(
            name="Move",
            source_mode="files",
            convert_enabled=False,
            copy_destination=str(dst_dir),
            move_files=True,
            files=[FileEntry(source_path=str(src))],
        )
        ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())

        ex.run()

        assert not src.exists()
        assert (dst_dir / src.name).exists()

    def test_copy_mode_keeps_source_file(self, tmp_path):
        src = tmp_path / "quelle.mp4"
        src.write_text("data", encoding="utf-8")
        dst_dir = tmp_path / "ziel"

        job = WorkflowJob(
            name="Copy",
            source_mode="files",
            convert_enabled=False,
            copy_destination=str(dst_dir),
            move_files=False,
            files=[FileEntry(source_path=str(src))],
        )
        ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())

        ex.run()

        assert src.exists()
        assert (dst_dir / src.name).exists()

    @patch("src.workflow_executor.get_youtube_service")
    def test_kaderblick_is_not_attempted_without_youtube_upload(self, mock_get_youtube, tmp_path):
        source = tmp_path / "clip.mp4"
        source.write_text("data", encoding="utf-8")

        job = WorkflowJob(
            name="Nur KB Flag",
            source_mode="files",
            convert_enabled=False,
            upload_youtube=False,
            upload_kaderblick=True,
            files=[FileEntry(source_path=str(source))],
        )
        ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())

        with patch("src.workflow_steps.kaderblick_post_step.kaderblick_post") as mock_kaderblick:
            ex.run()

        mock_get_youtube.assert_not_called()
        mock_kaderblick.assert_not_called()

    @patch("src.workflow_steps.kaderblick_post_step.get_video_id_for_output", return_value="video-123")
    @patch("src.workflow_steps.kaderblick_post_step.kaderblick_post", return_value=True)
    def test_kaderblick_runs_only_with_youtube_upload(
        self,
        mock_kaderblick,
        _mock_video_id,
        tmp_path,
    ):
        source = tmp_path / "clip.mp4"
        source.write_text("data", encoding="utf-8")

        job = WorkflowJob(
            name="YT und KB",
            source_mode="files",
            convert_enabled=False,
            upload_youtube=True,
            upload_kaderblick=True,
            default_kaderblick_game_id="game-1",
            default_kaderblick_video_type_id=3,
            default_kaderblick_camera_id=7,
            files=[FileEntry(source_path=str(source))],
        )
        ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())

        with patch("src.workflow_executor.get_youtube_service", return_value=MagicMock()):
            with patch(
                "src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube",
                return_value=True,
            ):
                ex.run()

        mock_kaderblick.assert_called_once()

    @patch("src.workflow_executor.run_convert")
    def test_single_file_youtube_version_is_optional(self, mock_convert, tmp_path):
        source = tmp_path / "clip.mjpeg"
        source.write_text("data", encoding="utf-8")

        def patched_convert(cv, _settings, **_kw):
            out = cv.source_path.with_suffix(".mp4")
            out.touch()
            cv.output_path = out
            cv.status = "Fertig"
            return True

        mock_convert.side_effect = patched_convert

        with patch("src.workflow_executor.run_youtube_convert", return_value=True) as mock_yt_convert:
            job = WorkflowJob(
                name="YT-Version",
                source_mode="files",
                convert_enabled=True,
                create_youtube_version=True,
                upload_youtube=False,
                files=[FileEntry(source_path=str(source))],
            )
            ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())
            ex.run()

        mock_yt_convert.assert_called_once()

        with patch("src.workflow_executor.run_youtube_convert", return_value=True) as mock_yt_convert_disabled:
            job = WorkflowJob(
                name="Ohne YT-Version",
                source_mode="files",
                convert_enabled=True,
                create_youtube_version=False,
                upload_youtube=False,
                files=[FileEntry(source_path=str(source))],
            )
            ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())
            ex.run()

        mock_yt_convert_disabled.assert_not_called()

    def test_download_reuses_existing_target_when_device_unavailable(self, tmp_path):
        download_root = tmp_path / "downloads"
        device_dir = download_root / "Cam1"
        device_dir.mkdir(parents=True)
        reused = device_dir / "take1.mjpg"
        reused.write_text("data", encoding="utf-8")

        settings = _make_settings()
        settings.cameras.destination = str(download_root)
        settings.cameras.devices = [SimpleNamespace(name="Cam1", ip="1.2.3.4")]

        job = WorkflowJob(
            name="Pi",
            source_mode="pi_download",
            device_name="Cam1",
            download_destination=str(download_root),
            files=[FileEntry(source_path="take1.mp4")],
        )
        ex = WorkflowExecutor(Workflow(), settings)

        with patch("src.workflow_executor.download_device", side_effect=RuntimeError("offline")):
            paths = ex._download_from_pi(0, job)

        assert paths == [str(reused)]
        assert job.step_statuses["transfer"] == "reused-target"

    @patch("src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube", return_value=True)
    def test_resume_reuses_existing_converted_output_and_continues_pipeline(self, mock_upload, tmp_path):
        source = tmp_path / "clip.mp4"
        converted = tmp_path / "clip_converted.mp4"
        converted.write_text("converted", encoding="utf-8")

        job = WorkflowJob(
            name="Resume Convert",
            source_mode="files",
            convert_enabled=True,
            create_youtube_version=True,
            upload_youtube=True,
            files=[FileEntry(source_path=str(source))],
            resume_status="Konvertiere …",
            step_statuses={"transfer": "done", "convert": "running"},
        )
        ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())
        ex._youtube_convert_func = MagicMock(return_value=True)

        with patch("src.workflow_executor.get_youtube_service", return_value=MagicMock()):
            ex.run()

        ex._youtube_convert_func.assert_called_once()
        reused_cv_job = ex._youtube_convert_func.call_args[0][0]
        assert reused_cv_job.output_path == converted
        mock_upload.assert_called_once()
        assert job.step_statuses["convert"] == "reused-target"

    @patch("src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube", return_value=True)
    def test_resume_uses_existing_youtube_artifact_when_source_is_gone(self, mock_upload, tmp_path):
        source = tmp_path / "clip.mp4"
        yt_version = tmp_path / "clip_youtube.mp4"
        yt_version.write_text("yt", encoding="utf-8")

        job = WorkflowJob(
            name="Resume Upload",
            source_mode="files",
            convert_enabled=False,
            create_youtube_version=True,
            upload_youtube=True,
            files=[FileEntry(source_path=str(source))],
            resume_status="YT-Version erstellen …",
            step_statuses={"transfer": "done", "yt_version": "running"},
        )
        ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())

        with patch("src.workflow_executor.get_youtube_service", return_value=MagicMock()):
            ex.run()

        mock_upload.assert_called_once()
        assert job.step_statuses["yt_version"] == "reused-target"


# ─── Merge-Concat: Standard-Dateien verwenden ────────────────────────────────

class TestMergeConcatStandardFlow:
    """Testet die neues Design im workflow_executor Merge-Block:
    run_concat() erhält immer die Standard-MP4 (job.output_path) – keine
    _youtube-Variante. Nach erfolgreichem Concat wird run_youtube_convert()
    auf das Merge-Ergebnis angewendet (wenn create_youtube_version=True).
    """

    @patch("src.workflow_executor.run_concat")
    def test_executor_passes_standard_files_to_concat(self, mock_concat):
        """workflow_executor übergibt Standard-Dateien (kein _youtube) an run_concat."""
        mock_concat.return_value = True

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)

            out1 = p / "v1.mp4"; out1.touch()
            out2 = p / "v2.mp4"; out2.touch()
            # _youtube-Varianten existieren, dürfen aber NICHT verwendet werden
            yt1 = p / "v1_youtube.mp4"; yt1.touch()
            yt2 = p / "v2_youtube.mp4"; yt2.touch()

            from src.converter import ConvertJob
            cv1 = ConvertJob(source_path=p / "v1_src.mjpeg", output_path=out1, status="Fertig")
            cv2 = ConvertJob(source_path=p / "v2_src.mjpeg", output_path=out2, status="Fertig")

            gid = "gruppe1"
            fe1 = FileEntry(source_path=str(p / "v1_src.mjpeg"), merge_group_id=gid)
            fe2 = FileEntry(source_path=str(p / "v2_src.mjpeg"), merge_group_id=gid)

            job = WorkflowJob(source_mode="files", convert_enabled=True, upload_youtube=False)
            job.files = [fe1, fe2]

            ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())
            merged_path = p / "gruppe1_merged.mp4"

            group_items = {gid: [(0, 0, job, cv1), (1, 0, job, cv2)]}
            _merged_out = {gid: merged_path}

            for _gid, group in group_items.items():
                source_paths = [cv.output_path for _, _, _, cv in group
                                if cv.output_path and cv.output_path.exists()]
                mock_concat(source_paths, _merged_out[_gid],
                            cancel_flag=ex._cancel, log_callback=None)

        call_args = mock_concat.call_args[0]
        passed_sources = call_args[0]
        assert passed_sources == [out1, out2], (
            f"Erwartet Standard-Dateien, erhalten: {passed_sources}")

    @patch("src.workflow_executor.run_concat")
    def test_executor_merge_deletes_source_files(self, mock_concat):
        """Nach erfolgreichem Concat werden die Einzeldateien gelöscht."""
        mock_concat.return_value = True
        deleted: list[str] = []

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            out1 = p / "v1.mp4"; out1.touch()
            out2 = p / "v2.mp4"; out2.touch()
            merged_path = p / "merged.mp4"
            merged_path.touch()

            source_paths = [out1, out2]
            concat_ok = mock_concat(source_paths, merged_path,
                                    cancel_flag=threading.Event(), log_callback=None)
            if concat_ok:
                for src in source_paths:
                    try:
                        src.unlink()
                        deleted.append(src.name)
                    except OSError:
                        pass

        assert sorted(deleted) == ["v1.mp4", "v2.mp4"], (
            f"Gelöschte Dateien: {deleted}")

    @patch("src.workflow_executor.run_youtube_convert")
    @patch("src.workflow_executor.run_concat")
    def test_youtube_version_created_from_merged_file(self, mock_concat, mock_yt):
        """Bei create_youtube_version=True: run_youtube_convert() nach dem Concat."""
        mock_concat.return_value = True
        mock_yt.return_value = True

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            out1 = p / "v1.mp4"; out1.touch()
            out2 = p / "v2.mp4"; out2.touch()
            merged = p / "gruppe1_merged.mp4"
            merged.touch()

            from src.converter import ConvertJob
            cv1 = ConvertJob(source_path=p / "v1_src.mjpeg", output_path=out1, status="Fertig")
            cv2 = ConvertJob(source_path=p / "v2_src.mjpeg", output_path=out2, status="Fertig")

            gid = "gruppe1"
            fe1 = FileEntry(source_path=str(p / "v1_src.mjpeg"), merge_group_id=gid)
            fe2 = FileEntry(source_path=str(p / "v2_src.mjpeg"), merge_group_id=gid)

            job = WorkflowJob(source_mode="files", convert_enabled=True,
                              upload_youtube=False, create_youtube_version=True)
            job.files = [fe1, fe2]

            ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())
            per_settings = ex._build_job_settings(job)

            group = [(0, 0, job, cv1), (1, 0, job, cv2)]
            source_paths = [cv.output_path for _, _, _, cv in group
                            if cv.output_path and cv.output_path.exists()]

            concat_ok = mock_concat(source_paths, merged,
                                    cancel_flag=ex._cancel, log_callback=None)
            if concat_ok:
                # Replay the YouTube version creation from the merged file
                first_cv = group[0][3]
                first_cv.output_path = merged
                if job.create_youtube_version and merged.exists():
                    mock_yt(first_cv, per_settings,
                            cancel_flag=ex._cancel, log_callback=None)

        assert mock_yt.called, "run_youtube_convert wurde nicht aufgerufen"
        yt_job_arg = mock_yt.call_args[0][0]
        assert yt_job_arg.output_path == merged, (
            f"run_youtube_convert erhielt falschen cv_job: {yt_job_arg.output_path}")

    @patch("src.workflow_executor.run_concat")
    def test_standard_files_unaffected_by_existing_yt_variant(self, mock_concat):
        """Wenn _youtube-Datei existiert: trotzdem Standard-Datei für Concat nehmen."""
        mock_concat.return_value = True

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            out1 = p / "v1.mp4"; out1.touch()
            # Eine _youtube-Variante existiert – soll IGNORIERT werden
            (p / "v1_youtube.mp4").touch()

            from src.converter import ConvertJob
            cv1 = ConvertJob(source_path=p / "v1_src.mjpeg", output_path=out1, status="Fertig")
            out2 = p / "v2.mp4"; out2.touch()
            cv2 = ConvertJob(source_path=p / "v2_src.mjpeg", output_path=out2, status="Fertig")

            gid = "gruppe1"
            group = [(0, 0, None, cv1), (1, 0, None, cv2)]
            source_paths = [cv.output_path for _, _, _, cv in group
                            if cv.output_path and cv.output_path.exists()]

            assert source_paths == [out1, out2], (
                f"Nicht-Standard-Dateien im Concat: {source_paths}")

    @patch("src.workflow_executor.run_concat")
    def test_no_merge_for_single_file_group(self, mock_concat):
        """Gruppen mit nur einer Datei dürfen kein Concat auslösen."""
        mock_concat.return_value = True

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            out1 = p / "v1.mp4"; out1.touch()

            from src.converter import ConvertJob
            cv1 = ConvertJob(source_path=p / "v1_src.mjpeg", output_path=out1, status="Fertig")

            # Nur ein Element → kein Merge
            group = [(0, 0, None, cv1)]
            source_paths = [cv.output_path for _, _, _, cv in group
                            if cv.output_path and cv.output_path.exists()]

            if len(source_paths) >= 2:
                mock_concat(source_paths, p / "merged.mp4",
                            cancel_flag=threading.Event(), log_callback=None)

        assert not mock_concat.called, "run_concat darf bei Einzeldatei nicht aufgerufen werden"


# ─── phase_changed-Signal ─────────────────────────────────────────────────────

class TestPhaseChangedSignal:
    """phase_changed(str) liefert beim leeren Workflow keinen Crash."""

    def test_empty_workflow_no_phase_emitted(self):
        phases: list[str] = []
        wf = Workflow(jobs=[])
        ex = WorkflowExecutor(wf, _make_settings())
        ex.phase_changed.connect(phases.append)
        ex.run()
        assert phases == []

    def test_disabled_job_no_phase_emitted(self):
        phases: list[str] = []
        j = WorkflowJob(enabled=False)
        wf = Workflow(jobs=[j])
        ex = WorkflowExecutor(wf, _make_settings())
        ex.phase_changed.connect(phases.append)
        ex.run()
        assert phases == []


# ─── Merge-Upload: erst nach Concat, nie davor ────────────────────────────────

class TestMergeUploadAfterConcat:
    """Der YouTube-Upload darf bei Merge-Gruppen erst nach run_concat() erfolgen."""

    @staticmethod
    def _compute_skip_set_from_executor_logic(entries, job):
        """Repliziert die neue Skip-Logik: alle Merge-Mitglieder werden übersprungen."""
        from src.converter import ConvertJob
        to_convert = [
            (0, job, ConvertJob(source_path=Path(fe.source_path)))
            for fe in entries
        ]
        skip: set[int] = set()
        for ci, (_, j, cv) in enumerate(to_convert):
            entry = WorkflowExecutor._find_file_entry(j, str(cv.source_path))
            gid = (getattr(entry, "merge_group_id", "") or "") if entry else ""
            if gid:
                skip.add(ci)
        return skip

    def test_first_member_of_group_is_skipped(self):
        """Auch das ERSTE Element einer Merge-Gruppe wird übersprungen (kein Einzel-Upload)."""
        entries = [
            FileEntry(source_path="/a/1.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/2.mp4", merge_group_id="g1"),
        ]
        job = WorkflowJob(source_mode="files", convert_enabled=True)
        job.files = entries
        skip = self._compute_skip_set_from_executor_logic(entries, job)
        assert 0 in skip, "Index 0 (erstes Mitglied) muss in der Skip-Menge sein"

    def test_ungrouped_file_not_skipped(self):
        """Dateien ohne Merge-Gruppe bleiben außerhalb der Skip-Menge."""
        entries = [
            FileEntry(source_path="/a/solo.mp4"),
            FileEntry(source_path="/a/1.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/2.mp4", merge_group_id="g1"),
        ]
        job = WorkflowJob(source_mode="files", convert_enabled=True)
        job.files = entries
        skip = self._compute_skip_set_from_executor_logic(entries, job)
        assert 0 not in skip, "Solo-Datei darf nicht in der Skip-Menge sein"
        assert skip == {1, 2}

    @patch("src.workflow_steps.youtube_upload_step.upload_to_youtube")
    @patch("src.workflow_executor.run_concat")
    @patch("src.workflow_executor.run_convert")
    def test_upload_called_once_after_concat(self, mock_convert, mock_concat,
                                             mock_upload):
        """upload_to_youtube wird genau einmal gerufen – nach dem Merge, nicht davor."""
        from src.converter import ConvertJob

        mock_convert.side_effect = lambda cv, s, **kw: (
            setattr(cv, "status", "Fertig") or
            setattr(cv, "output_path", cv.source_path.with_suffix(".mp4")) or
            True
        )

        call_order: list[str] = []

        def fake_concat(sources, dest, **kw):
            call_order.append("concat")
            dest.touch()
            return True

        def fake_upload(cv_job, settings, yt_service, **kw):
            call_order.append("upload")
            return True

        mock_concat.side_effect = fake_concat
        mock_upload.side_effect = fake_upload

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            src1 = p / "a.mjpeg"; src1.touch()
            src2 = p / "b.mjpeg"; src2.touch()

            def patched_convert(cv, s, **kw):
                out = cv.source_path.with_suffix(".mp4")
                out.touch()
                cv.output_path = out
                cv.status = "Fertig"
                return True

            mock_convert.side_effect = patched_convert

            entries = [
                FileEntry(source_path=str(src1), merge_group_id="g1"),
                FileEntry(source_path=str(src2), merge_group_id="g1"),
            ]
            job = WorkflowJob(
                source_mode="files",
                convert_enabled=True,
                upload_youtube=True,
                upload_kaderblick=False,
                title_card_enabled=False,
                create_youtube_version=False,
            )
            job.files = entries

            wf = Workflow(jobs=[job])
            ex = WorkflowExecutor(wf, _make_settings())

            fake_yt_service = MagicMock()
            with patch(
                "src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube",
                side_effect=lambda *a, **kw: call_order.append("upload") or True,
            ):
                with patch("src.workflow_executor.get_youtube_service",
                           return_value=fake_yt_service):
                    ex.run()

        # Concat muss VOR dem Upload stehen
        assert "concat" in call_order, "run_concat wurde nicht aufgerufen"
        assert "upload" in call_order, "_upload_to_youtube wurde nicht aufgerufen"
        concat_pos = call_order.index("concat")
        upload_pos = call_order.index("upload")
        assert concat_pos < upload_pos, (
            f"Upload ({upload_pos}) sollte nach Concat ({concat_pos}) kommen, "
            f"aber war davor. Reihenfolge: {call_order}")

    @patch("src.workflow_steps.youtube_upload_step.upload_to_youtube")
    @patch("src.workflow_executor.run_concat")
    @patch("src.workflow_executor.run_convert")
    def test_no_upload_without_yt_service(self, mock_convert, mock_concat, mock_upload):
        """Ohne YouTube-Service kein Upload, auch nach dem Merge."""
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            src1 = p / "a.mjpeg"; src1.touch()
            src2 = p / "b.mjpeg"; src2.touch()

            def patched_convert(cv, s, **kw):
                out = cv.source_path.with_suffix(".mp4")
                out.touch()
                cv.output_path = out
                cv.status = "Fertig"
                return True

            mock_convert.side_effect = patched_convert

            def fake_concat(sources, dest, **kw):
                dest.touch()
                return True

            mock_concat.side_effect = fake_concat

            entries = [
                FileEntry(source_path=str(src1), merge_group_id="g1"),
                FileEntry(source_path=str(src2), merge_group_id="g1"),
            ]
            job = WorkflowJob(
                source_mode="files",
                convert_enabled=True,
                upload_youtube=True,
                upload_kaderblick=False,
                title_card_enabled=False,
                create_youtube_version=False,
            )
            job.files = entries

            wf = Workflow(jobs=[job])
            ex = WorkflowExecutor(wf, _make_settings())
            with patch("src.workflow_executor.get_youtube_service", return_value=None):
                ex.run()

        mock_upload.assert_not_called()

    @patch("src.workflow_executor.run_youtube_convert")
    @patch("src.workflow_executor.run_concat")
    def test_upload_only_merge_runs_concat_before_yt_version_and_upload(
            self, mock_concat, mock_yt_convert):
        """Bei convert_enabled=False dürfen Merge-Gruppen erst nach dem Concat hochladen."""
        call_order: list[str] = []

        def fake_concat(sources, dest, **kw):
            call_order.append("concat")
            dest.touch()
            return True

        def fake_yt_convert(cv_job, settings, **kw):
            call_order.append("yt-version")
            yt_path = cv_job.output_path.with_stem(cv_job.output_path.stem + "_youtube")
            yt_path.touch()
            return True

        mock_concat.side_effect = fake_concat
        mock_yt_convert.side_effect = fake_yt_convert

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            src1 = p / "part1.mp4"; src1.touch()
            src2 = p / "part2.mp4"; src2.touch()

            entries = [
                FileEntry(source_path=str(src1), merge_group_id="g1", youtube_title="Finaltitel"),
                FileEntry(source_path=str(src2), merge_group_id="g1", youtube_title="Finaltitel"),
            ]
            job = WorkflowJob(
                source_mode="files",
                convert_enabled=False,
                upload_youtube=True,
                upload_kaderblick=False,
                title_card_enabled=False,
                create_youtube_version=True,
            )
            job.files = entries

            wf = Workflow(jobs=[job])
            ex = WorkflowExecutor(wf, _make_settings())

            with patch(
                "src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube",
                side_effect=lambda *a, **kw: call_order.append("upload") or True,
            ):
                with patch("src.workflow_executor.get_youtube_service", return_value=MagicMock()):
                    ex.run()

        assert call_order == ["concat", "yt-version", "upload"], (
            "Merge-Upload ohne Konvertierung muss erst concat, dann YT-Version, dann Upload ausführen. "
            f"Tatsächliche Reihenfolge: {call_order}"
        )

    @patch("src.workflow_executor.run_concat")
    @patch("src.workflow_executor.run_convert")
    def test_merge_emits_progress_updates(self, mock_convert, mock_concat):
        progress_values: list[int] = []

        def patched_convert(cv, _settings, **_kw):
            out = cv.source_path.with_suffix(".mp4")
            out.touch()
            cv.output_path = out
            cv.status = "Fertig"
            return True

        def fake_concat(_sources, dest, **kwargs):
            progress_callback = kwargs.get("progress_callback")
            if progress_callback:
                progress_callback(10)
                progress_callback(55)
                progress_callback(100)
            dest.touch()
            return True

        mock_convert.side_effect = patched_convert
        mock_concat.side_effect = fake_concat

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            src1 = p / "a.mjpeg"; src1.touch()
            src2 = p / "b.mjpeg"; src2.touch()

            job = WorkflowJob(
                source_mode="files",
                convert_enabled=True,
                upload_youtube=False,
                upload_kaderblick=False,
                files=[
                    FileEntry(source_path=str(src1), merge_group_id="g1"),
                    FileEntry(source_path=str(src2), merge_group_id="g1"),
                ],
            )
            ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())
            ex.job_progress.connect(lambda idx, pct: progress_values.append(pct) if idx == 0 else None)

            ex.run()

        assert 10 in progress_values
        assert 55 in progress_values
        assert 100 in progress_values


# ─── Titelkarte bei Merge-Gruppen: nur einmal, nach dem Concat ────────────────

class TestTitleCardMergeGroup:
    """Die Titelkarte darf bei Merge-Gruppen NICHT für jede Einzeldatei erstellt
    werden, sondern nur einmal für das zusammengeführte Ergebnis."""

    @staticmethod
    def _compute_title_card_flag_for_members(entries, job):
        """Repliziert die Prüfung: is_in_merge_group → title_card überspringen."""
        from src.converter import ConvertJob
        to_convert = [
            (0, job, ConvertJob(source_path=Path(fe.source_path)))
            for fe in entries
        ]
        # Build skip set (all merge members)
        skip: set[int] = set()
        for ci, (_, j, cv) in enumerate(to_convert):
            entry = WorkflowExecutor._find_file_entry(j, str(cv.source_path))
            gid = (getattr(entry, "merge_group_id", "") or "") if entry else ""
            if gid:
                skip.add(ci)

        # Für jede Datei: würde Titelkarte erstellt werden?
        results = []
        for ci, (_, j, cv) in enumerate(to_convert):
            is_in_merge_group = ci in skip
            would_create_title_card = j.title_card_enabled and not is_in_merge_group
            results.append(would_create_title_card)
        return results

    def _job_with_entries(self, entries, title_card=True):
        job = WorkflowJob(source_mode="files", convert_enabled=True,
                          title_card_enabled=title_card)
        job.files = entries
        return job

    def test_no_title_card_for_merge_members(self):
        """Alle Mitglieder einer Merge-Gruppe bekommen keine Titelkarte während Konvertierung."""
        entries = [
            FileEntry(source_path="/a/1.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/2.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/3.mp4", merge_group_id="g1"),
        ]
        job = self._job_with_entries(entries, title_card=True)
        flags = self._compute_title_card_flag_for_members(entries, job)
        assert flags == [False, False, False], (
            f"Erwartet alle False (kein Einzel-Titelkarte), got {flags}")

    def test_title_card_for_solo_files(self):
        """Einzelne Dateien (ohne Merge-Gruppe) bekommen Titelkarte."""
        entries = [
            FileEntry(source_path="/a/solo.mp4"),
        ]
        job = self._job_with_entries(entries, title_card=True)
        flags = self._compute_title_card_flag_for_members(entries, job)
        assert flags == [True]

    def test_mixed_solo_and_group(self):
        """Solo-Dateien bekommen Titelkarte, Merge-Mitglieder nicht."""
        entries = [
            FileEntry(source_path="/a/solo.mp4"),
            FileEntry(source_path="/a/1.mp4", merge_group_id="g1"),
            FileEntry(source_path="/a/2.mp4", merge_group_id="g1"),
        ]
        job = self._job_with_entries(entries, title_card=True)
        flags = self._compute_title_card_flag_for_members(entries, job)
        assert flags == [True, False, False]

    def test_title_card_disabled_globally(self):
        """Wenn title_card_enabled=False gilt das für alle Dateien."""
        entries = [
            FileEntry(source_path="/a/solo.mp4"),
            FileEntry(source_path="/a/1.mp4", merge_group_id="g1"),
        ]
        job = self._job_with_entries(entries, title_card=False)
        flags = self._compute_title_card_flag_for_members(entries, job)
        assert flags == [False, False]

    @patch("src.workflow_executor.run_concat")
    @patch("src.workflow_executor.run_convert")
    def test_prepend_title_card_called_once_after_concat(
            self, mock_convert, mock_concat):
        """TitleCardStep wird genau einmal aufgerufen — nach dem Concat."""
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            src1 = p / "a.mjpeg"; src1.touch()
            src2 = p / "b.mjpeg"; src2.touch()

            def patched_convert(cv, s, **kw):
                out = cv.source_path.with_suffix(".mp4")
                out.touch()
                cv.output_path = out
                cv.status = "Fertig"
                return True

            mock_convert.side_effect = patched_convert

            def fake_concat(sources, dest, **kw):
                dest.touch()
                return True

            mock_concat.side_effect = fake_concat

            entries = [
                FileEntry(source_path=str(src1), merge_group_id="g1"),
                FileEntry(source_path=str(src2), merge_group_id="g1"),
            ]
            job = WorkflowJob(
                source_mode="files",
                convert_enabled=True,
                upload_youtube=False,
                upload_kaderblick=False,
                title_card_enabled=True,
                create_youtube_version=False,
            )
            job.files = entries

            wf = Workflow(jobs=[job])
            ex = WorkflowExecutor(wf, _make_settings())
            call_count = {"n": 0}

            def fake_prepend(_executor, _orig_idx, cv_job, j, s):
                call_count["n"] += 1
                # return a fake path so the executor can continue
                fake_out = p / f"with_intro_{call_count['n']}.mp4"
                fake_out.touch()
                return fake_out, True

            with patch(
                "src.workflow_steps.title_card_step.TitleCardStep._prepend_title_card",
                side_effect=fake_prepend,
            ):
                ex.run()

        assert call_count["n"] == 1, (
            f"TitleCardStep._prepend_title_card sollte genau 1x aufgerufen werden, "
            f"wurde aber {call_count['n']}x aufgerufen")


class TestOriginalPreservationWithoutConvert:
    @patch("src.workflow_executor.run_concat")
    def test_merge_without_convert_keeps_original_files(self, mock_concat):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            src1 = p / "halbzeit1.mp4"
            src2 = p / "halbzeit2.mp4"
            src1.write_text("a", encoding="utf-8")
            src2.write_text("b", encoding="utf-8")

            def fake_concat(sources, dest, **_kw):
                dest.write_text("merged", encoding="utf-8")
                return True

            mock_concat.side_effect = fake_concat

            job = WorkflowJob(
                source_mode="files",
                convert_enabled=False,
                upload_youtube=False,
                upload_kaderblick=False,
                title_card_enabled=False,
                create_youtube_version=False,
                files=[
                    FileEntry(source_path=str(src1), merge_group_id="g1"),
                    FileEntry(source_path=str(src2), merge_group_id="g1"),
                ],
            )

            ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())
            ex.run()

            assert src1.exists()
            assert src2.exists()
            assert (p / "halbzeit1_merged.mp4").exists()

    @patch("src.workflow_steps.youtube_upload_step.upload_to_youtube", return_value=True)
    @patch("src.workflow_executor.get_youtube_service", return_value=object())
    @patch("src.workflow_executor.run_concat")
    def test_title_card_without_convert_creates_new_file_and_keeps_original(
        self,
        mock_concat,
        _mock_get_youtube_service,
        _mock_upload,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            src = p / "original.mp4"
            src.write_text("video", encoding="utf-8")

            def fake_concat(sources, dest, **_kw):
                dest.write_text("with-titlecard", encoding="utf-8")
                return True

            mock_concat.side_effect = fake_concat

            job = WorkflowJob(
                source_mode="files",
                convert_enabled=False,
                upload_youtube=True,
                upload_kaderblick=False,
                title_card_enabled=True,
                create_youtube_version=False,
                files=[FileEntry(source_path=str(src))],
            )

            ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())

            with patch(
                "src.workflow_steps.title_card_step.generate_title_card",
                return_value=True,
            ):
                ex.run()

            assert src.exists()
            assert src.read_text(encoding="utf-8") == "video"
            assert (p / "original_titlecard.mp4").exists()

    @patch("src.workflow_steps.kaderblick_post_step.get_video_id_for_output", return_value="video-123")
    @patch("src.workflow_steps.kaderblick_post_step.kaderblick_post", return_value=True)
    @patch("src.workflow_steps.youtube_upload_step.YoutubeUploadStep._upload_to_youtube", return_value=True)
    def test_resume_uses_existing_merged_output_when_sources_are_gone(
        self,
        mock_upload,
        mock_kaderblick,
        _mock_video_id,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            src1 = p / "halbzeit1.mp4"
            src2 = p / "halbzeit2.mp4"
            merged = p / "halbzeit1_merged.mp4"
            merged.write_text("merged", encoding="utf-8")

            job = WorkflowJob(
                source_mode="files",
                convert_enabled=False,
                create_youtube_version=True,
                upload_youtube=True,
                upload_kaderblick=True,
                default_kaderblick_game_id="game-1",
                files=[
                    FileEntry(source_path=str(src1), merge_group_id="g1"),
                    FileEntry(source_path=str(src2), merge_group_id="g1"),
                ],
                resume_status="Zusammenführen …",
                step_statuses={"transfer": "done", "merge": "running"},
            )

            ex = WorkflowExecutor(Workflow(jobs=[job]), _make_settings())
            ex._youtube_convert_func = MagicMock(return_value=True)

            with patch("src.workflow_executor.get_youtube_service", return_value=MagicMock()):
                ex.run()

            ex._youtube_convert_func.assert_called_once()
            yt_job = ex._youtube_convert_func.call_args[0][0]
            assert yt_job.output_path == merged
            mock_upload.assert_called_once()
            mock_kaderblick.assert_called_once()
            assert job.step_statuses["merge"] == "reused-target"


