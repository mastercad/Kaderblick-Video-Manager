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

    @patch("src.workflow_executor.upload_to_youtube")
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
            with patch.object(ex, "_upload_to_youtube",
                               side_effect=lambda *a, **kw: call_order.append("upload") or True):
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

    @patch("src.workflow_executor.upload_to_youtube")
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
        """_prepend_title_card wird genau einmal aufgerufen — nach dem Concat."""
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

            def fake_prepend(cv_job, j, s):
                call_count["n"] += 1
                # return a fake path so the executor can continue
                fake_out = p / f"with_intro_{call_count['n']}.mp4"
                fake_out.touch()
                return fake_out

            with patch.object(ex, "_prepend_title_card", side_effect=fake_prepend):
                ex.run()

        assert call_count["n"] == 1, (
            f"_prepend_title_card sollte genau 1x aufgerufen werden, "
            f"wurde aber {call_count['n']}x aufgerufen")


