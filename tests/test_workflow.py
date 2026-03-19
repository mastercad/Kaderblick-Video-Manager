"""Tests für das Workflow-Datenmodell (workflow.py).

Geprüft:
- FileEntry Standardwerte und merge_group_id-Feld
- WorkflowJob Serialisierung (to_dict / from_dict, Roundtrip)
- Workflow Serialisierung + Persistenz (save/load)
- Migration alter WorkflowSource-Formate in das neue WorkflowJob-Format
"""

import json
import tempfile
from dataclasses import asdict
from pathlib import Path

import pytest

# --- Modulimport (kein Qt nötig) ---
from src.workflow import FileEntry, WorkflowJob, Workflow, _migrate_source_to_job


# ─── FileEntry ────────────────────────────────────────────────────────────────

class TestFileEntry:
    def test_defaults(self):
        fe = FileEntry()
        assert fe.source_path == ""
        assert fe.output_filename == ""
        assert fe.youtube_title == ""
        assert fe.youtube_description == ""
        assert fe.youtube_playlist == ""
        assert fe.kaderblick_game_id == ""
        assert fe.kaderblick_game_start == 0
        assert fe.kaderblick_video_type_id == 0
        assert fe.kaderblick_camera_id == 0
        assert fe.merge_group_id == ""  # neu hinzugefügtes Feld

    def test_merge_group_id_is_set(self):
        fe = FileEntry(source_path="/foo/bar.mp4", merge_group_id="grp-abc")
        assert fe.merge_group_id == "grp-abc"

    def test_all_fields_survive_asdict(self):
        fe = FileEntry(
            source_path="/a/b.mp4",
            output_filename="b_out.mp4",
            youtube_title="Titel",
            youtube_description="Beschr.",
            youtube_playlist="Playlist",
            kaderblick_game_id="99",
            kaderblick_game_start=10,
            kaderblick_video_type_id=2,
            kaderblick_camera_id=3,
            merge_group_id="mg1",
        )
        d = asdict(fe)
        assert d["merge_group_id"] == "mg1"
        assert d["kaderblick_camera_id"] == 3


# ─── WorkflowJob ─────────────────────────────────────────────────────────────

class TestWorkflowJob:
    def test_id_is_generated(self):
        j1 = WorkflowJob()
        j2 = WorkflowJob()
        assert j1.id != j2.id
        assert len(j1.id) == 8

    def test_defaults(self):
        job = WorkflowJob()
        assert job.source_mode == "files"
        assert job.convert_enabled is True
        assert job.encoder == "auto"
        assert job.crf == 18
        assert job.fps == 25
        assert job.output_format == "mp4"
        assert job.upload_youtube is False
        assert job.upload_kaderblick is False
        assert job.merge_audio is False

    def test_to_dict_excludes_runtime_fields(self):
        job = WorkflowJob()
        job.status = "Läuft"
        job.progress_pct = 42
        job.error_msg = "test"
        d = job.to_dict()
        assert "status" not in d
        assert "progress_pct" not in d
        assert "error_msg" not in d

    def test_to_dict_contains_core_fields(self):
        job = WorkflowJob(name="Test", encoder="libx264", crf=22)
        d = job.to_dict()
        assert d["name"] == "Test"
        assert d["encoder"] == "libx264"
        assert d["crf"] == 22
        assert "source_mode" in d
        assert "files" in d

    def test_roundtrip_empty(self):
        job = WorkflowJob()
        restored = WorkflowJob.from_dict(job.to_dict())
        assert restored.id == job.id
        assert restored.source_mode == "files"
        assert restored.files == []

    def test_roundtrip_with_files(self):
        fe = FileEntry(source_path="/a/b.mp4", merge_group_id="grp1")
        job = WorkflowJob(
            name="Mit Dateien",
            source_mode="files",
            files=[fe],
            upload_youtube=True,
            default_youtube_playlist="Meine Playlist",
        )
        d = job.to_dict()
        restored = WorkflowJob.from_dict(d)
        assert restored.name == "Mit Dateien"
        assert restored.upload_youtube is True
        assert restored.default_youtube_playlist == "Meine Playlist"
        assert len(restored.files) == 1
        assert restored.files[0].source_path == "/a/b.mp4"
        assert restored.files[0].merge_group_id == "grp1"

    def test_roundtrip_pi_mode(self):
        job = WorkflowJob(
            source_mode="pi_download",
            device_name="Pi-Links",
            download_destination="/mnt/footage",
            delete_after_download=True,
        )
        restored = WorkflowJob.from_dict(job.to_dict())
        assert restored.source_mode == "pi_download"
        assert restored.device_name == "Pi-Links"
        assert restored.download_destination == "/mnt/footage"
        assert restored.delete_after_download is True

    def test_from_dict_ignores_unknown_keys(self):
        job = WorkflowJob()
        d = job.to_dict()
        d["nonexistent_field"] = "garbage"
        restored = WorkflowJob.from_dict(d)
        assert not hasattr(restored, "nonexistent_field")

    def test_from_dict_ignores_runtime_fields_even_if_present(self):
        job = WorkflowJob()
        d = job.to_dict()
        d["status"] = "Läuft"   # wurde von to_dict entfernt → manuell einfügen
        restored = WorkflowJob.from_dict(d)
        # Laufzeitfeld soll nicht zurückgeschrieben werden
        assert restored.status == "Wartend"  # Standardwert


# ─── Workflow ─────────────────────────────────────────────────────────────────

class TestWorkflow:
    def _make_workflow(self) -> Workflow:
        j1 = WorkflowJob(name="Job A")
        j2 = WorkflowJob(name="Job B", upload_youtube=True)
        return Workflow(name="Test-WF", jobs=[j1, j2])

    def test_to_dict_structure(self):
        wf = self._make_workflow()
        d = wf.to_dict()
        assert d["name"] == "Test-WF"
        assert isinstance(d["jobs"], list)
        assert len(d["jobs"]) == 2
        assert d["jobs"][0]["name"] == "Job A"

    def test_roundtrip(self):
        wf = self._make_workflow()
        restored = Workflow.from_dict(wf.to_dict())
        assert restored.name == "Test-WF"
        assert len(restored.jobs) == 2
        assert restored.jobs[1].upload_youtube is True

    def test_save_and_load(self):
        wf = self._make_workflow()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "workflow.json"
            wf.save(path)
            assert path.exists()
            loaded = Workflow.load(path)
        assert loaded.name == "Test-WF"
        assert len(loaded.jobs) == 2

    def test_save_creates_parent_dirs(self):
        wf = self._make_workflow()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sub" / "dir" / "wf.json"
            wf.save(path)
            assert path.exists()
            loaded = Workflow.load(path)
        assert len(loaded.jobs) == 2

    def test_shutdown_after_defaults_false(self):
        wf = Workflow()
        assert wf.shutdown_after is False
        d = wf.to_dict()
        assert d["shutdown_after"] is False

    def test_json_valid_utf8(self):
        wf = Workflow(name="Umlaut: äöü ß", jobs=[
            WorkflowJob(name="Auftrag: Þórsmörk")
        ])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "wf.json"
            wf.save(path)
            raw = path.read_text(encoding="utf-8")
            # ensure_ascii=False → Umlaute werden direkt gespeichert
            assert "äöü" in raw
            assert "Þórsmörk" in raw


# ─── Migration ────────────────────────────────────────────────────────────────

class TestMigration:
    """Alte WorkflowSource-Formate müssen nahtlos migriert werden."""

    def _old_source_pi(self) -> dict:
        return {
            "id": "aa001122",
            "enabled": True,
            "name": "Kamera Links",
            "source_type": "pi_camera",
            "source_path": "",
            "device_name": "pi-links",
            "destination_path": "/footage",
            "delete_source": True,
            "encoder": "libx264",
            "crf": 20,
            "preset": "fast",
            "fps": 30,
            "output_format": "mp4",
            "merge_audio_video": True,
            "upload_youtube": True,
            "youtube_title": "Test Titel",
            "youtube_playlist": "Meine PL",
        }

    def test_pi_migration_mode(self):
        job_dict = _migrate_source_to_job(self._old_source_pi())
        assert job_dict["source_mode"] == "pi_download"
        assert job_dict["device_name"] == "pi-links"
        assert job_dict["download_destination"] == "/footage"
        assert job_dict["delete_after_download"] is True

    def test_pi_migration_encoding(self):
        job_dict = _migrate_source_to_job(self._old_source_pi())
        assert job_dict["encoder"] == "libx264"
        assert job_dict["crf"] == 20
        assert job_dict["preset"] == "fast"

    def test_pi_migration_youtube(self):
        job_dict = _migrate_source_to_job(self._old_source_pi())
        assert job_dict["upload_youtube"] is True
        assert job_dict["default_youtube_title"] == "Test Titel"
        assert job_dict["default_youtube_playlist"] == "Meine PL"

    def test_old_format_loads_via_from_dict(self):
        """Workflow.from_dict verarbeitet altes sources-Format korrekt."""
        old_data = {
            "name": "Altes WF",
            "shutdown_after": False,
            "sources": [self._old_source_pi()],
        }
        wf = Workflow.from_dict(old_data)
        assert len(wf.jobs) == 1
        assert wf.jobs[0].source_mode == "pi_download"

    def test_folder_scan_migration(self):
        source = {
            "source_type": "local",
            "source_path": "/media/videos",
            "destination_path": "/media/converted",
            "move_to_destination": True,
            "file_extensions": "*.mjpg",
            "encoder": "auto",
            "crf": 18,
            "preset": "medium",
            "fps": 25,
            "output_format": "mp4",
        }
        job_dict = _migrate_source_to_job(source)
        assert job_dict["source_mode"] == "folder_scan"
        assert job_dict["source_folder"] == "/media/videos"
        assert job_dict["move_files"] is True
        assert job_dict["file_pattern"] == "*.mjpg"
