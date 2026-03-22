"""Tests für converter.py und merge.py.

Geprüft:
- ConvertJob Defaultwerte (inkl. youtube_tags)
- ConvertJob.to_dict / from_dict Roundtrip
- run_concat(): Listendatei korrekt, ffmpeg-Aufruf, Ergebnis
- merge_halves(): YouTube-Variante (_youtube.mp4) wird bevorzugt
"""

import os
import tempfile
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from src.converter import ConvertJob, run_concat, run_youtube_convert
from src.merge import merge_halves
from src.settings import AppSettings


# ─── ConvertJob ───────────────────────────────────────────────────────────────

class TestConvertJob:
    def test_defaults(self):
        job = ConvertJob(source_path=Path("/a/b.mp4"))
        assert job.job_type == "convert"
        assert job.status == "Wartend"
        assert job.output_path is None
        assert job.audio_override is None
        assert job.youtube_title == ""
        assert job.youtube_description == ""
        assert job.youtube_playlist == ""
        assert job.youtube_tags == []   # neu hinzugefügtes Feld
        assert job.error_msg == ""
        assert job.progress_pct == 0

    def test_youtube_tags_set(self):
        job = ConvertJob(
            source_path=Path("/a/b.mp4"),
            youtube_tags=["Fußball", "Liga", "2026"],
        )
        assert job.youtube_tags == ["Fußball", "Liga", "2026"]

    def test_to_dict_basic(self):
        job = ConvertJob(
            source_path=Path("/a/b.mp4"),
            output_path=Path("/a/b.mp4"),
            youtube_title="Spielbericht",
            youtube_playlist="Liga",
            youtube_tags=["tag1"],
            device_name="pi-links",
        )
        d = job.to_dict()
        assert d["source_path"] == "/a/b.mp4"
        assert d["youtube_title"] == "Spielbericht"
        assert d["youtube_playlist"] == "Liga"
        assert d["device_name"] == "pi-links"

    def test_to_dict_none_paths(self):
        job = ConvertJob(source_path=Path("/a/b.mp4"))
        d = job.to_dict()
        assert d["output_path"] == ""
        assert d["audio_override"] == ""

    def test_from_dict_roundtrip(self):
        job = ConvertJob(
            source_path=Path("/media/video.mjpg"),
            job_type="download",
            youtube_title="Halbzeit 1",
            youtube_description="Beschreibung",
            youtube_playlist="Kreisliga",
            device_name="pi-rechts",
        )
        restored = ConvertJob.from_dict(job.to_dict())
        assert restored.source_path == Path("/media/video.mjpg")
        assert restored.job_type == "download"
        assert restored.youtube_title == "Halbzeit 1"
        assert restored.device_name == "pi-rechts"

    def test_from_dict_with_paths(self):
        d = {
            "source_path": "/a/b.mp4",
            "output_path": "/a/b_out.mp4",
            "audio_override": "/a/b.wav",
            "job_type": "convert",
            "status": "Fertig",
            "youtube_title": "",
            "youtube_description": "",
            "youtube_playlist": "",
            "device_name": "",
        }
        job = ConvertJob.from_dict(d)
        assert job.output_path == Path("/a/b_out.mp4")
        assert job.audio_override == Path("/a/b.wav")

    def test_from_dict_missing_optional_keys(self):
        """from_dict soll auch mit minimalem dict funktionieren."""
        job = ConvertJob.from_dict({"source_path": "/a/b.mp4"})
        assert job.source_path == Path("/a/b.mp4")
        assert job.status == "Wartend"


# ─── run_concat ───────────────────────────────────────────────────────────────

class TestRunConcat:
    """run_concat() – Tests mit echter Temp-Datei-Erzeugung, aber gemocktem ffmpeg."""

    def _make_files(self, tmp: str, count: int) -> list[Path]:
        """Erstellt count echte (leere) Dateien und gibt ihre Pfade zurück."""
        paths = []
        for i in range(count):
            p = Path(tmp) / f"video_{i:02d}.mp4"
            p.touch()
            paths.append(p)
        return paths

    def test_empty_list_returns_false(self):
        assert run_concat([], Path("/tmp/out.mp4")) is False

    @patch("src.converter.run_ffmpeg", return_value=0)
    def test_success_creates_output(self, mock_ffmpeg):
        """run_concat meldet True, wenn ffmpeg 0 zurückgibt und Ausgabe existiert."""
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            out.touch()   # simuliert von ffmpeg erstellte Ausgabedatei

            result = run_concat(srcs, out, overwrite=True)

        assert result is True
        mock_ffmpeg.assert_called_once()

    @patch("src.converter.run_ffmpeg", return_value=1)
    def test_nonzero_exit_code_returns_false(self, mock_ffmpeg):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            result = run_concat(srcs, out)
        assert result is False

    @patch("src.converter.run_ffmpeg", return_value=-1)
    def test_cancelled_returns_false(self, mock_ffmpeg):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            out.touch()
            result = run_concat(srcs, out, overwrite=True)
        assert result is False

    @patch("src.converter.run_ffmpeg", return_value=0)
    def test_all_sources_passed_as_inputs(self, mock_ffmpeg):
        """Alle Quell-Pfade müssen als -i Argumente im ffmpeg-Aufruf erscheinen."""
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 3)
            out  = Path(tmp) / "merged.mp4"
            out.touch()
            run_concat(srcs, out, overwrite=True)

        cmd = mock_ffmpeg.call_args[0][0]
        for src in srcs:
            assert str(src) in cmd, f"{src.name} nicht in ffmpeg-Aufruf"

    @patch("src.converter.run_ffmpeg", return_value=-1)
    def test_cancelled_deletes_partial_output(self, _mock):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            out.touch()   # existiert noch vor dem Aufruf
            run_concat(srcs, out, overwrite=True)
            # Ausgabedatei soll nach Abbruch gelöscht sein
            assert not out.exists()

    @patch("src.converter.run_ffmpeg", return_value=0)
    def test_log_callback_called(self, mock_ffmpeg):
        log_lines = []
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            out.touch()
            run_concat(srcs, out, log_callback=log_lines.append, overwrite=True)
        # Mindestens ein Log-Eintrag (z. B. "Zusammenführen: …")
        assert any(log_lines)

    @patch("src.converter.run_ffmpeg", return_value=0)
    def test_cancel_flag_passed_to_ffmpeg(self, mock_ffmpeg):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            out.touch()
            cancel = threading.Event()
            run_concat(srcs, out, cancel_flag=cancel, overwrite=True)
        _, kwargs = mock_ffmpeg.call_args
        assert kwargs.get("cancel_flag") is cancel

    @patch("src.converter.run_ffmpeg", return_value=0)
    def test_filter_complex_contains_concat(self, mock_ffmpeg):
        """Das ffmpeg-Kommando muss einen filter_complex mit concat enthalten."""
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            out.touch()
            run_concat(srcs, out, overwrite=True)

        cmd = mock_ffmpeg.call_args[0][0]
        assert "-filter_complex" in cmd
        fc_idx = cmd.index("-filter_complex")
        fc_val = cmd[fc_idx + 1]
        assert "concat" in fc_val
        assert "[outv]" in fc_val
        assert "[outa]" in fc_val


class TestRunYouTubeConvert:
    @patch("src.converter.run_ffmpeg", return_value=0)
    @patch("src.converter.build_video_encoder_args",
           return_value=("h264_nvenc", ["-c:v", "h264_nvenc", "-preset", "p5"]))
    @patch("src.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.converter.get_duration", return_value=12.0)
    def test_uses_central_encoder_plan_and_logs_gpu(self, _dur, _info,
                                                     _build_args, mock_ffmpeg):
        with tempfile.TemporaryDirectory() as tmp:
            mp4 = Path(tmp) / "video.mp4"
            yt = Path(tmp) / "video_youtube.mp4"
            mp4.touch()
            yt.touch()

            settings = AppSettings()
            settings.video.encoder = "auto"
            settings.video.overwrite = True
            job = ConvertJob(source_path=mp4, output_path=mp4)
            log_lines: list[str] = []

            ok = run_youtube_convert(job, settings, log_callback=log_lines.append)

        assert ok is True
        cmd = mock_ffmpeg.call_args[0][0]
        assert cmd[0].endswith("ffmpeg")
        assert "-c:v" in cmd
        assert "h264_nvenc" in cmd
        assert any("YouTube-Encoder: h264_nvenc" in line for line in log_lines)


# ─── merge_halves – _youtube-Variante bevorzugen ───────────────────────────────

class TestMergeHalvesStandardFiles:
    """merge_halves() soll die _youtube-Variante (CRF=23, kleiner) bevorzugen
    wenn sie existiert. Ohne _youtube-Variante wird job.output_path verwendet.
    Die YouTube-Version wird nach dem Merge aus dem zusammengeführten File erstellt."""

    def _make_settings(self):
        from src.settings import AppSettings
        return AppSettings()

    def _make_finished_job(self, tmp: Path, stem: str,
                           has_youtube_variant: bool = False) -> "ConvertJob":
        """Erzeugt einen ConvertJob im Status 'Fertig' mit echter Datei."""
        src  = tmp / f"{stem}_src.mjpeg"
        out  = tmp / f"{stem}.mp4"
        src.touch()
        out.touch()
        if has_youtube_variant:
            (tmp / f"{stem}_youtube.mp4").touch()
        job = ConvertJob(source_path=src, output_path=out, status="Fertig")
        return job

    @patch("src.merge.run_ffmpeg", return_value=0)
    @patch("src.merge.get_duration", return_value=5.0)
    @patch("src.merge.get_resolution", return_value=(1920, 1080))
    def test_standard_file_used_even_when_youtube_present(self, _res, _dur, mock_ffmpeg):
        """Auch wenn _youtube.mp4 existiert, wird die Standard-MP4 gemergt."""
        captured_cmds: list[list[str]] = []

        def capture_ffmpeg(cmd, **kwargs):
            if "-filter_complex" in cmd:
                captured_cmds.append(cmd)
            return 0

        mock_ffmpeg.side_effect = capture_ffmpeg

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            j1 = self._make_finished_job(p, "halb1", has_youtube_variant=True)
            j2 = self._make_finished_job(p, "halb2", has_youtube_variant=True)

            settings = self._make_settings()
            settings.video.merge_title_duration = 1
            merge_halves([j1, j2], settings)

        assert captured_cmds, "Concat-Aufruf fehlt"
        cmd = captured_cmds[-1]  # letzter Aufruf = der eigentliche Merge
        # Nur Standard-Dateien (keine _youtube) sollen als -i auftauchen
        i_args = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-i"]
        std_entries = [p for p in i_args if ("halb1.mp4" in p or "halb2.mp4" in p) and "_youtube" not in p]
        assert len(std_entries) == 2, (
            f"Erwartet 2 Standard-Einträge, gefunden: {std_entries}\n"
            f"-i Argumente: {i_args}")
        assert not any("_youtube" in p for p in i_args if "halb" in p), (
            f"Unerwartete _youtube-Einträge: {i_args}")

    @patch("src.merge.run_ffmpeg", return_value=0)
    @patch("src.merge.get_duration", return_value=5.0)
    @patch("src.merge.get_resolution", return_value=(1920, 1080))
    def test_standard_file_used_without_youtube_variant(self, _res, _dur, mock_ffmpeg):
        """Ohne _youtube.mp4 muss die Standard-MP4 im ffmpeg-Aufruf stehen."""
        captured_cmds: list[list[str]] = []

        def capture_ffmpeg(cmd, **kwargs):
            if "-filter_complex" in cmd:
                captured_cmds.append(cmd)
            return 0

        mock_ffmpeg.side_effect = capture_ffmpeg

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            j1 = self._make_finished_job(p, "halb1", has_youtube_variant=False)
            j2 = self._make_finished_job(p, "halb2", has_youtube_variant=False)

            settings = self._make_settings()
            settings.video.merge_title_duration = 1
            merge_halves([j1, j2], settings)

        assert captured_cmds, "Concat-Aufruf fehlt"
        cmd = captured_cmds[-1]
        i_args = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-i"]
        standard_entries = [p for p in i_args if "halb1.mp4" in p or "halb2.mp4" in p]
        assert len(standard_entries) == 2, (
            f"Erwartet 2 Standard-Einträge, gefunden: {standard_entries}")
        assert not any("_youtube" in p for p in i_args if "halb" in p)

    @patch("src.merge.run_ffmpeg", return_value=0)
    @patch("src.merge.get_duration", return_value=5.0)
    @patch("src.merge.get_resolution", return_value=(1920, 1080))
    def test_both_use_standard_regardless_of_variants(self, _res, _dur, mock_ffmpeg):
        """Immer Standard-Dateien nutzen – auch im gemischten Fall."""
        captured_cmds: list[list[str]] = []

        def capture_ffmpeg(cmd, **kwargs):
            if "-filter_complex" in cmd:
                captured_cmds.append(cmd)
            return 0

        mock_ffmpeg.side_effect = capture_ffmpeg

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            j1 = self._make_finished_job(p, "halb1", has_youtube_variant=True)
            j2 = self._make_finished_job(p, "halb2", has_youtube_variant=False)

            settings = self._make_settings()
            settings.video.merge_title_duration = 1
            merge_halves([j1, j2], settings)

        assert captured_cmds
        cmd = captured_cmds[-1]
        i_args = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-i"]
        # halb1.mp4 (nicht halb1_youtube.mp4) muss als -i auftauchen
        assert any("halb1.mp4" in p and "_youtube" not in p
                   for p in i_args), "halb1: Standard-Datei fehlt"
        assert any("halb2.mp4" in p and "_youtube" not in p
                   for p in i_args), "halb2: Standard-Datei fehlt"

    @patch("src.merge.run_ffmpeg", return_value=0)
    @patch("src.merge.get_duration", return_value=5.0)
    @patch("src.merge.get_resolution", return_value=(1920, 1080))
    def test_single_job_skipped(self, _res, _dur, mock_ffmpeg):
        """Gruppen mit nur einer Datei werden übersprungen (kein Merge)."""
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            j1 = self._make_finished_job(p, "halb1", has_youtube_variant=True)
            settings = self._make_settings()
            result = merge_halves([j1], settings)
        assert result == []
        # ffmpeg darf für den Merge nicht aufgerufen worden sein
        concat_calls = [
            c for c in mock_ffmpeg.call_args_list
            if "-f" in c.args[0] and "concat" in c.args[0]
        ]
        assert concat_calls == []
