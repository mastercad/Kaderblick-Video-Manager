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

from src.media.converter import ConvertJob, build_embedded_metadata_args, run_concat, run_convert, run_repair_output, run_youtube_convert
from src.media.encoder import HwAccelConfig
from src.media.merge import merge_halves
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


class TestEmbeddedMetadata:
    def test_build_embedded_metadata_args_contains_description_and_creator(self):
        job = ConvertJob(
            source_path=Path("/a/b.mp4"),
            youtube_title="2026-03-22 | Heim vs Gast | 1. Halbzeit",
            youtube_description="Beschreibung zum Spiel",
            youtube_playlist="22.03.2026 | Liga | Heim vs Gast",
            youtube_tags=["Fußball", "Liga", "Heim", "Gast"],
        )

        args = build_embedded_metadata_args(job)

        assert "-metadata" in args
        assert "title=2026-03-22 | Heim vs Gast | 1. Halbzeit" in args
        assert "description=Beschreibung zum Spiel" in args
        assert any(value.startswith("comment=Beschreibung zum Spiel") for value in args)
        assert "software=Kaderblick — Video Manager" in args
        assert "author=Kaderblick — Video Manager" in args
        assert "album=22.03.2026 | Liga | Heim vs Gast" in args
        assert "keywords=Fußball, Liga, Heim, Gast" in args


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


class TestRunRepairOutput:
    def test_reuses_existing_valid_repaired_output(self, tmp_path):
        source = tmp_path / "clip.mp4"
        repaired = tmp_path / "clip_repaired.mp4"
        source.write_bytes(b"source")
        repaired.write_bytes(b"repaired")
        settings = AppSettings()
        job = ConvertJob(source_path=source, output_path=source)

        with patch("src.media.converter.validate_media_output", return_value=True):
            ok = run_repair_output(job, settings)

        assert ok is True
        assert job.output_path == repaired

    def test_falls_back_to_transcode_when_lossless_repair_fails(self, tmp_path):
        source = tmp_path / "clip.mp4"
        source.write_bytes(b"source")
        settings = AppSettings()
        job = ConvertJob(source_path=source, output_path=source)
        ffmpeg_calls: list[list[str]] = []

        def _run_ffmpeg(cmd, **_kwargs):
            ffmpeg_calls.append(cmd)
            Path(cmd[-1]).write_bytes(b"video")
            return 1 if len(ffmpeg_calls) == 1 else 0

        with patch("src.media.converter.get_video_stream_info", return_value={"codec_name": "h264", "fps": 25.0, "bit_rate": 2_000_000}), \
             patch("src.media.converter.get_audio_stream_info", return_value={"codec_name": "aac"}), \
             patch("src.media.converter.get_duration", return_value=10.0), \
             patch("src.media.converter.validate_media_output", return_value=True), \
             patch("src.media.converter.build_video_encoder_args", return_value=("libx264", ["-c:v", "libx264"])), \
             patch("src.media.converter.run_ffmpeg", side_effect=_run_ffmpeg):
            ok = run_repair_output(job, settings)

        assert ok is True
        assert len(ffmpeg_calls) == 2
        assert job.output_path == tmp_path / "clip_repaired.mp4"
        assert job.output_path.exists()

    def test_builds_full_ffmpeg_command_for_lossless_repair(self, tmp_path):
        source = tmp_path / "clip.mp4"
        source.write_bytes(b"source")
        settings = AppSettings()
        job = ConvertJob(source_path=source, output_path=source)
        ffmpeg_calls: list[list[str]] = []

        def _run_ffmpeg(cmd, **_kwargs):
            ffmpeg_calls.append(cmd)
            Path(cmd[-1]).write_bytes(b"video")
            return 0

        with patch("src.media.ffmpeg_runner.get_ffmpeg_bin", return_value="/usr/bin/ffmpeg-test"), \
             patch("src.media.converter.get_video_stream_info", return_value={"codec_name": "h264", "fps": 25.0, "bit_rate": 2_000_000}), \
             patch("src.media.converter.get_audio_stream_info", return_value={"codec_name": "aac"}), \
             patch("src.media.converter.get_duration", return_value=10.0), \
             patch("src.media.converter.validate_media_output", return_value=True), \
             patch("src.media.converter.run_ffmpeg", side_effect=_run_ffmpeg):
            ok = run_repair_output(job, settings)

        assert ok is True
        assert ffmpeg_calls[0][0] == "/usr/bin/ffmpeg-test"
        assert ffmpeg_calls[0][1:3] == ["-hide_banner", "-y"]


class TestRunConcat:
    """run_concat() – Tests mit echter Temp-Datei-Erzeugung, aber gemocktem ffmpeg."""

    def _make_files(self, tmp: str, count: int) -> list[Path]:
        paths = []
        for i in range(count):
            p = Path(tmp) / f"video_{i:02d}.mp4"
            p.touch()
            paths.append(p)
        return paths

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    def test_success_creates_output(self, mock_ffmpeg, _validate_media):
        """run_concat meldet True, wenn ffmpeg 0 zurückgibt und Ausgabe existiert."""
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            out.touch()   # simuliert von ffmpeg erstellte Ausgabedatei

            result = run_concat(srcs, out, overwrite=True)

        assert result is True
        mock_ffmpeg.assert_called_once()

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=1)
    def test_nonzero_exit_code_returns_false(self, mock_ffmpeg, _validate_media):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            result = run_concat(srcs, out)
        assert result is False

    @patch("src.media.converter.run_ffmpeg", return_value=-1)
    def test_cancelled_returns_false(self, mock_ffmpeg):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            out.touch()
            result = run_concat(srcs, out, overwrite=True)
        assert result is False

    @patch("src.media.converter.run_ffmpeg", return_value=0)
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

    @patch("src.media.converter.run_ffmpeg", return_value=-1)
    def test_cancelled_deletes_partial_output(self, _mock):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            out.touch()   # existiert noch vor dem Aufruf
            run_concat(srcs, out, overwrite=True)
            # Ausgabedatei soll nach Abbruch gelöscht sein
            assert not out.exists()

    @patch("src.media.converter.run_ffmpeg", return_value=0)
    def test_log_callback_called(self, mock_ffmpeg):
        log_lines = []
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            out.touch()
            run_concat(srcs, out, log_callback=log_lines.append, overwrite=True)
        # Mindestens ein Log-Eintrag (z. B. "Zusammenführen: …")
        assert any(log_lines)

    @patch("src.media.converter.run_ffmpeg", return_value=0)
    def test_cancel_flag_passed_to_ffmpeg(self, mock_ffmpeg):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out  = Path(tmp) / "merged.mp4"
            out.touch()
            cancel = threading.Event()
            run_concat(srcs, out, cancel_flag=cancel, overwrite=True)
        _, kwargs = mock_ffmpeg.call_args
        assert kwargs.get("cancel_flag") is cancel

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.get_duration", side_effect=[12.5, 7.5])
    def test_passes_combined_source_duration_to_ffmpeg(self, _mock_duration, mock_ffmpeg, _validate_media):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out = Path(tmp) / "merged.mp4"
            out.touch()

            run_concat(srcs, out, overwrite=True)

        _, kwargs = mock_ffmpeg.call_args
        assert kwargs.get("duration") == 20.0

    @patch("src.media.converter.run_ffmpeg", return_value=0)
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

    @patch("src.media.converter.run_ffmpeg", return_value=0)
    def test_concat_uses_safe_gop_settings_without_bframes(self, mock_ffmpeg):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out = Path(tmp) / "merged.mp4"
            out.touch()
            run_concat(srcs, out, overwrite=True)

        cmd = mock_ffmpeg.call_args[0][0]
        assert "-bf" in cmd
        assert cmd[cmd.index("-bf") + 1] == "0"
        assert "-g" in cmd

    @patch("src.media.converter.run_ffmpeg", return_value=0)
    def test_concat_embeds_metadata_when_provided(self, mock_ffmpeg):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = self._make_files(tmp, 2)
            out = Path(tmp) / "merged.mp4"
            out.touch()
            metadata_job = ConvertJob(
                source_path=srcs[0],
                youtube_title="Merge Titel",
                youtube_description="Merge Beschreibung",
                youtube_playlist="Merge Playlist",
            )

            run_concat(srcs, out, overwrite=True, metadata_job=metadata_job)

        cmd = mock_ffmpeg.call_args[0][0]
        assert "title=Merge Titel" in cmd
        assert "description=Merge Beschreibung" in cmd
        assert "software=Kaderblick — Video Manager" in cmd


class TestRunYouTubeConvert:
    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=("h264_nvenc", ["-c:v", "h264_nvenc", "-preset", "p5"]))
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=12.0)
    def test_uses_central_encoder_plan_and_logs_gpu(self, _dur, _info,
                                                     _build_args, mock_ffmpeg, _validate_media):
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
        assert Path(cmd[0]).stem == "ffmpeg"
        assert "-c:v" in cmd
        assert "h264_nvenc" in cmd
        assert "-fflags" in cmd
        assert cmd[cmd.index("-fflags") + 1] == "+genpts"
        assert "-avoid_negative_ts" in cmd
        assert "-movflags" in cmd
        assert "-map" in cmd
        assert "-profile:a" in cmd
        assert cmd[cmd.index("-profile:a") + 1] == "aac_low"
        assert any("YouTube-Encoder: h264_nvenc" in line for line in log_lines)

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=("h264_nvenc", ["-c:v", "h264_nvenc", "-preset", "p5"]))
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=12.0)
    def test_embeds_title_description_and_creator_metadata(self, _dur, _info, _build_args, mock_ffmpeg, _validate_media):
        with tempfile.TemporaryDirectory() as tmp:
            mp4 = Path(tmp) / "video.mp4"
            yt = Path(tmp) / "video_youtube.mp4"
            mp4.touch()
            yt.touch()

            settings = AppSettings()
            settings.video.encoder = "auto"
            settings.video.overwrite = True
            job = ConvertJob(
                source_path=mp4,
                output_path=mp4,
                youtube_title="Merge Titel",
                youtube_description="Ausführliche Beschreibung",
                youtube_playlist="Merge Playlist",
            )

            ok = run_youtube_convert(job, settings)

        assert ok is True
        cmd = mock_ffmpeg.call_args[0][0]
        assert "title=Merge Titel" in cmd
        assert "description=Ausführliche Beschreibung" in cmd
        assert "album=Merge Playlist" in cmd
        assert "software=Kaderblick — Video Manager" in cmd

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=("h264_nvenc", ["-c:v", "h264_nvenc", "-preset", "p5"]))
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=12.0)
    def test_retries_without_faststart_after_mux_failure(self, _dur, _info,
                                                         _build_args, _validate_media):
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
            calls: list[list[str]] = []

            def fake_run_ffmpeg(cmd, **_kwargs):
                calls.append(cmd)
                if len(calls) == 1:
                    if yt.exists():
                        yt.unlink()
                    return -6
                yt.touch()
                return 0

            with patch("src.media.converter.run_ffmpeg", side_effect=fake_run_ffmpeg):
                ok = run_youtube_convert(job, settings, log_callback=log_lines.append)

        assert ok is True
        assert len(calls) == 2
        assert "-movflags" in calls[0]
        assert "-movflags" not in calls[1]
        assert any("ohne MP4-Faststart" in line for line in log_lines)

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=("h264_nvenc", ["-c:v", "h264_nvenc", "-preset", "p5"]))
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=12.0)
    def test_supports_avi_youtube_variant_without_mp4_faststart(self, _dur, _info,
                                                                _build_args, mock_ffmpeg, _validate_media):
        with tempfile.TemporaryDirectory() as tmp:
            mp4 = Path(tmp) / "video.mp4"
            yt = Path(tmp) / "video_youtube.avi"
            mp4.touch()
            yt.touch()

            settings = AppSettings()
            settings.video.encoder = "auto"
            settings.video.overwrite = True
            job = ConvertJob(source_path=mp4, output_path=mp4)

            ok = run_youtube_convert(job, settings, output_format="avi")

        assert ok is True
        cmd = mock_ffmpeg.call_args[0][0]
        assert str(yt) == cmd[-1]
        assert "-movflags" not in cmd
        assert "-avoid_negative_ts" not in cmd

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=("libx264", ["-c:v", "libx264", "-preset", "slow"]))
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0, "bit_rate": 6000000, "codec_name": "h264"})
    @patch("src.media.converter.get_duration", return_value=12.0)
    @patch("src.media.converter.has_audio_stream", return_value=False)
    def test_convert_applies_selected_output_resolution(self, _has_audio, _dur, _info, _build_args, mock_ffmpeg, _validate_media):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "input.mp4"
            out = Path(tmp) / "output.mp4"
            src.touch()

            settings = AppSettings()
            settings.video.output_format = "mp4"
            settings.video.output_resolution = "720p"
            settings.video.overwrite = True
            job = ConvertJob(source_path=src, output_path=out)

            def fake_run_ffmpeg(_cmd, **_kwargs):
                out.touch()
                return 0

            mock_ffmpeg.side_effect = fake_run_ffmpeg

            ok = run_convert(job, settings)

        assert ok is True
        cmd = mock_ffmpeg.call_args[0][0]
        assert "-vf" in cmd
        assert "scale=w=1280:h=720" in cmd[cmd.index("-vf") + 1]

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=("libx264", ["-c:v", "libx264", "-preset", "slow"]))
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=12.0)
    def test_concat_applies_selected_output_resolution(self, _dur, _info, _build_args, mock_ffmpeg, _validate_media):
        with tempfile.TemporaryDirectory() as tmp:
            srcs = [Path(tmp) / "part1.mp4", Path(tmp) / "part2.mp4"]
            for src in srcs:
                src.touch()
            out = Path(tmp) / "merged.mp4"
            out.touch()

            ok = run_concat(srcs, out, overwrite=True, target_resolution="1080p")

        assert ok is True
        cmd = mock_ffmpeg.call_args[0][0]
        fc = cmd[cmd.index("-filter_complex") + 1]
        assert "scale=w=1920:h=1080" in fc

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=("h264_nvenc", ["-c:v", "h264_nvenc", "-preset", "p5"]))
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=12.0)
    def test_youtube_convert_applies_selected_output_resolution(self, _dur, _info, _build_args, mock_ffmpeg, _validate_media):
        with tempfile.TemporaryDirectory() as tmp:
            mp4 = Path(tmp) / "video.mp4"
            yt = Path(tmp) / "video_youtube.mp4"
            mp4.touch()
            yt.touch()

            settings = AppSettings()
            settings.video.encoder = "auto"
            settings.video.overwrite = True
            job = ConvertJob(source_path=mp4, output_path=mp4)

            ok = run_youtube_convert(job, settings, output_resolution="2160p")

        assert ok is True
        cmd = mock_ffmpeg.call_args[0][0]
        assert "-vf" in cmd
        assert "scale=w=3840:h=2160" in cmd[cmd.index("-vf") + 1]


class TestRunConvertCompatibility:
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.get_duration", return_value=12.0)
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0, "bit_rate": 6000000, "codec_name": "h264"})
    @patch("src.media.converter.build_video_encoder_args",
           return_value=("libx264", ["-c:v", "libx264", "-profile:v", "high", "-level:v", "4.2"]))
    @patch("src.media.converter.has_audio_stream", return_value=True)
    def test_mp4_outputs_are_standardized_instead_of_stream_copy(
        self,
        _has_audio,
        _build_args,
        _video_info,
        _duration,
        mock_ffmpeg,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "input.mp4"
            out = Path(tmp) / "output.mp4"
            src.touch()

            settings = AppSettings()
            settings.video.output_format = "mp4"
            settings.video.overwrite = True
            settings.audio.include_audio = True
            settings.audio.amplify_audio = False

            job = ConvertJob(source_path=src, output_path=out)

            from src.media.converter import run_convert

            def fake_run_ffmpeg(_cmd, **_kwargs):
                out.touch()
                return 0

            mock_ffmpeg.side_effect = fake_run_ffmpeg

            ok = run_convert(job, settings)

        assert ok is True
        cmd = mock_ffmpeg.call_args[0][0]
        assert "-c" not in cmd or "copy" not in cmd
        assert "-map" in cmd
        assert "-profile:a" in cmd
        assert "-movflags" in cmd


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

    @patch("src.media.merge.run_ffmpeg", return_value=0)
    @patch("src.media.merge.get_duration", return_value=5.0)
    @patch("src.media.merge.get_resolution", return_value=(1920, 1080))
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

    @patch("src.media.merge.run_ffmpeg", return_value=0)
    @patch("src.media.merge.get_duration", return_value=5.0)
    @patch("src.media.merge.get_resolution", return_value=(1920, 1080))
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

    @patch("src.media.merge.run_ffmpeg", return_value=0)
    @patch("src.media.merge.get_duration", return_value=5.0)
    @patch("src.media.merge.get_resolution", return_value=(1920, 1080))
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

    @patch("src.media.merge.run_ffmpeg", return_value=0)
    @patch("src.media.merge.get_duration", return_value=5.0)
    @patch("src.media.merge.get_resolution", return_value=(1920, 1080))
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


# ─── GPU-Dekodierung: -hwaccel cuda + -hwaccel_output_format cuda ────────────

class TestHwaccelDecoding:
    """
    Aller hwaccel-Code läuft über den zentralen Service get_hwaccel_config()
    (in encoder.py, via Import in converter.py).  Jeder Test mockt ihn explizit.

    run_youtube_convert Zero-Copy (kein Skalierungsfilter, NVDEC verfügbar):
      - NVDEC dekodiert → Frames bleiben im VRAM (-hwaccel_output_format cuda)
      - NVENC liest direkt aus VRAM → kein CPU-Memcpy
      - -pix_fmt yuv420p wird entfernt (würde hwdownload erzwingen)
        1.  -hwaccel cuda vorhanden
        2.  -hwaccel_output_format cuda vorhanden
        3.  -hwaccel_output_format cuda vor -i
        4.  -pix_fmt NICHT im finalen Kommando
        5.  Beide Faststart-Versuche haben Flags und kein pix_fmt

    run_youtube_convert Partiell (Skalierungsfilter aktiv, NVDEC verfügbar):
      - Nur -hwaccel cuda; Frames für CPU-Filter in System-RAM
      - -pix_fmt bleibt erhalten
        6+7+8.  -hwaccel cuda, KEIN output_format, pix_fmt bleibt

    run_youtube_convert kein hwaccel:
        9.   libx264 → kein -hwaccel
        9b.  h264_nvenc + NVDEC nicht verfügbar → kein -hwaccel

    run_concat NVDEC-Assist (concat-Filter ist immer CPU → NIEMALS output_format):
        10.  h264_nvenc + NVDEC: -hwaccel cuda pro Input, kein output_format
        11.  h264_nvenc + NVDEC n=3: 3× -hwaccel cuda, kein output_format
        11b. h264_nvenc + NVDEC nicht verfügbar → kein -hwaccel

    run_concat mit target_resolution:
        12.  gleich wie 10, nur mit Skalierungsfilter (kein output_format)

    run_concat libx264:
        13.  kein -hwaccel
    """

    _NVENC_ARGS_WITH_PIX_FMT = ("h264_nvenc",
                                 ["-c:v", "h264_nvenc", "-preset", "p6",
                                  "-pix_fmt", "yuv420p"])
    _NVENC_ARGS_NO_PIX_FMT   = ("h264_nvenc",
                                 ["-c:v", "h264_nvenc", "-preset", "p6"])
    _X264_ARGS               = ("libx264",
                                 ["-c:v", "libx264", "-preset", "slow",
                                  "-pix_fmt", "yuv420p"])

    _HWACCEL_ZERO_COPY = HwAccelConfig(
        input_flags=["-hwaccel", "cuda", "-hwaccel_output_format", "cuda"],
        strip_pix_fmt=True,
    )
    _HWACCEL_ASSIST = HwAccelConfig(input_flags=["-hwaccel", "cuda"])
    _HWACCEL_NONE   = HwAccelConfig()

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _yt_job(self, tmp: str):
        mp4 = Path(tmp) / "video.mp4"
        yt  = Path(tmp) / "video_youtube.mp4"
        mp4.touch(); yt.touch()
        settings = AppSettings()
        settings.video.overwrite = True
        job = ConvertJob(source_path=mp4, output_path=mp4)
        return mp4, yt, settings, job

    # ── run_youtube_convert: Zero-Copy ────────────────────────────────────────

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_NVENC_ARGS_WITH_PIX_FMT)
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=10.0)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_ZERO_COPY)
    def test_yt_zero_copy_hwaccel_cuda_present(
        self, _hwaccel, _dur, _info, _build, mock_ffmpeg, _validate
    ):
        """1. Zero-Copy: -hwaccel cuda im Kommando."""
        with tempfile.TemporaryDirectory() as tmp:
            _, _, settings, job = self._yt_job(tmp)
            run_youtube_convert(job, settings)
        cmd = mock_ffmpeg.call_args[0][0]
        assert "-hwaccel" in cmd and cmd[cmd.index("-hwaccel") + 1] == "cuda"

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_NVENC_ARGS_WITH_PIX_FMT)
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=10.0)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_ZERO_COPY)
    def test_yt_zero_copy_hwaccel_output_format_present(
        self, _hwaccel, _dur, _info, _build, mock_ffmpeg, _validate
    ):
        """2. Zero-Copy: -hwaccel_output_format cuda im Kommando."""
        with tempfile.TemporaryDirectory() as tmp:
            _, _, settings, job = self._yt_job(tmp)
            run_youtube_convert(job, settings)
        cmd = mock_ffmpeg.call_args[0][0]
        assert "-hwaccel_output_format" in cmd
        assert cmd[cmd.index("-hwaccel_output_format") + 1] == "cuda"

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_NVENC_ARGS_WITH_PIX_FMT)
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=10.0)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_ZERO_COPY)
    def test_yt_zero_copy_hwaccel_output_format_before_i(
        self, _hwaccel, _dur, _info, _build, mock_ffmpeg, _validate
    ):
        """3. Zero-Copy: -hwaccel_output_format steht vor -i."""
        with tempfile.TemporaryDirectory() as tmp:
            _, _, settings, job = self._yt_job(tmp)
            run_youtube_convert(job, settings)
        cmd = mock_ffmpeg.call_args[0][0]
        assert cmd.index("-hwaccel_output_format") < cmd.index("-i")

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_NVENC_ARGS_WITH_PIX_FMT)
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=10.0)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_ZERO_COPY)
    def test_yt_zero_copy_pix_fmt_removed(
        self, _hwaccel, _dur, _info, _build, mock_ffmpeg, _validate
    ):
        """4. Zero-Copy: -pix_fmt darf NICHT im Kommando stehen (würde hwdownload triggern)."""
        with tempfile.TemporaryDirectory() as tmp:
            _, _, settings, job = self._yt_job(tmp)
            run_youtube_convert(job, settings)
        cmd = mock_ffmpeg.call_args[0][0]
        assert "-pix_fmt" not in cmd

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_NVENC_ARGS_WITH_PIX_FMT)
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=10.0)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_ZERO_COPY)
    def test_yt_zero_copy_both_faststart_attempts(
        self, _hwaccel, _dur, _info, _build, _validate
    ):
        """5. Zero-Copy: Beide Faststart-Versuche haben -hwaccel_output_format cuda
        und kein -pix_fmt."""
        with tempfile.TemporaryDirectory() as tmp:
            mp4 = Path(tmp) / "video.mp4"
            yt  = Path(tmp) / "video_youtube.mp4"
            mp4.touch()
            settings = AppSettings()
            settings.video.overwrite = True
            job = ConvertJob(source_path=mp4, output_path=mp4)
            captured: list[list[str]] = []

            def fake_ffmpeg(cmd, **_kwargs):
                captured.append(list(cmd))
                if len(captured) == 1:
                    return -6
                yt.touch()
                return 0

            with patch("src.media.converter.run_ffmpeg", side_effect=fake_ffmpeg):
                run_youtube_convert(job, settings)

        assert len(captured) == 2
        for idx, cmd in enumerate(captured):
            assert "-hwaccel_output_format" in cmd, \
                f"-hwaccel_output_format fehlt in Versuch {idx + 1}"
            assert cmd[cmd.index("-hwaccel_output_format") + 1] == "cuda"
            assert "-pix_fmt" not in cmd, \
                f"-pix_fmt in Versuch {idx + 1} vorhanden – darf nicht sein"

    # ── run_youtube_convert: Partiell (CPU-Filter) ────────────────────────────

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_NVENC_ARGS_WITH_PIX_FMT)
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=10.0)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_ASSIST)
    def test_yt_partial_gpu_path_when_filter_active(
        self, _hwaccel, _dur, _info, _build, mock_ffmpeg, _validate
    ):
        """6+7+8. output_resolution gesetzt → partieller GPU-Pfad:
        -hwaccel cuda vorhanden, KEIN -hwaccel_output_format, -pix_fmt erhalten."""
        with tempfile.TemporaryDirectory() as tmp:
            mp4 = Path(tmp) / "video.mp4"
            yt  = Path(tmp) / "video_youtube.mp4"
            mp4.touch(); yt.touch()
            settings = AppSettings()
            settings.video.overwrite = True
            job = ConvertJob(source_path=mp4, output_path=mp4)
            run_youtube_convert(job, settings, output_resolution="1080p")

        cmd = mock_ffmpeg.call_args[0][0]
        assert "-hwaccel" in cmd and cmd[cmd.index("-hwaccel") + 1] == "cuda"
        assert "-hwaccel_output_format" not in cmd
        assert "-pix_fmt" in cmd and cmd[cmd.index("-pix_fmt") + 1] == "yuv420p"

    # ── run_youtube_convert: libx264 ──────────────────────────────────────────

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_X264_ARGS)
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=10.0)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_NONE)
    def test_yt_no_hwaccel_for_libx264(
        self, _hwaccel, _dur, _info, _build, mock_ffmpeg, _validate
    ):
        """9. libx264 → kein -hwaccel."""
        with tempfile.TemporaryDirectory() as tmp:
            _, _, settings, job = self._yt_job(tmp)
            run_youtube_convert(job, settings)
        cmd = mock_ffmpeg.call_args[0][0]
        assert "-hwaccel" not in cmd

    @patch("src.media.converter.validate_media_output", return_value=True)
    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_NVENC_ARGS_WITH_PIX_FMT)
    @patch("src.media.converter.get_video_stream_info", return_value={"fps": 25.0})
    @patch("src.media.converter.get_duration", return_value=10.0)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_NONE)
    def test_yt_no_hwaccel_when_nvdec_unavailable(
        self, _hwaccel, _dur, _info, _build, mock_ffmpeg, _validate
    ):
        """9b. h264_nvenc aber NVDEC nicht verfügbar → kein -hwaccel (Service gibt HwAccelConfig())."""
        with tempfile.TemporaryDirectory() as tmp:
            _, _, settings, job = self._yt_job(tmp)
            run_youtube_convert(job, settings)
        cmd = mock_ffmpeg.call_args[0][0]
        assert "-hwaccel" not in cmd

    # ── run_concat: NVDEC-Assist (concat-Filter ist immer CPU → nie output_format) ──

    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_NVENC_ARGS_WITH_PIX_FMT)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_ASSIST)
    def test_concat_nvdec_assist_hwaccel_present_no_output_format(self, _hwaccel, _build, mock_ffmpeg):
        """10. concat h264_nvenc + NVDEC verfügbar:
        -hwaccel cuda pro Input, NIEMALS -hwaccel_output_format
        (concat-Filter ist CPU-basiert)."""
        with tempfile.TemporaryDirectory() as tmp:
            srcs = [Path(tmp) / f"p{i}.mp4" for i in range(2)]
            for s in srcs: s.touch()
            out = Path(tmp) / "merged.mp4"; out.touch()
            run_concat(srcs, out, overwrite=True, encoder="h264_nvenc")

        cmd = mock_ffmpeg.call_args[0][0]
        assert "-hwaccel" in cmd and cmd[cmd.index("-hwaccel") + 1] == "cuda"
        assert "-hwaccel_output_format" not in cmd

    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_NVENC_ARGS_WITH_PIX_FMT)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_ASSIST)
    def test_concat_nvdec_assist_order_and_count_n3(self, _hwaccel, _build, mock_ffmpeg):
        """11. concat NVDEC-Assist (n=3): je -hwaccel cuda vor -i,
        kein -hwaccel_output_format."""
        with tempfile.TemporaryDirectory() as tmp:
            srcs = [Path(tmp) / f"p{i}.mp4" for i in range(3)]
            for s in srcs: s.touch()
            out = Path(tmp) / "merged.mp4"; out.touch()
            run_concat(srcs, out, overwrite=True, encoder="h264_nvenc")

        cmd = mock_ffmpeg.call_args[0][0]
        assert cmd.count("-hwaccel") == 3
        assert "-hwaccel_output_format" not in cmd
        assert cmd.count("-i") == 3
        # Reihenfolge: -hwaccel cuda -fflags +genpts -i für jede Quelle
        relevant = [(i, t) for i, t in enumerate(cmd)
                    if t in ("-hwaccel", "-i")]
        state = 0  # 0=expect_hwaccel, 1=expect_i
        for _, tok in relevant:
            if state == 0:
                assert tok == "-hwaccel", f"Erwartet -hwaccel, got {tok}"
                state = 1
            else:
                assert tok == "-i", f"Erwartet -i, got {tok}"
                state = 0

    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_NVENC_ARGS_WITH_PIX_FMT)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_NONE)
    def test_concat_no_hwaccel_when_nvdec_unavailable(self, _hwaccel, _build, mock_ffmpeg):
        """11b. h264_nvenc aber NVDEC nicht verfügbar → kein -hwaccel in concat."""
        with tempfile.TemporaryDirectory() as tmp:
            srcs = [Path(tmp) / f"p{i}.mp4" for i in range(2)]
            for s in srcs: s.touch()
            out = Path(tmp) / "merged.mp4"; out.touch()
            run_concat(srcs, out, overwrite=True, encoder="h264_nvenc")

        cmd = mock_ffmpeg.call_args[0][0]
        assert "-hwaccel" not in cmd

    # ── run_concat: mit target_resolution (Skalierungsfilter + concat-Filter) ─

    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_NVENC_ARGS_WITH_PIX_FMT)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_ASSIST)
    def test_concat_nvdec_assist_with_resolution(self, _hwaccel, _build, mock_ffmpeg):
        """12. concat mit target_resolution → NVDEC-Assist:
        -hwaccel cuda vorhanden, KEIN -hwaccel_output_format."""
        with tempfile.TemporaryDirectory() as tmp:
            srcs = [Path(tmp) / f"p{i}.mp4" for i in range(2)]
            for s in srcs: s.touch()
            out = Path(tmp) / "merged.mp4"; out.touch()
            run_concat(srcs, out, overwrite=True, encoder="h264_nvenc",
                       target_resolution="1080p")

        cmd = mock_ffmpeg.call_args[0][0]
        assert "-hwaccel" in cmd
        assert "-hwaccel_output_format" not in cmd

    # ── run_concat: libx264 ───────────────────────────────────────────────────

    @patch("src.media.converter.run_ffmpeg", return_value=0)
    @patch("src.media.converter.build_video_encoder_args",
           return_value=_X264_ARGS)
    @patch("src.media.converter.get_hwaccel_config",
           return_value=_HWACCEL_NONE)
    def test_concat_no_hwaccel_for_libx264(self, _hwaccel, _build, mock_ffmpeg):
        """13. libx264 → kein -hwaccel."""
        with tempfile.TemporaryDirectory() as tmp:
            srcs = [Path(tmp) / f"p{i}.mp4" for i in range(2)]
            for s in srcs: s.touch()
            out = Path(tmp) / "merged.mp4"; out.touch()
            run_concat(srcs, out, overwrite=True, encoder="libx264")

        cmd = mock_ffmpeg.call_args[0][0]
        assert "-hwaccel" not in cmd


# ─── merge_halves – hwaccel via get_hwaccel_config ───────────────────────────

class TestMergeHalvesHwaccel:
    """merge_halves leitet die hwaccel-Konfiguration für den internen
    concat-Befehl über get_hwaccel_config() (has_cpu_filter=True).

    Alle Tests mocken get_hwaccel_config in src.media.merge damit
    kein echter ffmpeg gestartet wird und die Entscheidungslogik
    ausschließlich im Service getestet wird.

    14. h264_nvenc + NVDEC verfügbar → -hwaccel cuda pro Input, kein output_format
    15. h264_nvenc + NVDEC verfügbar, n=3 → 3× -hwaccel cuda, alle vor -i
    16. h264_nvenc + NVDEC nicht verfügbar → kein -hwaccel
    17. libx264 → kein -hwaccel
    """

    _HWACCEL_ASSIST = HwAccelConfig(input_flags=["-hwaccel", "cuda"])
    _HWACCEL_NONE   = HwAccelConfig()

    def _make_settings(self):
        s = AppSettings()
        s.video.merge_title_duration = 1
        return s

    def _make_finished_job(self, tmp: Path, stem: str) -> ConvertJob:
        src = tmp / f"{stem}_src.mjpeg"
        out = tmp / f"{stem}.mp4"
        src.touch()
        out.touch()
        return ConvertJob(source_path=src, output_path=out, status="Fertig")

    @patch("src.media.merge.run_ffmpeg", return_value=0)
    @patch("src.media.merge.get_duration", return_value=5.0)
    @patch("src.media.merge.get_resolution", return_value=(1920, 1080))
    @patch("src.media.merge.get_hwaccel_config", return_value=_HWACCEL_ASSIST)
    def test_merge_nvdec_assist_flag_present(self, _hwaccel, _res, _dur, mock_ffmpeg):
        """14. h264_nvenc + NVDEC: -hwaccel cuda vor jedem -i im concat-Kommando."""
        captured: list[list[str]] = []

        def cap(cmd, **kw):
            if "-filter_complex" in cmd:
                captured.append(list(cmd))
            return 0

        mock_ffmpeg.side_effect = cap

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            j1 = self._make_finished_job(p, "halb1")
            j2 = self._make_finished_job(p, "halb2")
            merge_halves([j1, j2], self._make_settings())

        assert captured, "Kein concat-ffmpeg-Aufruf"
        cmd = captured[-1]
        assert "-hwaccel" in cmd and cmd[cmd.index("-hwaccel") + 1] == "cuda"
        assert "-hwaccel_output_format" not in cmd

    @patch("src.media.merge.run_ffmpeg", return_value=0)
    @patch("src.media.merge.get_duration", return_value=5.0)
    @patch("src.media.merge.get_resolution", return_value=(1920, 1080))
    @patch("src.media.merge.get_hwaccel_config", return_value=_HWACCEL_ASSIST)
    def test_merge_nvdec_assist_count_n3(self, _hwaccel, _res, _dur, mock_ffmpeg):
        """15. n=3 Quellen → 3× -hwaccel cuda, alle je vor dem zugehörigen -i."""
        captured: list[list[str]] = []

        def cap(cmd, **kw):
            if "-filter_complex" in cmd:
                captured.append(list(cmd))
            return 0

        mock_ffmpeg.side_effect = cap

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            jobs = [self._make_finished_job(p, f"halb{i}") for i in range(3)]
            s = self._make_settings()
            merge_halves(jobs, s)

        assert captured, "Kein concat-ffmpeg-Aufruf"
        cmd = captured[-1]
        # 3 Quellen + mindestens 3 Titelkarten → mehrere -i
        # Wichtig: für jede tatsächliche Quelle muss -hwaccel cuda davor stehen
        assert cmd.count("-hwaccel") >= 3
        assert "-hwaccel_output_format" not in cmd
        # Reihenfolge: jedes -hwaccel muss vor dem nächsten -i kommen
        relevant = [(i, t) for i, t in enumerate(cmd) if t in ("-hwaccel", "-i")]
        state = 0  # 0=erwarte hwaccel, 1=erwarte -i
        for _, tok in relevant:
            if state == 0:
                assert tok == "-hwaccel", f"Erwartet -hwaccel, got {tok}"
                state = 1
            else:
                assert tok == "-i", f"Erwartet -i, got {tok}"
                state = 0

    @patch("src.media.merge.run_ffmpeg", return_value=0)
    @patch("src.media.merge.get_duration", return_value=5.0)
    @patch("src.media.merge.get_resolution", return_value=(1920, 1080))
    @patch("src.media.merge.get_hwaccel_config", return_value=_HWACCEL_NONE)
    def test_merge_no_hwaccel_when_nvdec_unavailable(self, _hwaccel, _res, _dur, mock_ffmpeg):
        """16. NVDEC nicht verfügbar → kein -hwaccel im concat-Kommando."""
        captured: list[list[str]] = []

        def cap(cmd, **kw):
            if "-filter_complex" in cmd:
                captured.append(list(cmd))
            return 0

        mock_ffmpeg.side_effect = cap

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            j1 = self._make_finished_job(p, "halb1")
            j2 = self._make_finished_job(p, "halb2")
            merge_halves([j1, j2], self._make_settings())

        assert captured, "Kein concat-ffmpeg-Aufruf"
        cmd = captured[-1]
        assert "-hwaccel" not in cmd

    @patch("src.media.merge.run_ffmpeg", return_value=0)
    @patch("src.media.merge.get_duration", return_value=5.0)
    @patch("src.media.merge.get_resolution", return_value=(1920, 1080))
    @patch("src.media.merge.get_hwaccel_config", return_value=_HWACCEL_NONE)
    def test_merge_no_hwaccel_for_libx264(self, _hwaccel, _res, _dur, mock_ffmpeg):
        """17. libx264 → kein -hwaccel."""
        captured: list[list[str]] = []

        def cap(cmd, **kw):
            if "-filter_complex" in cmd:
                captured.append(list(cmd))
            return 0

        mock_ffmpeg.side_effect = cap

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            j1 = self._make_finished_job(p, "halb1")
            j2 = self._make_finished_job(p, "halb2")
            s = self._make_settings()
            s.video.encoder = "libx264"
            merge_halves([j1, j2], s)

        assert captured, "Kein concat-ffmpeg-Aufruf"
        cmd = captured[-1]
        assert "-hwaccel" not in cmd
