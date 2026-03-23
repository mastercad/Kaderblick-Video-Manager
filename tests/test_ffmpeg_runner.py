from pathlib import Path
from unittest.mock import patch

from src.media.ffmpeg_runner import MediaValidationResult, inspect_media_compatibility, validate_media_output


class _ProcResult:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class TestValidateMediaOutput:
    def test_rejects_invalid_mp4_without_moov(self, tmp_path):
        path = tmp_path / "broken.mp4"
        path.write_bytes(b"broken")
        logs: list[str] = []

        with patch(
            "src.media.ffmpeg_runner.subprocess.run",
            return_value=_ProcResult(returncode=1, stdout="{}", stderr="moov atom not found\nInvalid data found"),
        ):
            ok = validate_media_output(path, log_callback=logs.append)

        assert ok is False
        assert any("moov atom not found" in line for line in logs)

    def test_accepts_valid_media_with_video_stream(self, tmp_path):
        path = tmp_path / "ok.mp4"
        path.write_bytes(b"ok")
        payload = '{"streams":[{"codec_type":"video"}],"format":{"duration":"10.0","size":"1234","bit_rate":"1000"}}'

        with patch(
            "src.media.ffmpeg_runner.subprocess.run",
            return_value=_ProcResult(returncode=0, stdout=payload, stderr=""),
        ):
            ok = validate_media_output(path)

        assert ok is True


class TestInspectMediaCompatibility:
    def test_surface_scan_reports_repairable_for_incompatible_codec(self, tmp_path):
        path = tmp_path / "hevc.mp4"
        path.write_bytes(b"ok")
        payload = (
            '{"streams":[{"codec_type":"video","codec_name":"hevc","pix_fmt":"yuv420p",'
            '"field_order":"progressive","avg_frame_rate":"25/1"},'
            '{"codec_type":"audio","codec_name":"aac"}],'
            '"format":{"format_name":"mp4","duration":"12.0","size":"1234","bit_rate":"1000"}}'
        )

        with patch(
            "src.media.ffmpeg_runner.subprocess.run",
            return_value=_ProcResult(returncode=0, stdout=payload, stderr=""),
        ):
            result = inspect_media_compatibility(path, deep_scan=False)

        assert result.status == "repairable"
        assert result.compatible is False
        assert any("hevc" in detail for detail in result.details)

    def test_surface_scan_reports_irreparable_without_video_stream(self, tmp_path):
        path = tmp_path / "audio_only.mp4"
        path.write_bytes(b"ok")
        payload = '{"streams":[{"codec_type":"audio","codec_name":"aac"}],"format":{"format_name":"mp4","duration":"12.0","size":"1234","bit_rate":"1000"}}'

        with patch(
            "src.media.ffmpeg_runner.subprocess.run",
            return_value=_ProcResult(returncode=0, stdout=payload, stderr=""),
        ):
            result = inspect_media_compatibility(path, deep_scan=False)

        assert result.status == "irreparable"
        assert "Keine Video-Spur" in result.summary

    def test_deep_scan_reports_ok_for_clean_decode_and_frame_count(self, tmp_path):
        path = tmp_path / "clean.mp4"
        path.write_bytes(b"ok")
        probe_payload = (
            '{"streams":[{"codec_type":"video","codec_name":"h264","pix_fmt":"yuv420p",'
            '"field_order":"progressive","avg_frame_rate":"25/1"},'
            '{"codec_type":"audio","codec_name":"aac"}],'
            '"format":{"format_name":"mp4","duration":"10.0","size":"1234","bit_rate":"1000"}}'
        )
        frame_payload = '{"streams":[{"nb_read_frames":"250","avg_frame_rate":"25/1","duration":"10.0"}]}'

        with patch(
            "src.media.ffmpeg_runner.subprocess.run",
            side_effect=[
                _ProcResult(returncode=0, stdout=probe_payload, stderr=""),
                _ProcResult(returncode=0, stdout="", stderr=""),
                _ProcResult(returncode=0, stdout=frame_payload, stderr=""),
            ],
        ):
            result = inspect_media_compatibility(path, deep_scan=True)

        assert isinstance(result, MediaValidationResult)
        assert result.status == "ok"
        assert result.compatible is True
        assert result.details == []