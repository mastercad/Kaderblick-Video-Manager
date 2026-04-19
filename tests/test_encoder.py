from unittest.mock import patch

from src.media.encoder import (
    HwAccelConfig,
    build_aac_audio_args,
    build_encoder_args,
    build_mp4_output_args,
    get_hwaccel_config,
)


class TestEncoderCompatibility:
    def test_libx264_uses_compatible_h264_profile(self):
        cmd = build_encoder_args(
            "libx264",
            preset="medium",
            crf=18,
            lossless=False,
            fps=25.0,
            no_bframes=True,
            keyframe_interval=1,
        )

        assert "-pix_fmt" in cmd
        assert cmd[cmd.index("-pix_fmt") + 1] == "yuv420p"
        assert "-profile:v" in cmd
        assert cmd[cmd.index("-profile:v") + 1] == "high"
        assert "-bf" in cmd
        assert cmd[cmd.index("-bf") + 1] == "0"

    def test_nvenc_uses_compatible_h264_profile(self):
        cmd = build_encoder_args(
            "h264_nvenc",
            preset="medium",
            crf=18,
            lossless=False,
            fps=25.0,
            no_bframes=True,
            keyframe_interval=1,
        )

        assert "-pix_fmt" in cmd
        assert cmd[cmd.index("-pix_fmt") + 1] == "yuv420p"
        assert "-profile:v" in cmd
        assert cmd[cmd.index("-profile:v") + 1] == "high"

    def test_aac_audio_defaults_to_lc_stereo_48k(self):
        cmd = build_aac_audio_args("192k")

        assert cmd == [
            "-c:a", "aac",
            "-profile:a", "aac_low",
            "-b:a", "192k",
            "-ar", "48000",
            "-ac", "2",
        ]

    def test_mp4_output_args_enable_cfr_and_timestamp_normalization(self):
        cmd = build_mp4_output_args()

        assert cmd == [
            "-map_metadata", "-1",
            "-map_chapters", "-1",
            "-dn",
            "-fps_mode", "cfr",
            "-avoid_negative_ts", "make_zero",
            "-write_tmcd", "0",
            "-metadata:s:v:0", "timecode=",
            "-movflags", "+faststart",
        ]

# ─── get_hwaccel_config ───────────────────────────────────────────────────────

class TestGetHwaccelConfig:
    """Zentraler hwaccel-Service: GPU wird NUR aktiviert wenn ALLE Bedingungen
    erfüllt sind (encoder == h264_nvenc UND detect_cuda_hwdec() == True).

    Zero-Copy  (has_cpu_filter=False): input_flags=[-hwaccel, cuda, -hwaccel_output_format, cuda], strip_pix_fmt=True
    Assist     (has_cpu_filter=True):  input_flags=[-hwaccel, cuda], strip_pix_fmt=False
    Kein GPU   (beliebig):             input_flags=[], strip_pix_fmt=False
    """

    # ── Zero-Copy (kein CPU-Filter, NVDEC verfügbar) ──────────────────────────

    @patch("src.media.encoder.detect_cuda_hwdec", return_value=True)
    def test_zero_copy_input_flags(self, _):
        cfg = get_hwaccel_config("h264_nvenc", has_cpu_filter=False)
        assert cfg.input_flags == ["-hwaccel", "cuda",
                                   "-hwaccel_output_format", "cuda"]

    @patch("src.media.encoder.detect_cuda_hwdec", return_value=True)
    def test_zero_copy_strip_pix_fmt(self, _):
        cfg = get_hwaccel_config("h264_nvenc", has_cpu_filter=False)
        assert cfg.strip_pix_fmt is True

    # ── Assist (CPU-Filter aktiv, NVDEC verfügbar) ────────────────────────────

    @patch("src.media.encoder.detect_cuda_hwdec", return_value=True)
    def test_assist_input_flags(self, _):
        cfg = get_hwaccel_config("h264_nvenc", has_cpu_filter=True)
        assert cfg.input_flags == ["-hwaccel", "cuda"]

    @patch("src.media.encoder.detect_cuda_hwdec", return_value=True)
    def test_assist_no_strip_pix_fmt(self, _):
        cfg = get_hwaccel_config("h264_nvenc", has_cpu_filter=True)
        assert cfg.strip_pix_fmt is False

    @patch("src.media.encoder.detect_cuda_hwdec", return_value=True)
    def test_assist_no_hwaccel_output_format(self, _):
        cfg = get_hwaccel_config("h264_nvenc", has_cpu_filter=True)
        assert "-hwaccel_output_format" not in cfg.input_flags

    # ── Kein GPU: NVDEC nicht verfügbar ──────────────────────────────────────

    @patch("src.media.encoder.detect_cuda_hwdec", return_value=False)
    def test_no_gpu_when_nvdec_unavailable_zero_copy(self, _):
        cfg = get_hwaccel_config("h264_nvenc", has_cpu_filter=False)
        assert cfg.input_flags == []
        assert cfg.strip_pix_fmt is False

    @patch("src.media.encoder.detect_cuda_hwdec", return_value=False)
    def test_no_gpu_when_nvdec_unavailable_assist(self, _):
        cfg = get_hwaccel_config("h264_nvenc", has_cpu_filter=True)
        assert cfg.input_flags == []
        assert cfg.strip_pix_fmt is False

    # ── Kein GPU: Encoder ist nicht h264_nvenc ────────────────────────────────

    @patch("src.media.encoder.detect_cuda_hwdec", return_value=True)
    def test_no_gpu_for_libx264(self, _):
        cfg = get_hwaccel_config("libx264", has_cpu_filter=False)
        assert cfg.input_flags == []
        assert cfg.strip_pix_fmt is False

    @patch("src.media.encoder.detect_cuda_hwdec", return_value=True)
    def test_no_gpu_for_libx264_with_cpu_filter(self, _):
        cfg = get_hwaccel_config("libx264", has_cpu_filter=True)
        assert cfg.input_flags == []
        assert cfg.strip_pix_fmt is False

    @patch("src.media.encoder.detect_cuda_hwdec", return_value=True)
    def test_no_gpu_for_hevc_nvenc(self, _):
        """hevc_nvenc wird (noch) nicht unterstützt – kein hwaccel."""
        cfg = get_hwaccel_config("hevc_nvenc", has_cpu_filter=False)
        assert cfg.input_flags == []
        assert cfg.strip_pix_fmt is False

    def test_default_has_cpu_filter_is_zero_copy(self):
        """has_cpu_filter ist ein Keyword-Argument mit Default False →
        Aufruf ohne has_cpu_filter liefert Zero-Copy-Konfiguration."""
        with patch("src.media.encoder.detect_cuda_hwdec", return_value=True):
            cfg = get_hwaccel_config("h264_nvenc")
        assert "-hwaccel_output_format" in cfg.input_flags
        assert cfg.strip_pix_fmt is True
