from src.media.encoder import (
    build_aac_audio_args,
    build_encoder_args,
    build_mp4_output_args,
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