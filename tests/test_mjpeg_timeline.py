import json
import struct
import wave
from pathlib import Path

from src.media.mjpeg_timeline import (
    analyze_mjpeg_timeline,
    analyze_wav_timeline,
    has_frame_timestamps,
    iter_mjpeg_frames,
    iter_reconstructed_mjpeg,
    wav_duration,
)


_KBFM = struct.Struct(">4sBBHQIQQI")
_KATS_HEADER = struct.Struct("<4sHHIIIIHHQQ")
_KATS_RECORD = struct.Struct("<QQII")


def _segment(marker: int, payload: bytes) -> bytes:
    return bytes((0xFF, marker)) + (len(payload) + 2).to_bytes(2, "big") + payload


def _frame(index: int, sequence: int, timestamp_ns: int, *, fps: int | None = None) -> bytes:
    parts = [b"\xff\xd8"]
    if fps is not None:
        parts.append(_segment(0xFE, json.dumps({"fps": fps}).encode()))
    parts.append(_segment(0xEF, _KBFM.pack(
        b"KBFM", 1, 0, _KBFM.size, index, sequence,
        timestamp_ns, timestamp_ns + 1_000_000, 0x12001,
    )))
    parts += [_segment(0xDA, b""), b"frame-data", b"\xff\xd9"]
    return b"".join(parts)


def test_timestamp_analysis_finds_and_reconstructs_local_drop(tmp_path: Path):
    source = tmp_path / "recording.mjpg"
    frames = [
        _frame(0, 100, 1_000_000_000, fps=15),
        _frame(1, 102, 1_066_667_000),
        _frame(2, 106, 1_200_000_000),
    ]
    source.write_bytes(b"".join(frames))

    analysis = analyze_mjpeg_timeline(source, fallback_fps=25)

    assert analysis is not None
    assert analysis.fps == 15
    assert analysis.source_frames == 3
    assert analysis.output_frames == 4
    assert analysis.inserted_frames == 1
    assert analysis.duration == 4 / 15
    assert len(analysis.drops) == 1
    assert analysis.drops[0].missing_frames == 1
    assert analysis.drops[0].previous_sequence == 102
    assert analysis.drops[0].next_sequence == 106

    reconstructed = list(iter_reconstructed_mjpeg(source, analysis))
    assert reconstructed == [frames[0], frames[1], frames[1], frames[2]]


def test_old_mjpeg_without_kbfm_is_not_timestamped(tmp_path: Path):
    source = tmp_path / "old.mjpg"
    old_frame = b"\xff\xd8" + _segment(0xDA, b"") + b"old-frame\xff\xd9"
    source.write_bytes(old_frame)

    assert list(iter_mjpeg_frames(source)) == [old_frame]
    assert has_frame_timestamps(source) is False
    assert analyze_mjpeg_timeline(source, fallback_fps=15) is None


def test_wav_duration_reads_pcm_header(tmp_path: Path):
    source = tmp_path / "audio.wav"
    with wave.open(str(source), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(48_000)
        output.writeframes(b"\x00\x00" * 12_000)

    assert wav_duration(source) == 0.25


def test_wav_timeline_reads_monotonic_alsa_anchors(tmp_path: Path):
    source = tmp_path / "audio.wav"
    with wave.open(str(source), "wb") as output:
        output.setnchannels(2)
        output.setsampwidth(2)
        output.setframerate(48_000)
        output.writeframes(b"\x00\x00\x00\x00" * 48_000)

    anchors = [(480, 1_010_000_000, 480, 0), (48_000, 2_000_000_000, 960, 0)]
    payload = _KATS_HEADER.pack(
        b"KATS", 1, _KATS_HEADER.size, _KATS_RECORD.size,
        0x07, 1, 48_000, 2, 16, 1_000_000_000, len(anchors),
    ) + b"".join(_KATS_RECORD.pack(*anchor) for anchor in anchors)
    with source.open("r+b") as output:
        output.seek(0, 2)
        output.write(b"kbts" + struct.pack("<I", len(payload)) + payload)
        size = output.tell()
        output.seek(4)
        output.write(struct.pack("<I", size - 8))

    analysis = analyze_wav_timeline(source)

    assert analysis is not None
    assert analysis.sample_frames == 48_000
    assert analysis.effective_sample_rate == 48_000
    assert analysis.sample_at_timestamp(1_050_000_000) == 2_400
    assert analysis.xrun_count == 0


def test_binary_metadata_may_contain_eoi_bytes_without_splitting_frame(tmp_path: Path):
    source = tmp_path / "binary-marker.mjpg"
    frame = _frame(0, 1, 0x00000000FFD90000, fps=15)
    source.write_bytes(frame)

    assert list(iter_mjpeg_frames(source)) == [frame]
    analysis = analyze_mjpeg_timeline(source, fallback_fps=25)
    assert analysis is not None
    assert analysis.source_frames == 1
