"""Zeitstempelbasierte Rekonstruktion von Kaderblick-MJPEG-Aufnahmen."""

from __future__ import annotations

import json
import struct
import threading
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Optional


_SOI = b"\xff\xd8"
_EOI = b"\xff\xd9"
_KBFM_MAGIC = b"KBFM"
_KBFM_VERSION = 1
_KBFM_STRUCT = struct.Struct(">4sBBHQIQQI")
_KATS_MAGIC = b"KATS"
_KATS_VERSION = 1
_KBTS_CHUNK_ID = b"kbts"
_KATS_CLOCK_MONOTONIC = 1
_KATS_FLAG_CLOCK_MONOTONIC = 0x02
_KATS_HEADER = struct.Struct("<4sHHIIIIHHQQ")
_KATS_RECORD = struct.Struct("<QQII")
_KATS_RECORD_FLAG_XRUN = 0x01


@dataclass(frozen=True)
class FrameMetadata:
    frame_index: int
    sequence: int
    timestamp_ns: int
    receive_monotonic_ns: int
    buffer_flags: int


@dataclass(frozen=True)
class FrameDrop:
    after_frame_index: int
    before_frame_index: int
    missing_frames: int
    gap_seconds: float
    previous_sequence: int
    next_sequence: int


@dataclass(frozen=True)
class MjpegTimelineAnalysis:
    fps: float
    source_frames: int
    output_frames: int
    first_timestamp_ns: int
    last_timestamp_ns: int
    drops: tuple[FrameDrop, ...]

    @property
    def inserted_frames(self) -> int:
        return self.output_frames - self.source_frames

    @property
    def duration(self) -> float:
        return self.output_frames / self.fps


@dataclass(frozen=True)
class AudioTimestampAnchor:
    sample_index: int
    timestamp_ns: int
    available_frames: int
    flags: int


@dataclass(frozen=True)
class WavTimelineAnalysis:
    sample_rate: int
    channels: int
    bits_per_sample: int
    sample_frames: int
    first_sample_timestamp_ns: int
    anchors: tuple[AudioTimestampAnchor, ...]

    @property
    def xrun_count(self) -> int:
        return sum(bool(anchor.flags & _KATS_RECORD_FLAG_XRUN) for anchor in self.anchors)

    @property
    def effective_sample_rate(self) -> float:
        """Tatsaechliche Samples pro Sekunde laut erstem/letztem ALSA-Anker."""
        if len(self.anchors) < 2:
            return float(self.sample_rate)
        first = self.anchors[0]
        last = self.anchors[-1]
        elapsed_ns = last.timestamp_ns - first.timestamp_ns
        sample_delta = last.sample_index - first.sample_index
        if elapsed_ns <= 0 or sample_delta <= 0:
            return float(self.sample_rate)
        return sample_delta * 1_000_000_000 / elapsed_ns

    @property
    def rate_error_ppm(self) -> float:
        return (self.effective_sample_rate / self.sample_rate - 1.0) * 1_000_000

    def sample_at_timestamp(self, timestamp_ns: int) -> int:
        """Ordnet einen CLOCK_MONOTONIC-Zeitpunkt einem PCM-Sample zu."""
        delta_ns = timestamp_ns - self.first_sample_timestamp_ns
        return round(delta_ns * self.effective_sample_rate / 1_000_000_000)


def iter_mjpeg_frames(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> Iterator[bytes]:
    """Liest einen rohen MJPEG-Strom frameweise, ohne JPEGs zu dekodieren."""
    buffer = bytearray()
    entropy_start: Optional[int] = None
    with path.open("rb") as source:
        eof = False
        while not eof:
            chunk = source.read(chunk_size)
            if chunk:
                buffer.extend(chunk)
            else:
                eof = True

            while True:
                start = buffer.find(_SOI)
                if start < 0:
                    if eof:
                        return
                    if len(buffer) > 1:
                        del buffer[:-1]
                    break
                if start:
                    del buffer[:start]
                    entropy_start = None
                if entropy_start is None:
                    entropy_start = _jpeg_entropy_start(buffer)
                    if entropy_start is None:
                        break
                # APP-/COM-Segmente duerfen beliebige Binaerdaten enthalten, also
                # auch FF D9. Erst nach dem SOS-Header ist FF D9 ein JPEG-Endmarker.
                end = buffer.find(_EOI, entropy_start)
                if end < 0:
                    break
                frame_end = end + len(_EOI)
                yield bytes(buffer[:frame_end])
                del buffer[:frame_end]
                entropy_start = None

        if buffer:
            raise ValueError("Unvollstaendiger JPEG-Frame am Ende des MJPEG-Stroms")


def _jpeg_entropy_start(data: bytearray) -> Optional[int]:
    """Liefert den Beginn der Scan-Daten oder ``None`` bei unvollstaendigem Header."""
    offset = 2
    while True:
        if offset + 2 > len(data):
            return None
        if data[offset] != 0xFF:
            raise ValueError("Ungueltiger JPEG-Marker vor den Scan-Daten")
        while offset < len(data) and data[offset] == 0xFF:
            offset += 1
        if offset >= len(data):
            return None
        marker = data[offset]
        offset += 1
        if marker in (0xD8, 0xD9) or 0xD0 <= marker <= 0xD7:
            continue
        if offset + 2 > len(data):
            return None
        segment_length = int.from_bytes(data[offset:offset + 2], "big")
        if segment_length < 2:
            raise ValueError("Ungueltige JPEG-Segmentlaenge")
        segment_end = offset + segment_length
        if segment_end > len(data):
            return None
        if marker == 0xDA:
            return segment_end
        offset = segment_end


def _jpeg_metadata(frame: bytes) -> tuple[Optional[float], Optional[FrameMetadata]]:
    if not frame.startswith(_SOI):
        return None, None

    fps: Optional[float] = None
    metadata: Optional[FrameMetadata] = None
    offset = 2
    frame_length = len(frame)
    while offset + 4 <= frame_length and frame[offset] == 0xFF:
        marker = frame[offset + 1]
        offset += 2
        if marker in (0xD8, 0xD9) or 0xD0 <= marker <= 0xD7:
            continue
        if offset + 2 > frame_length:
            break
        segment_length = int.from_bytes(frame[offset:offset + 2], "big")
        if segment_length < 2 or offset + segment_length > frame_length:
            break
        payload = frame[offset + 2:offset + segment_length]
        if marker == 0xFE:
            try:
                value = json.loads(payload.decode("utf-8"))
                raw_fps = float(value.get("fps") or 0)
                if raw_fps > 0:
                    fps = raw_fps
            except (UnicodeDecodeError, ValueError, TypeError, json.JSONDecodeError):
                pass
        elif marker == 0xEF and len(payload) >= _KBFM_STRUCT.size:
            values = _KBFM_STRUCT.unpack(payload[:_KBFM_STRUCT.size])
            magic, version, _flags, header_size = values[:4]
            if magic == _KBFM_MAGIC and version == _KBFM_VERSION and header_size == _KBFM_STRUCT.size:
                metadata = FrameMetadata(
                    frame_index=values[4],
                    sequence=values[5],
                    timestamp_ns=values[6],
                    receive_monotonic_ns=values[7],
                    buffer_flags=values[8],
                )
        offset += segment_length
        if marker == 0xDA:
            break
    return fps, metadata


def has_frame_timestamps(path: Path) -> bool:
    """Prueft nur den ersten Frame auf den KBFM-v1-Marker."""
    try:
        first = next(iter_mjpeg_frames(path))
    except (StopIteration, OSError, ValueError):
        return False
    return _jpeg_metadata(first)[1] is not None


def wav_duration(path: Path) -> Optional[float]:
    """Liest die PCM-WAV-Dauer direkt aus dem Header."""
    try:
        with wave.open(str(path), "rb") as audio:
            rate = audio.getframerate()
            return audio.getnframes() / rate if rate > 0 else None
    except (OSError, EOFError, wave.Error):
        return None


def analyze_wav_timeline(path: Path) -> Optional[WavTimelineAnalysis]:
    """Liest den von der Kamera geschriebenen ``kbts``-RIFF-Chunk.

    Der Chunk verknuepft PCM-Samplepositionen mit ALSA-Hardwarezeitstempeln
    aus ``CLOCK_MONOTONIC`` und verwendet damit dieselbe Uhr wie KBFM-v1.
    """
    try:
        with wave.open(str(path), "rb") as audio:
            sample_rate = audio.getframerate()
            channels = audio.getnchannels()
            bits_per_sample = audio.getsampwidth() * 8
            sample_frames = audio.getnframes()

        payload: Optional[bytes] = None
        with path.open("rb") as source:
            if source.read(4) != b"RIFF":
                return None
            source.seek(8)
            if source.read(4) != b"WAVE":
                return None
            while True:
                chunk_header = source.read(8)
                if not chunk_header:
                    break
                if len(chunk_header) != 8:
                    return None
                chunk_id, chunk_size = struct.unpack("<4sI", chunk_header)
                if chunk_id == _KBTS_CHUNK_ID:
                    payload = source.read(chunk_size)
                    if len(payload) != chunk_size:
                        return None
                    break
                source.seek(chunk_size + (chunk_size & 1), 1)
        if payload is None or len(payload) < _KATS_HEADER.size:
            return None

        values = _KATS_HEADER.unpack_from(payload)
        (magic, version, header_size, record_size, flags, clock_id,
         metadata_rate, metadata_channels, metadata_bits,
         first_sample_timestamp_ns, record_count) = values
        if (
            magic != _KATS_MAGIC
            or version != _KATS_VERSION
            or header_size != _KATS_HEADER.size
            or record_size != _KATS_RECORD.size
            or not (flags & _KATS_FLAG_CLOCK_MONOTONIC)
            or clock_id != _KATS_CLOCK_MONOTONIC
            or metadata_rate != sample_rate
            or metadata_channels != channels
            or metadata_bits != bits_per_sample
            or first_sample_timestamp_ns <= 0
            or record_count < 2
            or header_size + record_count * record_size > len(payload)
        ):
            return None

        anchors = tuple(
            AudioTimestampAnchor(*_KATS_RECORD.unpack_from(payload, header_size + i * record_size))
            for i in range(record_count)
        )
        if any(
            current.sample_index <= previous.sample_index
            or current.timestamp_ns <= previous.timestamp_ns
            for previous, current in zip(anchors, anchors[1:])
        ):
            return None
        if anchors[-1].sample_index > sample_frames + max(anchor.available_frames for anchor in anchors):
            return None

        return WavTimelineAnalysis(
            sample_rate=sample_rate,
            channels=channels,
            bits_per_sample=bits_per_sample,
            sample_frames=sample_frames,
            first_sample_timestamp_ns=first_sample_timestamp_ns,
            anchors=anchors,
        )
    except (OSError, EOFError, ValueError, wave.Error, struct.error):
        return None


def analyze_mjpeg_timeline(
    path: Path,
    *,
    fallback_fps: float,
    cancel_flag: Optional[threading.Event] = None,
    log_callback: Optional[Callable[[str], None]] = None,
) -> Optional[MjpegTimelineAnalysis]:
    """Analysiert KBFM-Zeitstempel und lokalisiert fehlende Ausgabe-Frames."""
    source_frames = 0
    first_timestamp_ns: Optional[int] = None
    last_timestamp_ns: Optional[int] = None
    previous: Optional[FrameMetadata] = None
    previous_slot = -1
    drops: list[FrameDrop] = []
    fps = float(fallback_fps)
    file_size = path.stat().st_size
    bytes_read = 0
    last_progress = -1

    for frame in iter_mjpeg_frames(path):
        if cancel_flag and cancel_flag.is_set():
            return None
        frame_fps, metadata = _jpeg_metadata(frame)
        if source_frames == 0 and frame_fps and frame_fps > 0:
            fps = frame_fps
        if fps <= 0 or metadata is None:
            return None
        if metadata.frame_index != source_frames:
            return None

        if first_timestamp_ns is None:
            first_timestamp_ns = metadata.timestamp_ns
            slot = 0
        else:
            slot = round((metadata.timestamp_ns - first_timestamp_ns) * fps / 1_000_000_000)
            if slot <= previous_slot:
                return None
            missing = slot - previous_slot - 1
            if missing > 0 and previous is not None:
                drops.append(FrameDrop(
                    after_frame_index=previous.frame_index,
                    before_frame_index=metadata.frame_index,
                    missing_frames=missing,
                    gap_seconds=(metadata.timestamp_ns - previous.timestamp_ns) / 1_000_000_000,
                    previous_sequence=previous.sequence,
                    next_sequence=metadata.sequence,
                ))

        previous_slot = slot
        previous = metadata
        last_timestamp_ns = metadata.timestamp_ns
        source_frames += 1
        bytes_read += len(frame)
        progress = int(bytes_read * 100 / file_size) if file_size else 100
        if log_callback and progress >= last_progress + 10:
            last_progress = progress
            log_callback(f"  Zeitstempel-Scan: {progress}% ({source_frames:,} Frames)")

    if source_frames == 0 or first_timestamp_ns is None or last_timestamp_ns is None:
        return None
    return MjpegTimelineAnalysis(
        fps=fps,
        source_frames=source_frames,
        output_frames=previous_slot + 1,
        first_timestamp_ns=first_timestamp_ns,
        last_timestamp_ns=last_timestamp_ns,
        drops=tuple(drops),
    )


def iter_reconstructed_mjpeg(
    path: Path,
    analysis: MjpegTimelineAnalysis,
    *,
    cancel_flag: Optional[threading.Event] = None,
) -> Iterator[bytes]:
    """Erzeugt einen CFR-MJPEG-Strom und fuellt Zeitstempelluecken lokal auf."""
    first_timestamp_ns: Optional[int] = None
    previous_slot = -1
    previous_frame: Optional[bytes] = None
    produced = 0

    for frame in iter_mjpeg_frames(path):
        if cancel_flag and cancel_flag.is_set():
            return
        _fps, metadata = _jpeg_metadata(frame)
        if metadata is None:
            raise ValueError("KBFM-Metadaten fehlen waehrend der Rekonstruktion")
        if first_timestamp_ns is None:
            first_timestamp_ns = metadata.timestamp_ns
            slot = 0
        else:
            slot = round(
                (metadata.timestamp_ns - first_timestamp_ns)
                * analysis.fps / 1_000_000_000
            )
        if previous_frame is not None:
            for _ in range(max(0, slot - previous_slot - 1)):
                yield previous_frame
                produced += 1
        yield frame
        produced += 1
        previous_frame = frame
        previous_slot = slot

    if produced != analysis.output_frames:
        raise ValueError(
            f"Rekonstruierte Framezahl {produced} weicht von Analyse {analysis.output_frames} ab"
        )
