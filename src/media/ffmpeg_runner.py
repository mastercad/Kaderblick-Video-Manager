"""ffmpeg-Prozesssteuerung mit Fortschrittsanzeige und Abbruch.

Stellt low-level Funktionen bereit:
  - ``run_ffmpeg()`` – Prozess starten, stderr parsen, Fortschritt melden
  - ``get_duration()`` / ``get_resolution()`` – ffprobe-Helfer
  - ``find_audio()`` – zugehörige WAV-Datei suchen
  - ``estimate_duration_from_filesize()`` – Heuristik für MJPEG-Rohstreams
"""

import json
import os
import re
import signal
import shutil
import subprocess
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from ..runtime_paths import bundled_binary_path, popen_process_group_kwargs, terminate_process_tree

_RE_TIME = re.compile(r"time=(\d+):(\d+):(\d+)\.(\d+)")
_RE_STATUS = re.compile(r"(?:^|\s)(?:frame=|fps=|q=|size=|time=|bitrate=|speed=)")
_WARNING_MARKERS = (
    "warning",
    "error",
    "invalid",
    "failed",
    "unsupported",
    "non-monotonous",
    "moov atom",
    "assertion",
    "deprecated",
    "corrupt",
)
_TIMESTAMP_WARNING_MARKERS = (
    "non monoton",
    "invalid timestamp",
    "pts has no value",
    "invalid dts",
    "timestamp discontinuity",
    "out of order",
    "past duration",
)


@dataclass
class MediaValidationResult:
    status: str
    summary: str
    compatible: bool = False
    details: list[str] = field(default_factory=list)


def _parse_fractional_rate(raw: str) -> Optional[float]:
    try:
        num_s, den_s = str(raw or "0/0").split("/")
        num, den = int(num_s), int(den_s)
        if den > 0:
            return num / den
    except (ValueError, ZeroDivisionError):
        return None
    return None


def _media_compatibility_issues(payload: dict) -> list[str]:
    issues: list[str] = []
    streams = payload.get("streams") or []
    video = next((stream for stream in streams if stream.get("codec_type") == "video"), None)
    audio = next((stream for stream in streams if stream.get("codec_type") == "audio"), None)
    fmt = str((payload.get("format") or {}).get("format_name") or "")

    if video is None:
        issues.append("Keine Video-Spur vorhanden")
        return issues

    video_codec = str(video.get("codec_name") or "").lower()
    if video_codec != "h264":
        issues.append(f"Video-Codec ist {video_codec or 'unbekannt'} statt h264")

    pix_fmt = str(video.get("pix_fmt") or "").lower()
    if pix_fmt and pix_fmt not in {"yuv420p", "yuvj420p"}:
        issues.append(f"Pixel-Format ist {pix_fmt} statt yuv420p")

    field_order = str(video.get("field_order") or "").lower()
    if field_order and field_order not in {"progressive", "unknown"}:
        issues.append(f"Field-Order ist {field_order} statt progressive")

    if audio is not None:
        audio_codec = str(audio.get("codec_name") or "").lower()
        if audio_codec != "aac":
            issues.append(f"Audio-Codec ist {audio_codec or 'unbekannt'} statt aac")

    if fmt and "mp4" not in fmt and "mov" not in fmt:
        issues.append(f"Container ist {fmt} statt MP4/MOV")

    return issues


def inspect_media_compatibility(
    filepath: Path,
    *,
    require_video: bool = True,
    deep_scan: bool = False,
    log_callback=None,
) -> MediaValidationResult:
    def log(msg: str) -> None:
        if log_callback:
            log_callback(msg)

    if not filepath.exists() or filepath.stat().st_size <= 0:
        msg = f"Mediendatei fehlt oder ist leer: {filepath.name}"
        log(f"⚠ {msg}")
        return MediaValidationResult("irreparable", msg)

    try:
        probe = subprocess.run(
            ffprobe_cmd(
                "-v", "warning",
                "-show_entries",
                "stream=codec_type,codec_name,pix_fmt,field_order,avg_frame_rate,duration:format=format_name,duration,size,bit_rate",
                "-of", "json",
                str(filepath),
            ),
            capture_output=True,
            text=True,
            timeout=30 if deep_scan else 15,
        )
    except subprocess.TimeoutExpired:
        msg = f"ffprobe Timeout bei {filepath.name}"
        log(f"⚠ {msg}")
        return MediaValidationResult("irreparable", msg)
    except Exception as exc:
        msg = f"ffprobe Fehler bei {filepath.name}: {exc}"
        log(f"⚠ {msg}")
        return MediaValidationResult("irreparable", msg)

    stderr_lines = [line.strip() for line in probe.stderr.splitlines() if line.strip()]
    for line in stderr_lines[-20:]:
        log(f"  {line}")

    if probe.returncode != 0:
        msg = f"ffprobe kann {filepath.name} nicht lesbar analysieren"
        log(f"⚠ {msg}")
        return MediaValidationResult("irreparable", msg, details=stderr_lines[-20:])

    try:
        payload = json.loads(probe.stdout or "{}")
    except Exception:
        msg = f"ffprobe lieferte ungueltige Metadaten fuer {filepath.name}"
        log(f"⚠ {msg}")
        return MediaValidationResult("irreparable", msg)

    streams = payload.get("streams") or []
    if require_video and not any(stream.get("codec_type") == "video" for stream in streams):
        msg = f"Keine Video-Spur in {filepath.name}"
        log(f"⚠ {msg}")
        return MediaValidationResult("irreparable", msg)

    fmt = payload.get("format") or {}
    try:
        duration = float(fmt.get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0.0
    if duration <= 0:
        msg = f"Ungueltige Dauer fuer {filepath.name}"
        log(f"⚠ {msg}")
        return MediaValidationResult("irreparable", msg)

    issues = _media_compatibility_issues(payload)
    compatible = not issues

    if not deep_scan:
        if compatible:
            return MediaValidationResult("ok", f"Datei ist lesbar und kompatibel: {filepath.name}", compatible=True)
        for issue in issues:
            log(f"  ⚠ {issue}")
        return MediaValidationResult(
            "repairable",
            f"Datei ist lesbar, aber nicht voll kompatibel: {filepath.name}",
            compatible=False,
            details=issues,
        )

    deep_findings: list[str] = []
    try:
        decode = subprocess.run(
            ffmpeg_cmd(
                "-v", "warning",
                "-i", str(filepath),
                "-map", "0:v:0",
                "-map", "0:a?",
                "-f", "null", "-",
            ),
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        msg = f"Deep-Scan konnte nicht gestartet werden: {exc}"
        log(f"⚠ {msg}")
        return MediaValidationResult("irreparable", msg, compatible=False)

    decode_lines = [line.strip() for line in decode.stderr.splitlines() if line.strip()]
    warning_lines = [
        line for line in decode_lines
        if any(marker in line.lower() for marker in _WARNING_MARKERS)
    ]
    timestamp_lines = [
        line for line in decode_lines
        if any(marker in line.lower() for marker in _TIMESTAMP_WARNING_MARKERS)
    ]
    if decode.returncode != 0:
        deep_findings.append("ffmpeg meldet Dekodierfehler im Vollscan")
    if warning_lines:
        deep_findings.append("ffmpeg meldet Warnungen beim Vollscan")
    if timestamp_lines:
        deep_findings.append("Zeitstempelprobleme erkannt")

    try:
        frame_count_probe = subprocess.run(
            ffprobe_cmd(
                "-v", "error",
                "-count_frames",
                "-select_streams", "v:0",
                "-show_entries", "stream=nb_read_frames,avg_frame_rate,duration",
                "-of", "json",
                str(filepath),
            ),
            capture_output=True,
            text=True,
            timeout=120,
        )
        if frame_count_probe.returncode == 0:
            count_payload = json.loads(frame_count_probe.stdout or "{}")
            video_stream = next(iter(count_payload.get("streams") or []), {})
            read_frames = int(video_stream.get("nb_read_frames") or 0)
            fps = _parse_fractional_rate(str(video_stream.get("avg_frame_rate") or "0/0"))
            stream_duration = float(video_stream.get("duration") or duration or 0)
            if read_frames > 0 and fps and stream_duration > 0:
                expected_frames = int(round(stream_duration * fps))
                diff = abs(expected_frames - read_frames)
                tolerance = max(5, int(expected_frames * 0.01))
                if diff > tolerance:
                    deep_findings.append(
                        f"Auffaellige Frame-Anzahl: erwartet ca. {expected_frames}, gelesen {read_frames}"
                    )
    except Exception:
        pass

    for line in warning_lines[-20:]:
        log(f"  {line}")

    if issues or deep_findings:
        details = [*issues, *deep_findings]
        for issue in details:
            log(f"  ⚠ {issue}")
        return MediaValidationResult(
            "repairable",
            f"Deep-Scan erkennt reparierbare Probleme: {filepath.name}",
            compatible=False,
            details=details,
        )

    return MediaValidationResult("ok", f"Deep-Scan ohne Befund: {filepath.name}", compatible=True)


def get_ffmpeg_bin() -> str:
    """Liefert den zentral konfigurierten ffmpeg-Binärpfad.

    Über ``KADERBLICK_FFMPEG_BIN`` kann ein eigenes, selbst gebautes ffmpeg
    erzwungen werden. Ohne Override wird die PATH-Auflösung verwendet.
    """
    candidate = bundled_binary_path("KADERBLICK_FFMPEG_BIN", "ffmpeg")
    return shutil.which(candidate) or candidate


def get_ffprobe_bin() -> str:
    """Liefert den zentral konfigurierten ffprobe-Binärpfad."""
    candidate = bundled_binary_path("KADERBLICK_FFPROBE_BIN", "ffprobe")
    return shutil.which(candidate) or candidate


def ffmpeg_cmd(*args: str) -> list[str]:
    """Baut eine ffmpeg-Kommandozeile mit dem zentralen Binärpfad."""
    return [get_ffmpeg_bin(), *args]


def ffprobe_cmd(*args: str) -> list[str]:
    """Baut eine ffprobe-Kommandozeile mit dem zentralen Binärpfad."""
    return [get_ffprobe_bin(), *args]


# ═════════════════════════════════════════════════════════════════
#  Hilfsfunktionen (ffprobe, Audio-Suche)
# ═════════════════════════════════════════════════════════════════

def find_audio(mjpg_path: Path, suffix: str = "") -> Optional[Path]:
    """Sucht die zugehörige WAV-Datei zu einer MJPEG-Datei."""
    stem = mjpg_path.stem
    parent = mjpg_path.parent
    if suffix:
        wav = parent / f"{stem}{suffix}.wav"
        if wav.exists():
            return wav
    wav = parent / f"{stem}.wav"
    if wav.exists():
        return wav
    for candidate in sorted(parent.glob(f"{stem}*.wav")):
        return candidate
    return None


def get_duration(filepath: Path) -> Optional[float]:
    """Ermittelt die Dauer einer Mediendatei via ffprobe."""
    try:
        result = subprocess.run(
            ffprobe_cmd("-v", "quiet", "-show_entries", "format=duration",
                        "-of", "csv=p=0", str(filepath)),
            capture_output=True, text=True, timeout=30,
        )
        val = result.stdout.strip()
        return float(val) if val and val != "N/A" else None
    except Exception:
        return None


def get_resolution(filepath: Path) -> Optional[tuple[int, int]]:
    """Ermittelt die Auflösung (width, height) eines Videos via ffprobe."""
    try:
        result = subprocess.run(
            ffprobe_cmd("-v", "quiet", "-select_streams", "v:0",
                        "-show_entries", "stream=width,height",
                        "-of", "csv=p=0:s=x", str(filepath)),
            capture_output=True, text=True, timeout=30,
        )
        parts = result.stdout.strip().split("x")
        if len(parts) == 2:
            return int(parts[0]), int(parts[1])
    except Exception:
        pass
    return None


def has_audio_stream(filepath: Path) -> bool:
    """Prüft ob eine Mediendatei eine Audio-Spur enthält (ffprobe)."""
    try:
        result = subprocess.run(
            ffprobe_cmd("-v", "quiet", "-select_streams", "a",
                        "-show_entries", "stream=codec_type",
                        "-of", "csv=p=0", str(filepath)),
            capture_output=True, text=True, timeout=30,
        )
        return "audio" in result.stdout.strip().lower()
    except Exception:
        return False


def get_video_stream_info(filepath: Path) -> dict:
    """Ermittelt Codec, FPS und Bitrate des ersten Video-Streams via ffprobe.

    Bevorzugt die Stream-Bitrate; fällt auf die Container-Gesamt-Bitrate
    zurück, wenn der Stream keinen eigenen Wert liefert.

    Returns:
        Dict mit ``'codec_name'`` (str), ``'fps'`` (float|None),
        ``'bit_rate'`` (int Bits/s oder None). Leeres Dict bei Fehler.
    """
    import json as _json
    try:
        result = subprocess.run(
            ffprobe_cmd("-v", "quiet", "-select_streams", "v:0",
                        "-show_entries",
                        "stream=codec_name,bit_rate,avg_frame_rate:format=bit_rate",
                        "-of", "json", str(filepath)),
            capture_output=True, text=True, timeout=30,
        )
        data = _json.loads(result.stdout)
        streams = data.get("streams", [])
        codec_name = streams[0].get("codec_name", "") if streams else ""

        # FPS aus avg_frame_rate (z. B. "25/1" → 25.0)
        fps: Optional[float] = None
        if streams:
            avg_fr = streams[0].get("avg_frame_rate", "0/0")
            try:
                num_s, den_s = avg_fr.split("/")
                num, den = int(num_s), int(den_s)
                if den > 0:
                    fps = num / den
            except (ValueError, ZeroDivisionError):
                pass

        bit_rate: Optional[int] = None
        if streams:
            br = streams[0].get("bit_rate")
            if br and br not in ("N/A", "0", 0):
                bit_rate = int(br)
        if bit_rate is None:
            fmt_br = data.get("format", {}).get("bit_rate")
            if fmt_br and fmt_br not in ("N/A", "0", 0):
                bit_rate = int(fmt_br)

        return {"codec_name": codec_name, "fps": fps, "bit_rate": bit_rate}
    except Exception:
        return {}


def get_audio_stream_info(filepath: Path) -> dict:
    """Ermittelt Sample-Rate, Kanalanzahl und Codec der ersten Audio-Spur.

    Returns:
        Dict mit ``'codec_name'``, ``'sample_rate'`` (int Hz),
        ``'channels'`` (int). Leeres Dict bei Fehler oder fehlender Tonspur.
    """
    import json as _json
    try:
        result = subprocess.run(
            ffprobe_cmd("-v", "quiet", "-select_streams", "a:0",
                        "-show_entries", "stream=codec_name,sample_rate,channels",
                        "-of", "json", str(filepath)),
            capture_output=True, text=True, timeout=30,
        )
        data = _json.loads(result.stdout)
        streams = data.get("streams", [])
        if not streams:
            return {}
        s = streams[0]
        return {
            "codec_name": s.get("codec_name", ""),
            "sample_rate": int(s.get("sample_rate") or 48000),
            "channels": int(s.get("channels") or 2),
        }
    except Exception:
        return {}


def estimate_duration_from_filesize(filepath: Path, fps: int) -> Optional[float]:
    """Schätzt die Dauer einer MJPEG-Datei anhand Dateigröße + Auflösung.

    Empirische Werte für MJPEG (~0,3 Bytes pro Pixel):
      - 1080p (1920×1080): ~620 KB/Frame
      - 4K   (3840×2160): ~2,5 MB/Frame
    """
    try:
        size = filepath.stat().st_size
        if size <= 0 or fps <= 0:
            return None

        resolution = get_resolution(filepath)
        if resolution:
            w, h = resolution
            avg_frame_bytes = int(w * h * 0.3)
        else:
            avg_frame_bytes = 120_000  # Fallback

        est_frames = size / avg_frame_bytes
        return est_frames / fps
    except Exception:
        return None


def count_frames(filepath: Path,
                 cancel_flag: Optional[threading.Event] = None,
                 log_callback=None) -> Optional[int]:
    """Zählt alle JPEG-SOI-Marker (0xFF 0xD8) in einer MJPEG-Datei.

    Liest die gesamte Datei in 64-MB-Blöcken und zählt exakt alle
    Frames.  JPEG Byte-Stuffing garantiert, dass 0xFF 0xD8 nur als
    SOI-Marker auftreten kann (innerhalb der Entropie-Daten wird
    jedes 0xFF zu 0xFF 0x00 escaped).

    Bei großen Dateien (>200 GB) auf externen HDDs kann dies
    15-25 Minuten dauern.  Fortschritt wird über log_callback
    gemeldet, Abbruch über cancel_flag.
    """
    _SOI = b'\xff\xd8'
    _CHUNK = 64 * 1024 * 1024  # 64 MB
    try:
        file_size = filepath.stat().st_size
        if file_size <= 0:
            return None

        if log_callback:
            size_gb = file_size / (1024 ** 3)
            log_callback(f"Zähle Frames ({size_gb:.1f} GB) für Audio-Sync …")

        total_soi = 0
        bytes_read = 0
        last_pct = -1
        prev_tail = b''  # letztes Byte des vorigen Chunks (Grenzfall)
        with open(filepath, 'rb') as f:
            while True:
                if cancel_flag and cancel_flag.is_set():
                    return None
                chunk = f.read(_CHUNK)
                if not chunk:
                    break
                # Grenzfall: SOI-Marker über Chunk-Grenze hinweg
                if prev_tail == b'\xff' and chunk[:1] == b'\xd8':
                    total_soi += 1
                total_soi += chunk.count(_SOI)
                prev_tail = chunk[-1:] if chunk else b''
                bytes_read += len(chunk)
                pct = int(bytes_read * 100 / file_size)
                if log_callback and pct >= last_pct + 10:
                    last_pct = pct
                    log_callback(f"  Frame-Scan: {pct}% "
                                 f"({total_soi:,} Frames bisher)")

        if total_soi < 2:
            return None

        if log_callback:
            log_callback(f"Frame-Scan abgeschlossen: "
                         f"{total_soi:,} Frames gezählt")
        return total_soi
    except Exception:
        return None


# ═════════════════════════════════════════════════════════════════
#  ffmpeg-Prozess mit Fortschritts-Parsing
# ═════════════════════════════════════════════════════════════════

def run_ffmpeg(cmd: list, duration: Optional[float] = None,
               cancel_flag: Optional[threading.Event] = None,
               log_callback=None,
               progress_callback=None) -> int:
    """Führt ffmpeg als Popen aus mit Fortschrittsanzeige und Abbruch.

    Liest den Fortschritt aus der stderr-Statuszeile (``time=HH:MM:SS.xx``),
    weil ``-progress pipe:1`` bei MJPEG-Rohdaten nur N/A liefert.

    Args:
        cmd: ffmpeg-Kommandozeile.
        duration: Geschätzte Gesamtdauer in Sekunden (für %-Berechnung).
        cancel_flag: threading.Event – wenn gesetzt, wird der Prozess abgebrochen.
        log_callback: Callable für Log-Nachrichten.
        progress_callback: Callable(percent: int) für Fortschritt 0–100.

    Returns:
        Exit-Code des Prozesses (``-1`` bei Abbruch).
    """
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        # stderr im Binary-Mode lesen weil ffmpeg \r ohne \n benutzt
        text=False,
        **popen_process_group_kwargs(),
    )

    cancelled = False

    # Watcher-Thread: überwacht das cancel_flag unabhängig von der
    # Read-Schleife und killt den Prozess sofort.
    def _cancel_watcher():
        nonlocal cancelled
        if not cancel_flag:
            return
        while proc.poll() is None:
            if cancel_flag.wait(timeout=0.25):
                cancelled = True
                terminate_process_tree(proc)
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    terminate_process_tree(proc, force=True)
                return

    watcher = threading.Thread(target=_cancel_watcher, daemon=True)
    watcher.start()

    last_pct = -1
    stderr_tail: list[str] = []  # letzte Fehler-/Warnzeilen für Logging
    warning_lines: list[str] = []

    def _remember_line(line: str) -> None:
        stderr_tail.append(line)
        if len(stderr_tail) > 40:
            stderr_tail.pop(0)
        lowered = line.lower()
        if any(marker in lowered for marker in _WARNING_MARKERS):
            if line not in warning_lines:
                warning_lines.append(line)

    try:
        # ffmpeg schreibt Statusupdates auf stderr mit \r (kein \n).
        # Wir lesen block-weise in einen Puffer und splitten bei \r oder \n.
        buf = b""
        while True:
            chunk = proc.stderr.read(512)
            if not chunk:
                break
            buf += chunk
            while b"\r" in buf or b"\n" in buf:
                idx_r = buf.find(b"\r")
                idx_n = buf.find(b"\n")
                if idx_r == -1:
                    idx = idx_n
                elif idx_n == -1:
                    idx = idx_r
                else:
                    idx = min(idx_r, idx_n)

                line_bytes = buf[:idx]
                # \r\n als ein Trenner behandeln
                if idx < len(buf) - 1 and buf[idx:idx + 2] == b"\r\n":
                    buf = buf[idx + 2:]
                else:
                    buf = buf[idx + 1:]

                line = line_bytes.decode("utf-8", errors="replace").strip()
                if not line:
                    continue

                # Statuszeile mit time= parsen
                m = _RE_TIME.search(line)
                if m and duration and duration > 0:
                    h, mi, s, frac = m.groups()
                    current_secs = (int(h) * 3600 + int(mi) * 60
                                    + int(s) + int(frac) / (10 ** len(frac)))
                    pct = min(99, int(current_secs / duration * 100))
                    if pct != last_pct:
                        last_pct = pct
                        if progress_callback:
                            progress_callback(pct)

                # Nicht-Statuszeilen für Warn-/Fehlerlog merken.
                if not _RE_STATUS.search(line):
                    _remember_line(line)

        proc.wait()
    except Exception:
        terminate_process_tree(proc, force=True)
        proc.wait()

    watcher.join(timeout=2)

    if cancelled:
        return -1

    # Erfolg → 100 %
    if proc.returncode == 0 and progress_callback:
        progress_callback(100)

    if proc.returncode == 0 and log_callback and warning_lines:
        for warn_line in warning_lines[-20:]:
            log_callback(f"  {warn_line}")

    # Bei Fehler stderr ausführlicher ausgeben
    if proc.returncode != 0 and log_callback and stderr_tail:
        for err_line in stderr_tail[-20:]:
            log_callback(f"  {err_line}")

    return proc.returncode


def validate_media_output(
    filepath: Path,
    *,
    require_video: bool = True,
    decode_probe: bool = False,
    log_callback=None,
) -> bool:
    """Prüft, ob eine erzeugte Mediendatei strukturell lesbar ist.

    Erkennt insbesondere abgebrochene MP4-Dateien ohne ``moov``-Atom und
    protokolliert ffprobe-Warnungen sichtbar im App-Log.
    """
    def log(msg: str) -> None:
        if log_callback:
            log_callback(msg)

    if not filepath.exists() or filepath.stat().st_size <= 0:
        log(f"⚠ Mediendatei fehlt oder ist leer: {filepath.name}")
        return False

    try:
        result = subprocess.run(
            ffprobe_cmd(
                "-v", "warning",
                "-show_entries", "stream=codec_type:format=format_name,duration,size,bit_rate",
                "-of", "json",
                str(filepath),
            ),
            capture_output=True,
            text=True,
            timeout=15,
        )
    except subprocess.TimeoutExpired:
        log(f"⚠ ffprobe Timeout bei {filepath.name}")
        return False
    except Exception as exc:
        log(f"⚠ ffprobe Fehler bei {filepath.name}: {exc}")
        return False

    stderr_lines = [line.strip() for line in result.stderr.splitlines() if line.strip()]
    for line in stderr_lines[-20:]:
        log(f"  {line}")

    if result.returncode != 0:
        log(f"⚠ ffprobe meldet defekte Ausgabedatei: {filepath.name}")
        return False

    try:
        payload = json.loads(result.stdout or "{}")
    except Exception:
        log(f"⚠ ffprobe lieferte ungueltige Metadaten fuer {filepath.name}")
        return False

    streams = payload.get("streams") or []
    if require_video and not any(s.get("codec_type") == "video" for s in streams):
        log(f"⚠ Keine Video-Spur in {filepath.name}")
        return False

    fmt = payload.get("format") or {}
    try:
        duration = float(fmt.get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0.0
    if duration <= 0:
        log(f"⚠ Ungueltige Dauer fuer {filepath.name}")
        return False

    if decode_probe:
        try:
            decode = subprocess.run(
                ffmpeg_cmd(
                    "-v", "error",
                    "-i", str(filepath),
                    "-map", "0:v:0",
                    "-frames:v", "1",
                    "-f", "null", "-",
                ),
                capture_output=True,
                text=True,
                timeout=15,
            )
        except subprocess.TimeoutExpired:
            log(f"⚠ ffmpeg Decode-Probe Timeout bei {filepath.name}")
            return False
        except Exception as exc:
            log(f"⚠ ffmpeg Decode-Probe Fehler bei {filepath.name}: {exc}")
            return False

        if decode.stderr.strip():
            for line in decode.stderr.splitlines()[-20:]:
                line = line.strip()
                if line:
                    log(f"  {line}")
        if decode.returncode != 0:
            log(f"⚠ ffmpeg kann {filepath.name} nicht sauber decodieren")
            return False

    return True
