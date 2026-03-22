"""Video-Konvertierung: Job-Datenklasse und Konvertierungsfunktionen.

Unterstützt sowohl MJPEG-Rohstreams (Pi-Kameras) als auch reguläre
Video-Container (MP4, MKV, AVI, MOV) mit eingebetteter Tonspur.
"""

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .settings import AppSettings
from .encoder import build_video_encoder_args
from .ffmpeg_runner import (
    find_audio, get_duration, estimate_duration_from_filesize,
    count_frames, run_ffmpeg, has_audio_stream, ffmpeg_cmd,
    get_video_stream_info, get_audio_stream_info,
)

# Rohe MJPEG-Streams benötigen -framerate/-f mjpeg Input-Flags.
# Alles andere ist ein regulärer Container.
_MJPEG_EXTS = {".mjpg", ".mjpeg"}


# ═════════════════════════════════════════════════════════════════
#  Job-Datenklasse
# ═════════════════════════════════════════════════════════════════

@dataclass
class ConvertJob:
    source_path: Path
    job_type: str = "convert"          # "convert" | "download"
    status: str = "Wartend"
    output_path: Optional[Path] = None
    audio_override: Optional[Path] = None  # Explizite Audio-Datei
    youtube_title: str = ""
    youtube_description: str = ""
    youtube_playlist: str = ""
    youtube_tags: list = field(default_factory=list)
    error_msg: str = ""
    progress_pct: int = 0
    device_name: str = ""              # nur für job_type="download"

    def to_dict(self) -> dict:
        """Serialisiert den Job als JSON-fähiges dict."""
        return {
            "source_path": str(self.source_path),
            "job_type": self.job_type,
            "status": self.status,
            "output_path": str(self.output_path) if self.output_path else "",
            "audio_override": str(self.audio_override) if self.audio_override else "",
            "youtube_title": self.youtube_title,
            "youtube_description": self.youtube_description,
            "youtube_playlist": self.youtube_playlist,
            "device_name": self.device_name,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ConvertJob":
        """Erzeugt einen ConvertJob aus einem dict."""
        return cls(
            source_path=Path(d["source_path"]),
            job_type=d.get("job_type", "convert"),
            status=d.get("status", "Wartend"),
            output_path=Path(d["output_path"]) if d.get("output_path") else None,
            audio_override=Path(d["audio_override"]) if d.get("audio_override") else None,
            youtube_title=d.get("youtube_title", ""),
            youtube_description=d.get("youtube_description", ""),
            youtube_playlist=d.get("youtube_playlist", ""),
            device_name=d.get("device_name", ""),
        )


def save_jobs(jobs: list[ConvertJob], path: Path) -> None:
    """Speichert eine Jobliste als JSON-Datei."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = [j.to_dict() for j in jobs]
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def load_jobs(path: Path) -> list[ConvertJob]:
    """Lädt eine Jobliste aus einer JSON-Datei."""
    data = json.loads(path.read_text())
    return [ConvertJob.from_dict(d) for d in data]


# ═════════════════════════════════════════════════════════════════
#  Konvertierung
# ═════════════════════════════════════════════════════════════════

def run_convert(job: ConvertJob, settings: AppSettings,
                cancel_flag: Optional[threading.Event] = None,
                log_callback=None,
                progress_callback=None) -> bool:
    """Konvertiert eine Video-Datei gemäß den Einstellungen.

    Unterstützt zwei Input-Modi:
    - MJPEG-Rohstrom (.mjpg/.mjpeg): framerate + format flags
    - Container-Format (MP4/MKV/…): Standard-Input, eingebettete
      Tonspur wird erkannt und kann verstärkt werden.
    """
    vs = settings.video
    aus = settings.audio
    yt = settings.youtube
    src = job.source_path

    def log(msg: str):
        if log_callback:
            log_callback(msg)

    if not src.exists():
        job.status = "Fehler"
        job.error_msg = "Datei existiert nicht"
        log(f"FEHLER: {src} existiert nicht!")
        return False

    is_raw_mjpeg = src.suffix.lower() in _MJPEG_EXTS

    ext = "mp4" if vs.output_format == "mp4" else "avi"
    out_path = job.output_path or src.with_suffix(f".{ext}")
    # Kollision vermeiden: Input = Output (z. B. input.mp4 → output.mp4)
    if out_path == src:
        out_path = src.with_stem(f"{src.stem}_converted").with_suffix(f".{ext}")
    job.output_path = out_path

    if out_path.exists() and not vs.overwrite:
        job.status = "Übersprungen"
        log(f"Übersprungen: {out_path.name} existiert bereits")
        return True

    if out_path.exists() and vs.overwrite:
        # Datei vorab löschen – NTFS/FUSE kann existierende Dateien
        # beim Überschreiben durch ffmpeg (-y) blockieren.
        try:
            out_path.unlink()
            log(f"Überschreibe: {out_path.name}")
        except OSError:
            # Datei nicht löschbar (z. B. korrupter NTFS-Eintrag).
            # Ausweich-Dateiname verwenden.
            for suffix_nr in range(1, 100):
                alt = out_path.with_stem(f"{out_path.stem}_{suffix_nr}")
                if not alt.exists():
                    log(f"⚠ {out_path.name} nicht lösch-/überschreibbar "
                        f"(NTFS-Fehler?) → schreibe stattdessen: {alt.name}")
                    out_path = alt
                    break
            else:
                job.status = "Fehler"
                job.error_msg = "Ausgabedatei nicht überschreibbar"
                log(f"FEHLER: {out_path.name} kann weder gelöscht noch "
                    f"umgangen werden")
                return False
    job.output_path = out_path

    # ── Externe Audio-Datei suchen ───────────────────────────
    wav_path = None
    if aus.include_audio:
        if job.audio_override and job.audio_override.exists():
            wav_path = job.audio_override
        else:
            wav_path = find_audio(src, aus.audio_suffix)

    # ── Eingebettete Tonspur erkennen (nur Container-Formate) ─
    has_embedded_audio = False
    if not is_raw_mjpeg and not wav_path:
        has_embedded_audio = has_audio_stream(src)

    log(f"Eingabe:  {src.name}")
    if not is_raw_mjpeg:
        log(f"Format:   Container ({src.suffix})")
    log(f"Ausgabe:  {out_path.name}")
    if wav_path:
        log(f"Audio:    {wav_path.name} (extern)")
    elif has_embedded_audio:
        log(f"Audio:    eingebettete Tonspur")
        if aus.amplify_audio:
            log(f"          → wird verstärkt (volume +{aus.amplify_db:.0f} dB + loudnorm)")

    # Dauer der Eingabedatei ermitteln (für Fortschrittsanzeige)
    # Bevorzugt: Audio-Datei (hat zuverlässige Dauer)
    # Fallback: MJPEG-Quelle (ffprobe kennt oft keine Dauer bei Rohstreams)
    # Letzter Ausweg: Heuristik aus Dateigröße + FPS
    input_duration = None
    audio_duration = None
    if wav_path:
        audio_duration = get_duration(wav_path)
        input_duration = audio_duration
    if not input_duration:
        input_duration = get_duration(src)
    if not input_duration and is_raw_mjpeg:
        input_duration = estimate_duration_from_filesize(src, vs.fps)
        if input_duration and log_callback:
            log(f"Dauer geschätzt: ~{input_duration:.0f}s (aus Dateigröße)")

    # ── Audio-Video-Sync bei Frame-Drops (nur MJPEG) ─────────
    # MJPEG-Aufnahmen können Frame-Drops haben: weniger Frames
    # als erwartet → Video kürzer als Audio → zunehmender Versatz.
    # Lösung: tatsächliche Frames zählen und Input-Framerate so
    # anpassen, dass Video-Dauer = Audio-Dauer.
    effective_fps = vs.fps
    if (is_raw_mjpeg and vs.audio_sync and wav_path
            and audio_duration and audio_duration > 0):
        frame_count = count_frames(src, cancel_flag=cancel_flag,
                                   log_callback=log_callback)
        if cancel_flag and cancel_flag.is_set():
            job.status = "Abgebrochen"
            return False
        if frame_count and frame_count > 0:
            video_duration = frame_count / vs.fps
            drift = abs(video_duration - audio_duration)
            drift_pct = drift / audio_duration * 100
            if drift_pct > 0.1:  # > 0.1% Abweichung → anpassen
                effective_fps = frame_count / audio_duration
                log(f"Audio-Sync: {frame_count} Frames, "
                    f"Audio {audio_duration:.1f}s, "
                    f"Video {video_duration:.1f}s "
                    f"(Δ {drift:.1f}s / {drift_pct:.1f}%)")
                log(f"Framerate angepasst: {vs.fps} → {effective_fps:.4f} FPS")
            else:
                log(f"Audio-Sync: OK ({frame_count} Frames, "
                    f"Δ {drift:.1f}s)")

    # ── Quelldatei analysieren (Codec, FPS, Bitrate) ───────────────────────────
    # Für Container-Formate: tatsächliche Quell-FPS und Bitrate ermitteln.
    src_info: dict = {}
    src_maxrate_kbps: int | None = None

    if vs.output_format == "mp4":
        if is_raw_mjpeg and input_duration and input_duration > 0:
            # Rohstream: Bitrate aus Dateigröße (kein Container-Header)
            raw_kbps = int(src.stat().st_size * 8 / input_duration / 1000)
            src_maxrate_kbps = raw_kbps
            log(f"Quell-Bitrate: ~{raw_kbps // 1000} Mbit/s (MJPEG, aus Dateigröße)")
        else:
            src_info = get_video_stream_info(src)
            if src_info.get("bit_rate"):
                src_maxrate_kbps = src_info["bit_rate"] // 1000
                codec = src_info.get("codec_name", "?")
                log(f"Quell-Codec: {codec}, "
                    f"Bitrate: ~{src_maxrate_kbps // 1000} Mbit/s")

    # Quell-FPS für Container-Formate übernehmen (nicht aus Settings erzwingen)
    if not is_raw_mjpeg:
        if not src_info:
            src_info = get_video_stream_info(src)
        src_fps = src_info.get("fps")
        if src_fps and src_fps > 0:
            effective_fps = src_fps
            log(f"Quell-FPS: {src_fps:.3f}")

    # ── ffmpeg-Kommando aufbauen ──────────────────────────────
    # Für Container-Formate ohne Audio-Verarbeitung: stream-copy.
    # Erhält exakt FPS, Bitrate und Tonqualität der Quelldatei.
    needs_audio_encode = (
        wav_path is not None
        or (has_embedded_audio and aus.amplify_audio)
    )
    use_stream_copy = (
        not is_raw_mjpeg
        and vs.output_format == "mp4"
        and not needs_audio_encode
        and not vs.lossless
    )

    cmd = ffmpeg_cmd("-hide_banner", "-y")

    if use_stream_copy:
        # Nur Remux (Container-Wechsel falls nötig), kein Re-Encode
        cmd += ["-i", str(src)]
        if has_embedded_audio:
            cmd += ["-c", "copy"]
        else:
            cmd += ["-c:v", "copy", "-an"]
        cmd += ["-movflags", "+faststart", str(out_path)]
        log("Container → Stream-Copy "
            "(Original-FPS und -Qualität bleiben erhalten)")
    else:
        # Input
        if is_raw_mjpeg:
            cmd += ["-framerate", f"{effective_fps:.6f}", "-f", "mjpeg",
                    "-i", str(src)]
        else:
            cmd += ["-i", str(src)]
        if wav_path:
            cmd += ["-i", str(wav_path)]

        # Video-Encoder
        if vs.output_format == "mp4":
            encoder, encoder_args = build_video_encoder_args(
                vs.encoder,
                preset=vs.preset,
                crf=vs.crf,
                lossless=vs.lossless,
                fps=effective_fps,
                maxrate_kbps=src_maxrate_kbps,
                no_bframes=vs.no_bframes,
                keyframe_interval=vs.keyframe_interval,
                log_callback=log_callback,
            )
            log(f"Encoder:  {encoder}")
            cmd += encoder_args
        else:
            cmd += ["-c:v", "mjpeg", "-q:v", "2", "-r", str(vs.fps)]

        # Audio-Handling
        # Filter-Kette: volume (Verstärkung) → loudnorm (EBU R128)
        _amplify_filter = f"volume={aus.amplify_db}dB,loudnorm"

        if wav_path:
            cmd += ["-c:a", "aac", "-b:a", aus.audio_bitrate]
            if aus.amplify_audio:
                cmd += ["-af", _amplify_filter]
            cmd += ["-shortest"]
        elif has_embedded_audio:
            if aus.amplify_audio:
                cmd += ["-c:a", "aac", "-b:a", aus.audio_bitrate,
                        "-af", _amplify_filter]
            else:
                cmd += ["-c:a", "copy"]
        else:
            cmd += ["-an"]

        cmd += [str(out_path)]
    log("Starte ffmpeg …")
    log(f"  CMD: {' '.join(cmd)}")

    rc = run_ffmpeg(cmd, duration=input_duration,
                    cancel_flag=cancel_flag,
                    log_callback=log_callback,
                    progress_callback=progress_callback)

    if rc == -1:
        job.status = "Abgebrochen"
        if out_path.exists():
            out_path.unlink()
        return False

    if rc != 0:
        job.status = "Fehler"
        job.error_msg = f"ffmpeg exit {rc}"
        log(f"FEHLER (exit {rc})")
        return False

    if not out_path.exists():
        job.status = "Fehler"
        job.error_msg = "Ausgabedatei nicht erstellt"
        return False

    size_mb = out_path.stat().st_size / (1024 * 1024)
    dur = get_duration(out_path)
    dur_str = f", {dur:.0f}s" if dur else ""
    log(f"✓ Fertig: {out_path.name} ({size_mb:.0f} MB{dur_str})")

    if yt.create_youtube and vs.output_format == "mp4":
        yt_ok = run_youtube_convert(job, settings, cancel_flag, log_callback,
                                    progress_callback)
        if not yt_ok:
            log("⚠ YouTube-Version konnte nicht erstellt werden "
                "– beim Upload wird die konvertierte Datei verwendet.")

    job.status = "Fertig"
    return True


# ═════════════════════════════════════════════════════════════════
#  YouTube-optimierte Variante
# ═════════════════════════════════════════════════════════════════

def run_youtube_convert(job: ConvertJob, settings: AppSettings,
                        cancel_flag: Optional[threading.Event] = None,
                        log_callback=None,
                        progress_callback=None) -> bool:
    """Erstellt eine YouTube-optimierte Variante des konvertierten Videos."""
    vs = settings.video
    yt = settings.youtube
    mp4 = job.output_path
    if not mp4 or not mp4.exists():
        return False

    yt_path = mp4.with_stem(mp4.stem + "_youtube")

    def log(msg: str):
        if log_callback:
            log_callback(msg)

    if yt_path.exists() and not vs.overwrite:
        log(f"YouTube-Version existiert bereits: {yt_path.name}")
        return True

    input_duration = get_duration(mp4)
    log(f"Erstelle YouTube-Version: {yt_path.name}")

    src_info = get_video_stream_info(mp4)
    source_fps = src_info.get("fps") or vs.fps
    encoder, encoder_args = build_video_encoder_args(
        vs.encoder,
        preset=vs.preset,
        crf=yt.youtube_crf,
        lossless=False,
        fps=source_fps,
        log_callback=log_callback,
    )
    log(f"YouTube-Encoder: {encoder}")

    cmd = ffmpeg_cmd("-hide_banner", "-y", "-i", str(mp4))
    cmd += encoder_args
    cmd += ["-maxrate", yt.youtube_maxrate,
            "-bufsize", yt.youtube_bufsize,
            "-c:a", "aac", "-b:a", yt.youtube_audio_bitrate,
            "-movflags", "+faststart", str(yt_path)]

    rc = run_ffmpeg(cmd, duration=input_duration,
                    cancel_flag=cancel_flag,
                    log_callback=log_callback,
                    progress_callback=progress_callback)

    if rc == -1:
        if yt_path.exists():
            yt_path.unlink()
        return False

    if rc != 0:
        log(f"YouTube-Fehler (exit {rc})")
        return False

    if yt_path.exists():
        size_mb = yt_path.stat().st_size / (1024 * 1024)
        log(f"✓ YouTube-Version: {yt_path.name} ({size_mb:.0f} MB)")
        return True
    return False


def run_concat(
    source_files: list[Path],
    output: Path,
    cancel_flag: Optional[threading.Event] = None,
    log_callback=None,
    progress_callback=None,
    crf: int = 18,
    preset: str = "fast",
    overwrite: bool = False,
    encoder: str = "auto",
) -> bool:
    """Verbindet mehrere Videos mit dem ffmpeg concat-Filter (mit Re-Encode).

    Verwendet bewusst Re-Encoding statt stream copy (-c copy), weil H.264-Videos
    mit B-Frames und nicht-standardisierten Timebases beim concat-Demuxer + stream
    copy zu korrumpierten Ausgaben führen:
    - Falscher DTS-Sprung an Segment-Grenzen → A/V-Versatz von tausenden Sekunden
    - YouTube meldet fehlende Keyframes und erstellt neue → Szenen doppelt/fehlend

    Der concat-Filter verarbeitet alle Frames durch die FFmpeg-interne Pipeline und
    normalisiert Timestamps korrekt über Segment-Grenzen hinweg.
    """
    def log(msg: str):
        if log_callback:
            log_callback(msg)

    if not source_files:
        return False

    if output.exists() and not overwrite:
        log(f"Übersprungen: {output.name} existiert bereits")
        return True

    n = len(source_files)
    names = " + ".join(p.name for p in source_files)
    log(f"Zusammenführen (re-encode): {names} → {output.name}")

    # filter_complex: [0:v][0:a][1:v][1:a]...concat=n=N:v=1:a=1[outv][outa]
    filter_inputs = "".join(f"[{i}:v][{i}:a]" for i in range(n))
    filter_complex = f"{filter_inputs}concat=n={n}:v=1:a=1[outv][outa]"

    resolved, enc_args = build_video_encoder_args(
        encoder,
        preset=preset,
        crf=crf,
        lossless=False,
        fps=None,
        log_callback=log_callback,
    )  # fps=None: concat-Filter bestimmt fps

    cmd = ffmpeg_cmd("-hide_banner", "-y")
    for src in source_files:
        cmd += ["-i", str(src)]
    cmd += [
        "-filter_complex", filter_complex,
        "-map", "[outv]",
        "-map", "[outa]",
        *enc_args,
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        str(output),
    ]

    rc = run_ffmpeg(
        cmd,
        cancel_flag=cancel_flag,
        log_callback=log_callback,
        progress_callback=progress_callback,
    )
    if rc == -1:
        if output.exists():
            output.unlink()
        return False
    if rc != 0:
        log(f"concat-Fehler (exit {rc})")
        return False
    if output.exists():
        size_mb = output.stat().st_size / (1024 * 1024)
        log(f"✓ Zusammengeführt: {output.name} ({size_mb:.0f} MB)")
        return True
    return False
