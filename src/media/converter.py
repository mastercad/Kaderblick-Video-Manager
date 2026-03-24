"""Video-Konvertierung: Job-Datenklasse und Konvertierungsfunktionen.

Unterstützt sowohl MJPEG-Rohstreams (Pi-Kameras) als auch reguläre
Video-Container (MP4, MKV, AVI, MOV) mit eingebetteter Tonspur.
"""

import json
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from ..settings import AppSettings
from ..settings.profiles import resolution_dimensions
from .encoder import (
    build_video_encoder_args,
    build_aac_audio_args,
    build_mp4_output_args,
)
from .ffmpeg_runner import (
    find_audio, get_duration, estimate_duration_from_filesize,
    count_frames, run_ffmpeg, has_audio_stream, ffmpeg_cmd,
    get_video_stream_info, get_audio_stream_info,
    validate_media_output,
)
from ..integrations.youtube_title_editor import build_output_filename_from_title

# Rohe MJPEG-Streams benötigen -framerate/-f mjpeg Input-Flags.
# Alles andere ist ein regulärer Container.
_MJPEG_EXTS = {".mjpg", ".mjpeg"}
_EMBEDDED_METADATA_SOFTWARE = "Kaderblick — Video Manager "


def _build_scale_pad_filter(width: int, height: int) -> str:
    return (
        f"scale=w={width}:h={height}:force_original_aspect_ratio=decrease,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=black,"
        "setsar=1"
    )


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


def build_embedded_metadata_args(job: ConvertJob) -> list[str]:
    title = (job.youtube_title or "").strip()
    description = (job.youtube_description or "").strip()
    playlist = (job.youtube_playlist or "").strip()
    comment = description
    if comment:
        comment = f"{comment}\n\nErstellt mit {_EMBEDDED_METADATA_SOFTWARE}"
    else:
        comment = f"Erstellt mit {_EMBEDDED_METADATA_SOFTWARE}"

    keywords = _embedded_keywords(job)

    args = [
        "-metadata", f"software={_EMBEDDED_METADATA_SOFTWARE}",
        "-metadata", f"encoded_by={_EMBEDDED_METADATA_SOFTWARE}",
        "-metadata", f"encoder={_EMBEDDED_METADATA_SOFTWARE}",
        "-metadata", f"author={_EMBEDDED_METADATA_SOFTWARE}",
        "-metadata", f"artist={_EMBEDDED_METADATA_SOFTWARE}",
        "-metadata", f"comment={comment}",
    ]
    if title:
        args += ["-metadata", f"title={title}"]
    if description:
        args += ["-metadata", f"description={description}"]
    if playlist:
        args += ["-metadata", f"album={playlist}"]
    if keywords:
        args += ["-metadata", f"keywords={', '.join(keywords)}"]
    return args


def _embedded_keywords(job: ConvertJob) -> list[str]:
    raw_tags = [str(tag or "").strip() for tag in getattr(job, "youtube_tags", [])]
    tags = [tag for tag in raw_tags if tag]
    if not tags:
        hashtags = re.findall(r"#([\wÄÖÜäöüß]+)", job.youtube_description or "")
        tags = [tag.strip() for tag in hashtags if tag.strip()]

    unique: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        key = tag.casefold()
        if key in seen:
            continue
        seen.add(key)
        unique.append(tag)
    return unique


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
    out_path.parent.mkdir(parents=True, exist_ok=True)

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
            source_bit_rate = src_info.get("bit_rate")
            if source_bit_rate is not None:
                src_maxrate_kbps = source_bit_rate // 1000
                codec = src_info.get("codec_name", "?")
                bitrate_mbit = source_bit_rate // 1_000_000
                log(f"Quell-Codec: {codec}, "
                    f"Bitrate: ~{bitrate_mbit} Mbit/s")

    # Quell-FPS für Container-Formate übernehmen (nicht aus Settings erzwingen)
    if not is_raw_mjpeg:
        if not src_info:
            src_info = get_video_stream_info(src)
        src_fps = src_info.get("fps")
        if src_fps and src_fps > 0:
            effective_fps = src_fps
            log(f"Quell-FPS: {src_fps:.3f}")

    # ── ffmpeg-Kommando aufbauen ──────────────────────────────
    # MP4-Ausgaben werden immer standardisiert re-encodiert, damit Profil,
    # Pixel-Format, CFR und AAC-Spur auf allen Zielsystemen konsistent sind.
    use_stream_copy = False
    target_dimensions = resolution_dimensions(vs.output_resolution)
    if target_dimensions is not None:
        log(f"Ziel-Auflösung: {target_dimensions[0]}x{target_dimensions[1]}")

    cmd = ffmpeg_cmd("-hide_banner", "-y")

    if use_stream_copy:
        # Nur Remux (Container-Wechsel falls nötig), kein Re-Encode
        cmd += ["-i", str(src)]
        if has_embedded_audio:
            cmd += ["-c", "copy"]
        else:
            cmd += ["-c:v", "copy", "-an"]
        cmd += ["-movflags", "+faststart", *build_embedded_metadata_args(job), str(out_path)]
        log("Container → Stream-Copy "
            "(Original-FPS und -Qualität bleiben erhalten)")
    else:
        # Input
        if is_raw_mjpeg:
            cmd += ["-fflags", "+genpts",
                    "-framerate", f"{effective_fps:.6f}", "-f", "mjpeg",
                    "-i", str(src)]
        else:
            cmd += ["-fflags", "+genpts", "-i", str(src)]
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

        if target_dimensions is not None:
            cmd += ["-vf", _build_scale_pad_filter(target_dimensions[0], target_dimensions[1])]

        # Explizite Stream-Auswahl vermeidet Metadaten-/Spur-Reihenfolge-Probleme.
        if wav_path:
            cmd += ["-map", "0:v:0", "-map", "1:a:0"]
        elif has_embedded_audio:
            cmd += ["-map", "0:v:0", "-map", "0:a:0?"]
        else:
            cmd += ["-map", "0:v:0"]

        # Audio-Handling
        # Filter-Kette: volume (Verstärkung) → loudnorm (EBU R128)
        _amplify_filter = f"volume={aus.amplify_db}dB,loudnorm"

        if wav_path:
            if vs.output_format == "mp4":
                cmd += build_aac_audio_args(aus.audio_bitrate)
            else:
                cmd += ["-c:a", "aac", "-b:a", aus.audio_bitrate]
            if aus.amplify_audio:
                cmd += ["-af", _amplify_filter]
            cmd += ["-shortest"]
        elif has_embedded_audio:
            if vs.output_format == "mp4":
                cmd += build_aac_audio_args(aus.audio_bitrate)
                if aus.amplify_audio:
                    cmd += ["-af", _amplify_filter]
            elif aus.amplify_audio:
                cmd += ["-c:a", "aac", "-b:a", aus.audio_bitrate,
                        "-af", _amplify_filter]
            else:
                cmd += ["-c:a", "copy"]
        else:
            cmd += ["-an"]

        if vs.output_format == "mp4":
            cmd += build_mp4_output_args()
        cmd += build_embedded_metadata_args(job)
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


def run_repair_output(job: ConvertJob, settings: AppSettings,
                      cancel_flag: Optional[threading.Event] = None,
                      log_callback=None,
                      progress_callback=None) -> bool:
    """Erzeugt eine bereinigte, validierte MP4-Arbeitskopie fuer den weiteren Workflow.

    Die Reparatur arbeitet auf dem aktuellen Ausgabeartefakt, entfernt problematische
    Zusatzstreams und schreibt ein neues ``*_repaired.mp4``. Bereits kompatible
    H.264/AAC-Dateien werden bevorzugt per Stream-Copy bereinigt, bei Bedarf wird
    auf erneute Kodierung mit den standardisierten Kompatibilitaetsregeln gewechselt.
    """
    src = job.output_path or job.source_path
    vs = settings.video

    def log(msg: str):
        if log_callback:
            log_callback(msg)

    if src is None or not src.exists():
        job.error_msg = "Kein gueltiges Reparatur-Eingangsartefakt vorhanden"
        job.status = "Fehler"
        log("❌ Reparatur fehlgeschlagen: Eingangsdatei fehlt")
        return False

    derived_dir = str(getattr(job, "derived_output_dir", "") or "").strip()
    if derived_dir:
        repaired = Path(derived_dir) / f"{src.stem}_repaired.mp4"
    else:
        repaired = src.with_stem(src.stem + "_repaired").with_suffix(".mp4")
    repaired.parent.mkdir(parents=True, exist_ok=True)
    if repaired.exists() and not vs.overwrite:
        if validate_media_output(repaired, require_video=True, decode_probe=True, log_callback=log_callback):
            job.output_path = repaired
            job.status = "Fertig"
            return True
        log(f"⚠ Vorhandene Reparaturdatei ist defekt und wird neu erstellt: {repaired.name}")
        try:
            repaired.unlink()
        except OSError:
            job.error_msg = "Defekte Reparaturdatei ist nicht loeschbar"
            job.status = "Fehler"
            return False

    if repaired.exists() and vs.overwrite:
        try:
            repaired.unlink()
        except OSError:
            pass

    temp_out = repaired.with_stem(repaired.stem + "_tmp")
    if temp_out.exists():
        try:
            temp_out.unlink()
        except OSError:
            pass

    video_info = get_video_stream_info(src)
    audio_info = get_audio_stream_info(src)
    video_codec = str(video_info.get("codec_name") or "").lower()
    audio_codec = str(audio_info.get("codec_name") or "").lower()
    source_fps = video_info.get("fps") or vs.fps
    can_copy_video = video_codec == "h264"
    can_copy_audio = not audio_codec or audio_codec == "aac"

    common_args = ffmpeg_cmd(
        "-hide_banner",
        "-y",
        "-fflags",
        "+genpts",
        "-i",
        str(src),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
    )

    def _finalize_output() -> bool:
        if not validate_media_output(temp_out, require_video=True, decode_probe=True, log_callback=log_callback):
            log(f"❌ Repariertes Ergebnis ist ungueltig: {temp_out.name}")
            try:
                temp_out.unlink()
            except OSError:
                pass
            return False
        try:
            if repaired.exists():
                repaired.unlink()
        except OSError:
            pass
        temp_out.replace(repaired)
        job.output_path = repaired
        job.status = "Fertig"
        return True

    if can_copy_video and can_copy_audio:
        log(f"Repariere Container verlustfrei: {src.name} -> {repaired.name}")
        copy_cmd = [
            *common_args,
            "-c:v",
            "copy",
            "-c:a",
            "copy",
            *build_mp4_output_args(faststart=True),
            *build_embedded_metadata_args(job),
            str(temp_out),
        ]
        rc = run_ffmpeg(
            copy_cmd,
            duration=get_duration(src),
            cancel_flag=cancel_flag,
            log_callback=log_callback,
            progress_callback=progress_callback,
        )
        if rc == 0 and _finalize_output():
            return True
        log("⚠ Verlustfreie Reparatur nicht ausreichend, kodiere kompatible Ersatzdatei neu")
        try:
            if temp_out.exists():
                temp_out.unlink()
        except OSError:
            pass

    log(f"Repariere Ausgabe per Neu-Kodierung: {src.name} -> {repaired.name}")
    encoder, video_args = build_video_encoder_args(
        vs.encoder,
        preset=vs.preset,
        crf=vs.crf,
        lossless=False,
        fps=source_fps,
        maxrate_kbps=(video_info.get("bit_rate") or 0) // 1000 or None,
        no_bframes=vs.no_bframes,
        keyframe_interval=vs.keyframe_interval,
        log_callback=log_callback,
    )
    log(f"Reparatur-Encoder: {encoder}")
    transcode_cmd = [
        *common_args,
        *video_args,
    ]
    if audio_codec:
        transcode_cmd += build_aac_audio_args("320k")
    else:
        transcode_cmd += ["-an"]
    transcode_cmd += [*build_mp4_output_args(faststart=True), *build_embedded_metadata_args(job), str(temp_out)]
    rc = run_ffmpeg(
        transcode_cmd,
        duration=get_duration(src),
        cancel_flag=cancel_flag,
        log_callback=log_callback,
        progress_callback=progress_callback,
    )
    if rc != 0:
        job.error_msg = "Reparatur fehlgeschlagen"
        job.status = "Fehler"
        return False
    if not _finalize_output():
        job.error_msg = "Repariertes Ergebnis ist ungueltig"
        job.status = "Fehler"
        return False
    return True


# ═════════════════════════════════════════════════════════════════
#  YouTube-optimierte Variante
# ═════════════════════════════════════════════════════════════════

def run_youtube_convert(job: ConvertJob, settings: AppSettings,
                        cancel_flag: Optional[threading.Event] = None,
                        log_callback=None,
                        progress_callback=None,
                        preset: str | None = None,
                        no_bframes: bool | None = None,
                        output_format: str | None = None,
                        output_resolution: str | None = None) -> bool:
    """Erstellt eine YouTube-optimierte Variante des konvertierten Videos."""
    vs = settings.video
    yt = settings.youtube
    mp4 = job.output_path
    if not mp4 or not mp4.exists():
        return False

    target_ext = mp4.suffix if not output_format or output_format == "source" else f".{output_format}"
    derived_dir = str(getattr(job, "derived_output_dir", "") or "").strip()
    if derived_dir:
        yt_path = Path(derived_dir) / f"{mp4.stem}_youtube{target_ext}"
    else:
        yt_path = mp4.with_name(f"{mp4.stem}_youtube{target_ext}")
    yt_path.parent.mkdir(parents=True, exist_ok=True)

    def log(msg: str):
        if log_callback:
            log_callback(msg)

    if yt_path.exists() and not vs.overwrite:
        if validate_media_output(yt_path, require_video=True, decode_probe=True, log_callback=log_callback):
            log(f"YouTube-Version existiert bereits: {yt_path.name}")
            return True
        log(f"⚠ Vorhandene YouTube-Version ist defekt und wird neu erstellt: {yt_path.name}")
        try:
            yt_path.unlink()
        except OSError:
            log(f"⚠ Defekte YouTube-Version konnte nicht geloescht werden: {yt_path.name}")
            return False

    input_duration = get_duration(mp4)
    log(f"Erstelle YouTube-Version: {yt_path.name}")

    src_info = get_video_stream_info(mp4)
    source_fps = src_info.get("fps") or vs.fps
    target_dimensions = resolution_dimensions(output_resolution or vs.output_resolution)
    if target_dimensions is not None:
        log(f"YT-Ziel-Auflösung: {target_dimensions[0]}x{target_dimensions[1]}")
    encoder, encoder_args = build_video_encoder_args(
        vs.encoder,
        preset=preset or vs.preset,
        crf=yt.youtube_crf,
        lossless=False,
        fps=source_fps,
        no_bframes=vs.no_bframes if no_bframes is None else no_bframes,
        keyframe_interval=vs.keyframe_interval,
        log_callback=log_callback,
    )
    log(f"YouTube-Encoder: {encoder}")

    def build_cmd(*, faststart: bool) -> list[str]:
        cmd = ffmpeg_cmd(
            "-hide_banner", "-y",
            "-fflags", "+genpts",
            "-i", str(mp4),
        )
        cmd += encoder_args
        cmd += [
            "-map", "0:v:0",
            "-map", "0:a?",
            "-maxrate", yt.youtube_maxrate,
            "-bufsize", yt.youtube_bufsize,
            *build_aac_audio_args(yt.youtube_audio_bitrate),
        ]
        if target_dimensions is not None:
            cmd += ["-vf", _build_scale_pad_filter(target_dimensions[0], target_dimensions[1])]
        if yt_path.suffix.lower() == ".mp4":
            cmd += build_mp4_output_args(faststart=faststart)
        cmd += [*build_embedded_metadata_args(job), str(yt_path)]
        return cmd

    def run_attempt(*, faststart: bool) -> int:
        return run_ffmpeg(
            build_cmd(faststart=faststart),
            duration=input_duration,
            cancel_flag=cancel_flag,
            log_callback=log_callback,
            progress_callback=progress_callback,
        )

    rc = run_attempt(faststart=True)

    if rc == -1:
        if yt_path.exists():
            yt_path.unlink()
        return False

    if rc != 0:
        if yt_path.exists():
            yt_path.unlink()
        log(f"YouTube-Fehler (exit {rc})")
        if yt_path.suffix.lower() == ".mp4":
            log("YouTube-Version ohne MP4-Faststart erneut versuchen …")
            rc = run_attempt(faststart=False)
            if rc == -1:
                if yt_path.exists():
                    yt_path.unlink()
                return False
            if rc != 0:
                if yt_path.exists():
                    yt_path.unlink()
                log(f"YouTube-Fehler (exit {rc})")
                return False
        else:
            if yt_path.exists():
                yt_path.unlink()
            return False

    if yt_path.exists():
        if not validate_media_output(yt_path, require_video=True, decode_probe=True, log_callback=log_callback):
            try:
                yt_path.unlink()
            except OSError:
                pass
            log(f"⚠ Erzeugte YouTube-Version ist ungueltig: {yt_path.name}")
            return False
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
    no_bframes: bool = True,
    keyframe_interval: int = 1,
    target_resolution: str | None = None,
    metadata_job: ConvertJob | None = None,
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
        if validate_media_output(output, require_video=True, decode_probe=True, log_callback=log_callback):
            log(f"Übersprungen: {output.name} existiert bereits")
            return True
        log(f"⚠ Vorhandenes Merge-Ziel ist defekt und wird neu erstellt: {output.name}")
        try:
            output.unlink()
        except OSError:
            log(f"⚠ Defektes Merge-Ziel konnte nicht geloescht werden: {output.name}")
            return False

    n = len(source_files)
    names = " + ".join(p.name for p in source_files)
    log(f"Zusammenführen (re-encode): {names} → {output.name}")

    source_fps = get_video_stream_info(source_files[0]).get("fps") if source_files else None
    if not source_fps:
        source_fps = 25.0

    target_dimensions = resolution_dimensions(target_resolution)
    if target_dimensions is not None:
        log(f"Merge-Ziel-Auflösung: {target_dimensions[0]}x{target_dimensions[1]}")
        scale_filter = _build_scale_pad_filter(target_dimensions[0], target_dimensions[1])
        filter_parts = [f"[{i}:v]{scale_filter}[v{i}]" for i in range(n)]
        filter_inputs = "".join(f"[v{i}][{i}:a]" for i in range(n))
        filter_parts.append(f"{filter_inputs}concat=n={n}:v=1:a=1[outv][outa]")
        filter_complex = ";".join(filter_parts)
    else:
        filter_inputs = "".join(f"[{i}:v][{i}:a]" for i in range(n))
        filter_complex = f"{filter_inputs}concat=n={n}:v=1:a=1[outv][outa]"

    resolved, enc_args = build_video_encoder_args(
        encoder,
        preset=preset,
        crf=crf,
        lossless=False,
        fps=source_fps,
        no_bframes=no_bframes,
        keyframe_interval=keyframe_interval,
        log_callback=log_callback,
    )

    cmd = ffmpeg_cmd("-hide_banner", "-y")
    for src in source_files:
        cmd += ["-fflags", "+genpts", "-i", str(src)]
    cmd += [
        "-filter_complex", filter_complex,
        "-map", "[outv]",
        "-map", "[outa]",
        *enc_args,
        *build_aac_audio_args("192k"),
        *(build_embedded_metadata_args(metadata_job) if metadata_job is not None else []),
    ]
    if output.suffix.lower() == ".mp4":
        cmd += build_mp4_output_args()
    cmd += [str(output)]

    total_duration = sum(
        current_duration
        for current_duration in (get_duration(src) for src in source_files)
        if current_duration and current_duration > 0
    )

    rc = run_ffmpeg(
        cmd,
        duration=total_duration if total_duration > 0 else None,
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
        if not validate_media_output(output, require_video=True, decode_probe=True, log_callback=log_callback):
            try:
                output.unlink()
            except OSError:
                pass
            log(f"⚠ Zusammengefuehrte Ausgabe ist ungueltig: {output.name}")
            return False
        size_mb = output.stat().st_size / (1024 * 1024)
        log(f"✓ Zusammengeführt: {output.name} ({size_mb:.0f} MB)")
        return True
    return False
