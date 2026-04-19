"""Halbzeiten zusammenführen mit Titelkarten."""

import hashlib
import tempfile
import threading
import unicodedata
from pathlib import Path
from typing import Optional

from ..settings import AppSettings
from .encoder import (
    resolve_encoder,
    build_encoder_args,
    build_video_encoder_args,
    build_aac_audio_args,
    build_mp4_output_args,
    get_hwaccel_config,
)
from .ffmpeg_runner import (
    run_ffmpeg, get_duration, get_resolution,
    get_video_stream_info, get_audio_stream_info, ffmpeg_cmd,
)


# ═════════════════════════════════════════════════════════════════
#  Hilfsfunktionen
# ═════════════════════════════════════════════════════════════════

def _get_video_dimensions(filepath: Path) -> tuple[int, int]:
    """Gibt (width, height) eines Videos zurück, Fallback (1920, 1080)."""
    res = get_resolution(filepath)
    return res if res else (1920, 1080)


def _make_half_labels(count: int) -> list[str]:
    """Erzeugt Beschriftungen für die Halbzeiten/Teile."""
    if count == 2:
        return ["1. Halbzeit", "2. Halbzeit"]
    elif count == 3:
        return ["1. Halbzeit", "2. Halbzeit", "Verlängerung"]
    else:
        return [f"{i + 1}. Teil" for i in range(count)]


def _normalize_text(s: str) -> str:
    """Unicode-Normalisierung und Whitespace-Bereinigung für drawtext."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u00A0", " ")
    for ch in ["\u200B", "\u200C", "\u200D", "\uFEFF"]:
        s = s.replace(ch, "")
    return " ".join(s.split()).strip()


def _wrap_lines(text: str, font_size: int, max_width_px: int) -> list[str]:
    """Bricht Text auf maximal 3 Zeilen um (greedy word-wrap)."""
    text = text.strip()
    if not text:
        return []
    avg_char = max(4, font_size * 0.5)
    max_chars = max(12, int(max_width_px / avg_char))

    words = text.split()
    lines: list[str] = []
    cur = ""
    for w in words:
        candidate = w if not cur else cur + " " + w
        if len(candidate) <= max_chars:
            cur = candidate
        else:
            if cur:
                lines.append(cur)
            while len(w) > max_chars:
                lines.append(w[:max_chars])
                w = w[max_chars:]
            cur = w
    if cur:
        lines.append(cur)
    while len(lines) > 3:
        lines[-2] = lines[-2] + " " + lines[-1]
        lines.pop()
    return lines


def generate_title_card(
    output_path: Path,
    subtitle: str,
    duration: float,
    width: int,
    height: int,
    fps: int,
    *,
    title: str = "",
    logo_path: str = "",
    bg_color: str = "#000000",
    fg_color: str = "#FFFFFF",
    encoder: str = "auto",
    pix_fmt: str = "yuv420p",
    audio_sample_rate: int = 48000,
    cancel_flag: Optional[threading.Event] = None,
    log_callback=None,
    progress_callback=None,
    work_dir: Optional[Path] = None,
) -> bool:
    """Erzeugt eine Titelkarte mit Logo, Titel (Mannschaften) und Untertitel.

    Parameters
    ----------
    title:     Großer Text oben, z.B. "FC Heimstadt vs FC Auswärts" (optional)
    subtitle:  Kleinerer Text darunter, z.B. "1. Halbzeit" oder "Kamera 1"
    logo_path: Pfad zu einem Bild das als Logo oben eingeblendet wird (optional)
    """
    tmpdir = work_dir or Path(tempfile.mkdtemp(prefix="titlecard_"))
    title_norm    = _normalize_text(title)
    subtitle_norm = _normalize_text(subtitle)

    title_font    = max(48, min(height // 12, 96))
    subtitle_font = max(36, min(height // 18, 72))
    box_x  = int(width * 0.05)
    box_w  = int(width * 0.90)

    logo_exists = logo_path and Path(logo_path).is_file()
    logo_h      = min(int(height * 0.25), 260) if logo_exists else 0
    gap_logo    = 24 if logo_exists else 0
    title_lh    = int(title_font * 1.15)
    sub_lh      = int(subtitle_font * 1.15)
    title_lines    = _wrap_lines(title_norm,    title_font,    int(box_w))
    subtitle_lines = _wrap_lines(subtitle_norm, subtitle_font, int(box_w))
    gap_ts   = 20 if title_lines and subtitle_lines else 0
    block_h  = (logo_h + gap_logo
                + title_lh * len(title_lines)
                + gap_ts
                + sub_lh * len(subtitle_lines))
    block_y  = max(0, int((height - block_h) / 2))

    # ffmpeg complex filter graph (similar to videoschnitt/src/textclip.py)
    vf_parts: list[str] = []
    cur_y = block_y
    base  = "[0]"

    if logo_exists:
        # Escape the logo path for ffmpeg filter syntax
        safe_logo = logo_path.replace("'", "\\'").replace(":", "\\:")
        vf_parts.append(
            f"movie={safe_logo},scale=-1:{logo_h} [logo];"
            f" [0][logo] overlay=x=(W-w)/2:y={cur_y} [after_logo]"
        )
        base = "[after_logo]"
        cur_y += logo_h + gap_logo

    # Semi-transparent background box behind text
    if title_lines or subtitle_lines:
        box_y  = cur_y - 12
        box_h2 = (title_lh * len(title_lines)
                  + gap_ts
                  + sub_lh * len(subtitle_lines) + 24)
        next_b = "[after_box]"
        vf_parts.append(
            f"{base} drawbox=x={box_x}:y={box_y}:w={box_w}:h={box_h2}"
            f":color=black@0.7:t=fill {next_b}"
        )
        base = next_b

    # Title lines
    content_hash = hashlib.md5(
        f"{title_norm}_{subtitle_norm}".encode("utf-8")).hexdigest()[:8]
    for idx, line in enumerate(title_lines):
        tf = tmpdir / f"tc_title_{content_hash}_{idx}.txt"
        tf.write_text(line + "\n", encoding="utf-8")
        next_b = f"[tt{idx}]"
        y_px   = cur_y + idx * title_lh
        safe   = str(tf).replace("'", "\\'").replace(":", "\\:")
        vf_parts.append(
            f"{base} drawtext=fontfile=/usr/share/fonts/truetype/dejavu/"
            f"DejaVuSans-Bold.ttf:textfile='{safe}':reload=1"
            f":fontcolor={fg_color}:fontsize={title_font}"
            f":x=(w-text_w)/2:y={y_px} {next_b}"
        )
        base = next_b

    cur_y += title_lh * len(title_lines) + gap_ts

    # Subtitle lines
    for idx, line in enumerate(subtitle_lines):
        tf = tmpdir / f"tc_sub_{content_hash}_{idx}.txt"
        tf.write_text(line + "\n", encoding="utf-8")
        next_b = f"[ts{idx}]"
        y_px   = cur_y + idx * sub_lh
        safe   = str(tf).replace("'", "\\'").replace(":", "\\:")
        vf_parts.append(
            f"{base} drawtext=fontfile=/usr/share/fonts/truetype/dejavu/"
            f"DejaVuSans.ttf:textfile='{safe}':reload=1"
            f":fontcolor={fg_color}:fontsize={subtitle_font}"
            f":x=(w-text_w)/2:y={y_px} {next_b}"
        )
        base = next_b

    vf = "; ".join(vf_parts) if vf_parts else "null"

    encoder, enc_args = build_video_encoder_args(
        encoder,
        preset="veryfast",
        crf=18,
        lossless=False,
        fps=float(fps),
        no_bframes=True,
        keyframe_interval=1,
        log_callback=log_callback,
    )
    cmd = ffmpeg_cmd(
        "-hide_banner", "-y",
        "-f", "lavfi",
        "-i", (
            f"color=c={bg_color.lstrip('#')}:size={width}x{height}"
            f":duration={duration}:rate={fps}"
        ),
        "-f", "lavfi",
        "-i", "anullsrc=channel_layout=stereo"
              f":sample_rate={audio_sample_rate}",
        "-t", str(duration),
        "-vf", vf,
        *enc_args,
        *build_aac_audio_args("128k", sample_rate=audio_sample_rate),
        "-shortest",
        *build_mp4_output_args(),
        str(output_path),
    )
    rc = run_ffmpeg(cmd, duration=float(duration),
                    cancel_flag=cancel_flag,
                    log_callback=log_callback,
                    progress_callback=progress_callback)
    return rc == 0


# Keep the old private name as an alias for compatibility (merge_halves uses it).
def _generate_title_card(output_path: Path, text: str,
                         duration: int, width: int, height: int,
                         fps: int, bg_color: str = "#000000",
                         fg_color: str = "#FFFFFF",
                         audio_sample_rate: int = 48000,
                         cancel_flag: Optional[threading.Event] = None,
                         log_callback=None) -> bool:
    """Backward-compat wrapper – uses generate_title_card with subtitle only."""
    return generate_title_card(
        output_path, subtitle=text, duration=float(duration),
        width=width, height=height, fps=fps,
        bg_color=bg_color, fg_color=fg_color,
        audio_sample_rate=audio_sample_rate,
        cancel_flag=cancel_flag, log_callback=log_callback,
    )


# ═════════════════════════════════════════════════════════════════
#  Merge-Logik
# ═════════════════════════════════════════════════════════════════

def merge_halves(jobs: list, settings: AppSettings,
                 cancel_flag: Optional[threading.Event] = None,
                 log_callback=None,
                 progress_callback=None) -> list[Path]:
    """Gruppiert fertige Jobs nach Ordner und merged sie mit Titelkarten.

    Returns:
        Liste der erzeugten Merge-Dateien (Pfade).
    """
    vs = settings.video

    def log(msg: str):
        if log_callback:
            log_callback(msg)

    encoder = resolve_encoder(vs.encoder, log_callback=log_callback)
    log(f"Merge-Encoder: {encoder}")

    # Nur erfolgreich konvertierte MP4-Jobs
    finished = [j for j in jobs if j.status == "Fertig"
                and j.output_path and j.output_path.exists()
                and j.output_path.suffix.lower() == ".mp4"]

    if not finished:
        log("Merge: Keine fertigen MP4-Dateien zum Zusammenführen.")
        return []

    # Nach Quell-Ordner gruppieren
    groups: dict[Path, list] = {}
    for job in finished:
        folder = job.source_path.parent
        groups.setdefault(folder, []).append(job)

    # Jede Gruppe nach Dateiname (≈ Zeitstempel) sortieren
    for folder in groups:
        groups[folder].sort(key=lambda j: j.source_path.name)

    merged_files: list[Path] = []
    group_idx = 0
    total_groups = len(groups)

    for folder, group_jobs in groups.items():
        if cancel_flag and cancel_flag.is_set():
            break

        if len(group_jobs) < 2:
            log(f"Merge: {folder.name} – nur {len(group_jobs)} Datei, "
                f"übersprungen")
            group_idx += 1
            continue

        folder_name = folder.name
        merge_name = f"{folder_name}_komplett.mp4"
        merge_path = group_jobs[0].output_path.parent / merge_name

        if merge_path.exists() and not vs.overwrite:
            log(f"Merge: {merge_name} existiert bereits, übersprungen")
            merged_files.append(merge_path)
            group_idx += 1
            continue

        log(f"\n══ Merge: {folder_name} ({len(group_jobs)} Teile) ══")

        # ── Alle Dateien analysieren: FPS, Auflösung, Codec, Sample-Rate ──
        # Jede Datei wird einzeln geprüft. Unterschiede führen zum Abbruch,
        # da ffmpeg concat -c copy nur bei identischen Streams funktioniert.
        file_infos: list[dict] = []
        probe_error = False
        for job in group_jobs:
            mp4 = job.output_path
            vid = get_video_stream_info(mp4)
            aud = get_audio_stream_info(mp4)
            w_f, h_f = _get_video_dimensions(mp4)
            fps_f = vid.get("fps")
            fps_f = round(fps_f, 3) if fps_f and fps_f > 0 else None
            info = {
                "path": mp4,
                "codec": vid.get("codec_name", "?"),
                "fps": fps_f,
                "width": w_f,
                "height": h_f,
                "sample_rate": aud.get("sample_rate") or 48000,
                "channels": aud.get("channels") or 2,
            }
            file_infos.append(info)
            log(f"  {mp4.name}: {info['codec']}, "
                f"{w_f}x{h_f}, {fps_f} FPS, "
                f"{info['sample_rate']} Hz")

        # Konsistenz prüfen – bei Abweichungen Fehler ausgeben
        ref = file_infos[0]
        for info in file_infos[1:]:
            mismatches = []
            if info["fps"] is not None and ref["fps"] is not None:
                if abs((info["fps"] or 0) - (ref["fps"] or 0)) > 0.1:
                    mismatches.append(
                        f"FPS: {ref['fps']} vs {info['fps']}")
            if (info["width"], info["height"]) != (ref["width"], ref["height"]):
                mismatches.append(
                    f"Auflösung: {ref['width']}x{ref['height']} "
                    f"vs {info['width']}x{info['height']}")
            if info["sample_rate"] != ref["sample_rate"]:
                mismatches.append(
                    f"Audio-Sample-Rate: {ref['sample_rate']} "
                    f"vs {info['sample_rate']} Hz")
            if mismatches:
                log(f"  FEHLER: {info['path'].name} ist nicht kompatibel "
                    f"mit {ref['path'].name}:")
                for m in mismatches:
                    log(f"    – {m}")
                log(f"  → Merge abgebrochen. Bitte Dateien erst auf "
                    f"identische Parameter umkodieren.")
                probe_error = True
                break

        if probe_error:
            group_idx += 1
            continue

        # Gemeinsame Parameter aus der ersten (Referenz-)Datei übernehmen
        w, h = ref["width"], ref["height"]
        fps_raw = ref["fps"]
        fps = int(round(fps_raw)) if fps_raw and fps_raw > 0 else vs.fps
        src_sample_rate: int = ref["sample_rate"]

        log(f"  → FPS: {fps}, Auflösung: {w}x{h}, "
            f"Sample-Rate: {src_sample_rate} Hz")

        # Titelkarten erzeugen + Concat-Liste aufbauen.
        # YouTube-optimierte Variante (_youtube.mp4) bevorzugen,
        # damit das Merge-Ergebnis dieselbe Größe hat wie die Einzeldateien.
        tmpdir = Path(tempfile.mkdtemp(prefix="merge_"))
        concat_parts: list[Path] = []
        half_labels = _make_half_labels(len(group_jobs))

        for i, job in enumerate(group_jobs):
            if cancel_flag and cancel_flag.is_set():
                break

            label = half_labels[i]
            title_path = tmpdir / f"title_{i:02d}.mp4"

            log(f"  Erstelle Titelkarte: \"{label}\"")
            ok = generate_title_card(
                title_path, subtitle=label,
                duration=float(vs.merge_title_duration),
                width=w, height=h, fps=fps,
                encoder=encoder,
                bg_color=vs.merge_title_bg,
                fg_color=vs.merge_title_fg,
                audio_sample_rate=src_sample_rate,
                cancel_flag=cancel_flag,
                log_callback=log_callback)

            if not ok:
                log("  FEHLER: Titelkarte konnte nicht erstellt werden")
                continue

            concat_parts.append(title_path)
            concat_parts.append(job.output_path)

        if cancel_flag and cancel_flag.is_set():
            break

        if len(concat_parts) < 2:
            log("  Merge abgebrochen: zu wenig Teile")
            group_idx += 1
            continue

        # Zusammenführen via ffmpeg concat-Filter (mit Re-Encode).
        # Bewusst kein stream copy: H.264-Videos mit B-Frames erzeugen beim
        # concat-Demuxer + copy falsche DTS-Sprünge → A/V-Versatz, YouTube-
        # Keyframe-Fehler, doppelte/fehlende Sequenzen.
        log(f"  Zusammenführen (re-encode) → {merge_name}")

        total_dur = 0.0
        for part in concat_parts:
            d = get_duration(part)
            if d:
                total_dur += d

        n_parts = len(concat_parts)
        filter_inputs = "".join(f"[{i}:v][{i}:a]" for i in range(n_parts))
        filter_complex = f"{filter_inputs}concat=n={n_parts}:v=1:a=1[outv][outa]"

        # concat-Filter ist immer CPU → has_cpu_filter=True (kein Zero-Copy).
        hwaccel = get_hwaccel_config(encoder, has_cpu_filter=True)

        cmd = ffmpeg_cmd("-hide_banner", "-y")
        for part in concat_parts:
            cmd += hwaccel.input_flags
            cmd += ["-fflags", "+genpts", "-i", str(part)]
        cmd += [
            "-filter_complex", filter_complex,
            "-map", "[outv]",
            "-map", "[outa]",
            *build_encoder_args(encoder, vs.preset, vs.crf, False, float(fps)),
            *build_aac_audio_args("192k", sample_rate=src_sample_rate),
            *build_mp4_output_args(),
            str(merge_path),
        ]

        rc = run_ffmpeg(cmd, duration=total_dur if total_dur > 0 else None,
                        cancel_flag=cancel_flag,
                        log_callback=log_callback,
                        progress_callback=progress_callback)

        # Temporäre Titelkarten aufräumen
        for tmp_file in tmpdir.iterdir():
            try:
                tmp_file.unlink()
            except Exception:
                pass
        try:
            tmpdir.rmdir()
        except Exception:
            pass

        if rc == 0 and merge_path.exists():
            size_mb = merge_path.stat().st_size / (1024 * 1024)
            dur = get_duration(merge_path)
            dur_str = f", {dur:.0f}s" if dur else ""
            log(f"  ✓ Merge fertig: {merge_name} ({size_mb:.0f} MB{dur_str})")
            merged_files.append(merge_path)
        elif rc == -1:
            log("  Merge abgebrochen")
            if merge_path.exists():
                merge_path.unlink()
        else:
            log(f"  FEHLER beim Merge (exit {rc})")

        group_idx += 1
        if progress_callback:
            progress_callback(int(group_idx / total_groups * 100))

    return merged_files
