from __future__ import annotations

from pathlib import Path
from typing import Iterable

from .encoder import encoder_display_name, resolve_encoder
from .ffmpeg_runner import get_duration


def format_duration(seconds: float | None) -> str:
    if seconds is None or seconds <= 0:
        return "unbekannt"
    total_seconds = int(round(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def format_path_size(path: Path | None) -> str:
    if path is None or not path.exists():
        return "unbekannt"
    size = path.stat().st_size
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    unit = units[0]
    for current_unit in units[1:]:
        if value < 1024.0:
            break
        value /= 1024.0
        unit = current_unit
    if unit == "B":
        return f"{int(value)} {unit}"
    return f"{value:.2f} {unit}"


def format_media_artifact(path: Path | None) -> str:
    if path is None:
        return "Datei: unbekannt"
    duration = format_duration(get_duration(path)) if path.exists() else "unbekannt"
    size = format_path_size(path)
    return f"Datei: {path.name} | Dauer: {duration} | Größe: {size}"


def format_source_target_summary(source: Path | None, output: Path | None) -> str:
    source_name = source.name if source is not None else "unbekannt"
    return f"Quelle: {source_name} | {format_media_artifact(output)}"


def format_encoder_summary(encoder_setting: str) -> str:
    encoder_id = resolve_encoder(encoder_setting)
    return f"Encoder: {encoder_display_name(encoder_id)}"


def format_list_summary(prefix: str, values: Iterable[str], *, max_items: int = 4) -> str:
    items = [value for value in values if value]
    if not items:
        return f"{prefix}: -"
    if len(items) <= max_items:
        return f"{prefix}: {', '.join(items)}"
    visible = ", ".join(items[:max_items])
    return f"{prefix}: {visible} … (+{len(items) - max_items})"