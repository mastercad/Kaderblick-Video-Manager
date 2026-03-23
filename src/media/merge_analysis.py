from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .ffmpeg_runner import (
    get_audio_stream_info,
    get_resolution,
    get_video_stream_info,
    has_audio_stream,
)
from ..workflow import WorkflowJob, graph_merge_precedes_convert


@dataclass(frozen=True)
class MergeCompatibilityReport:
    mergeable: bool
    reasons: tuple[str, ...]


def analyze_merge_sources(source_files: list[Path]) -> MergeCompatibilityReport:
    existing_files = [path for path in source_files if path.exists()]
    if len(existing_files) < 2:
        return MergeCompatibilityReport(True, ())

    inspected: list[dict[str, object]] = []
    reasons: list[str] = []
    for path in existing_files:
        resolution = get_resolution(path)
        video_info = get_video_stream_info(path)
        audio_present = has_audio_stream(path)
        audio_info = get_audio_stream_info(path) if audio_present else {}
        fps = video_info.get("fps")
        if resolution is None or not video_info:
            reasons.append(f"{path.name}: Videoformat konnte nicht sicher analysiert werden")
            continue
        inspected.append(
            {
                "path": path,
                "resolution": resolution,
                "fps": float(fps) if fps else None,
                "audio_present": audio_present,
                "sample_rate": int(audio_info.get("sample_rate") or 0),
                "channels": int(audio_info.get("channels") or 0),
            }
        )

    if reasons:
        return MergeCompatibilityReport(False, tuple(reasons))

    baseline = inspected[0]
    for info in inspected[1:]:
        path = info["path"]
        if info["resolution"] != baseline["resolution"]:
            reasons.append(
                f"{path.name}: Auflösung {info['resolution'][0]}x{info['resolution'][1]} weicht von {baseline['resolution'][0]}x{baseline['resolution'][1]} ab"
            )
        baseline_fps = baseline["fps"]
        current_fps = info["fps"]
        if baseline_fps and current_fps and abs(current_fps - baseline_fps) > 0.05:
            reasons.append(f"{path.name}: FPS {current_fps:.3f} weicht von {baseline_fps:.3f} ab")
        if info["audio_present"] != baseline["audio_present"]:
            reasons.append(f"{path.name}: Audio-Spuren sind nicht konsistent über die Merge-Gruppe")
        if info["audio_present"] and baseline["audio_present"]:
            if info["sample_rate"] != baseline["sample_rate"]:
                reasons.append(
                    f"{path.name}: Audio-Samplerate {info['sample_rate']} Hz weicht von {baseline['sample_rate']} Hz ab"
                )
            if info["channels"] != baseline["channels"]:
                reasons.append(
                    f"{path.name}: Audio-Kanäle {info['channels']} weichen von {baseline['channels']} ab"
                )

    return MergeCompatibilityReport(not reasons, tuple(reasons))


def job_merge_warning(job: WorkflowJob) -> str:
    if not graph_merge_precedes_convert(job):
        return ""
    grouped: dict[str, list[Path]] = {}
    for entry in job.files:
        if not entry.merge_group_id or not entry.source_path:
            continue
        grouped.setdefault(entry.merge_group_id, []).append(Path(entry.source_path))

    warning_lines: list[str] = []
    for merge_group_id, paths in sorted(grouped.items()):
        report = analyze_merge_sources(paths)
        if report.mergeable:
            continue
        warning_lines.append(f"Merge-Gruppe {merge_group_id} ist vor der Konvertierung nicht sicher mergebar:")
        warning_lines.extend(f"- {reason}" for reason in report.reasons)

    return "\n".join(warning_lines)