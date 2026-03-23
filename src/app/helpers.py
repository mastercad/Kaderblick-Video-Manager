"""Pure helper functions for the main application."""

from __future__ import annotations

from pathlib import Path

from ..workflow import Workflow, WorkflowJob, graph_merge_precedes_convert, graph_reachable_types, normalize_workflow_name


def _summarize_source(job: WorkflowJob) -> str:
    mode_icons = {"files": "🗃", "folder_scan": "📁", "pi_download": "📷"}
    icon = mode_icons.get(job.source_mode, "?")
    if job.source_mode == "files":
        count = len(job.files)
        return f"{icon} {count} Datei{'en' if count != 1 else ''}"
    if job.source_mode == "folder_scan":
        folder = Path(job.source_folder).name if job.source_folder else "–"
        return f"{icon} {folder}"
    if job.source_mode == "pi_download":
        return f"{icon} {job.device_name or '–'}"
    return "?"


def _summarize_pipeline(job: WorkflowJob) -> str:
    graph_types = {
        str(node.get("type", ""))
        for node in getattr(job, "graph_nodes", [])
        if isinstance(node, dict)
    }
    parts = []
    if job.source_mode == "pi_download":
        parts.append("Download")
    if job.convert_enabled:
        parts.append("Konvert.")
    if any(file.merge_group_id for file in job.files):
        parts.append("Kombinieren")
    if job.title_card_enabled:
        parts.append("Titelkarte")
    if "validate_surface" in graph_types:
        parts.append("Quick-Check")
    if "validate_deep" in graph_types:
        parts.append("Deep-Scan")
    if "cleanup" in graph_types:
        parts.append("Cleanup")
    if "repair" in graph_types:
        parts.append("Reparatur")
    if job.create_youtube_version:
        parts.append("YT-Version")
    if "stop" in graph_types:
        parts.append("Stop")
    if job.upload_youtube:
        parts.append("YT-Upload")
    if job.upload_kaderblick:
        parts.append("KB")
    return " → ".join(parts) if parts else "—"


def _format_resume_tooltip(job: WorkflowJob) -> str:
    if not job.step_statuses:
        return job.resume_status or ""
    labels = {
        "transfer": "Transfer",
        "convert": "Konvertierung",
        "merge": "Zusammenführen",
        "titlecard": "Titelkarte",
        "validate_surface": "Quick-Check",
        "validate_deep": "Deep-Scan",
        "cleanup": "Cleanup",
        "repair": "Reparatur",
        "yt_version": "YT-Version",
        "stop": "Stop / Log",
        "youtube_upload": "YouTube-Upload",
        "kaderblick": "Kaderblick",
    }
    lines = []
    for key in (
        "transfer",
        "convert",
        "merge",
        "titlecard",
        "validate_surface",
        "validate_deep",
        "cleanup",
        "repair",
        "yt_version",
        "stop",
        "youtube_upload",
        "kaderblick",
    ):
        value = job.step_statuses.get(key)
        if value:
            lines.append(f"{labels.get(key, key)}: {value}")
            detail = job.step_details.get(key, "") if isinstance(job.step_details, dict) else ""
            if detail:
                lines.append(f"  {detail}")
    if job.resume_status:
        lines.insert(0, f"Letzter Status: {job.resume_status}")
    return "\n".join(lines)


def _planned_job_steps(job: WorkflowJob) -> list[str]:
    has_merge = any(file.merge_group_id for file in job.files)
    reachable_types = graph_reachable_types(job) if getattr(job, "graph_nodes", None) else set()
    has_graph = bool(getattr(job, "graph_nodes", None))
    convert_enabled = "convert" in reachable_types if has_graph else job.convert_enabled
    titlecard_enabled = "titlecard" in reachable_types if has_graph else job.title_card_enabled
    surface_validation_enabled = "validate_surface" in reachable_types if has_graph else False
    deep_validation_enabled = "validate_deep" in reachable_types if has_graph else False
    cleanup_enabled = "cleanup" in reachable_types if has_graph else False
    repair_enabled = "repair" in reachable_types if has_graph else False
    youtube_version_enabled = "yt_version" in reachable_types if has_graph else job.create_youtube_version
    stop_enabled = "stop" in reachable_types if has_graph else False
    youtube_upload_enabled = "youtube_upload" in reachable_types if has_graph else job.upload_youtube
    kaderblick_enabled = "kaderblick" in reachable_types if has_graph else (job.upload_youtube and job.upload_kaderblick)
    if has_graph:
        has_output_stack = (
            convert_enabled
            or has_merge
            or youtube_upload_enabled
            or youtube_version_enabled
            or surface_validation_enabled
            or deep_validation_enabled
            or cleanup_enabled
            or repair_enabled
            or stop_enabled
        )
    else:
        has_output_stack = convert_enabled or has_merge or youtube_upload_enabled

    steps = ["transfer"]
    if has_merge and graph_merge_precedes_convert(job):
        steps.append("merge")
        if convert_enabled:
            steps.append("convert")
    else:
        if convert_enabled:
            steps.append("convert")
        if has_merge:
            steps.append("merge")
    if has_output_stack and titlecard_enabled:
        steps.append("titlecard")
    if has_output_stack and surface_validation_enabled:
        steps.append("validate_surface")
    if has_output_stack and deep_validation_enabled:
        steps.append("validate_deep")
    if has_output_stack and cleanup_enabled:
        steps.append("cleanup")
    if has_output_stack and repair_enabled:
        steps.append("repair")
    if has_output_stack and youtube_version_enabled:
        steps.append("yt_version")
    if has_output_stack and stop_enabled:
        steps.append("stop")
    if has_output_stack and youtube_upload_enabled:
        steps.append("youtube_upload")
    if has_output_stack and kaderblick_enabled:
        steps.append("kaderblick")
    return steps


def _is_finished_step(status: str) -> bool:
    return status in {"done", "reused-target", "skipped"}


def _infer_step_key(job: WorkflowJob, status: str) -> str:
    if job.current_step_key:
        return job.current_step_key

    prefixes = (
        ("Transfer", "transfer"),
        ("Konvertiere", "convert"),
        ("Zusammenführen", "merge"),
        ("Titelkarte", "titlecard"),
        ("Kompatibilität prüfen", "validate_surface"),
        ("Deep-Scan", "validate_deep"),
        ("Bereinige Altdateien", "cleanup"),
        ("Repariere", "repair"),
        ("YT-Version", "yt_version"),
        ("Workflow-Zweig beendet", "stop"),
        ("YouTube-Upload", "youtube_upload"),
        ("Kaderblick", "kaderblick"),
    )
    for prefix, step_key in prefixes:
        if status.startswith(prefix):
            return step_key

    for step_key in reversed(_planned_job_steps(job)):
        if step_key in job.step_statuses:
            return step_key
    return "transfer"


def _compute_job_overall_progress(job: WorkflowJob, status: str, step_pct: int) -> int:
    planned_steps = _planned_job_steps(job)
    if not planned_steps:
        return 100 if status == "Fertig" else 0
    if status == "Fertig":
        return 100

    current_step = _infer_step_key(job, status)
    if current_step not in planned_steps:
        current_step = planned_steps[0]

    completed = sum(1 for step_key in planned_steps if _is_finished_step(job.step_statuses.get(step_key, "")))
    step_index = planned_steps.index(current_step)
    pct = max(0, min(step_pct, 100))

    if _is_finished_step(job.step_statuses.get(current_step, "")) and pct >= 100:
        completed = max(completed, step_index + 1)
        return int(completed / len(planned_steps) * 100)

    completed_before_current = min(completed, step_index)
    return int((completed_before_current + pct / 100.0) / len(planned_steps) * 100)


def _job_has_source_config(job: WorkflowJob) -> bool:
    if job.source_mode == "files":
        return bool(job.files)
    if job.source_mode == "folder_scan":
        return bool(job.source_folder.strip())
    if job.source_mode == "pi_download":
        return bool(job.device_name.strip())
    return False


def _job_is_placeholder(job: WorkflowJob) -> bool:
    return (
        not _job_has_source_config(job)
        and not job.resume_status
        and not job.step_statuses
        and job.name in {"", "Job 1"}
    )


def _jobs_look_compatible(restored: WorkflowJob, fallback: WorkflowJob) -> bool:
    if restored.source_mode != fallback.source_mode:
        return False
    if restored.id and restored.id == fallback.id:
        return True
    if restored.name and fallback.name and restored.name == fallback.name:
        return True
    return restored.name in {"", "Job 1"} or fallback.name in {"", "Job 1"}


def _overlay_resume_state(target: WorkflowJob, source: WorkflowJob) -> WorkflowJob:
    target.enabled = source.enabled
    if source.name:
        target.name = source.name
    target.resume_status = source.resume_status
    target.step_statuses = dict(source.step_statuses) if isinstance(source.step_statuses, dict) else {}
    target.step_details = dict(source.step_details) if isinstance(source.step_details, dict) else {}
    target.progress_pct = source.progress_pct
    target.overall_progress_pct = source.overall_progress_pct
    target.current_step_key = source.current_step_key
    return target


def _repair_restored_workflow(restored: Workflow, fallback: Workflow | None) -> tuple[Workflow, int, int]:
    fallback_jobs = list(fallback.jobs) if fallback else []
    if not restored.jobs:
        return restored, 0, 0

    if all(_job_has_source_config(job) for job in restored.jobs):
        return restored, 0, 0

    repaired_jobs: list[WorkflowJob] = []
    repaired_count = 0
    dropped_resume_state = 0

    for index, job in enumerate(restored.jobs):
        if _job_has_source_config(job):
            repaired_jobs.append(job)
            continue

        candidate = fallback_jobs[index] if index < len(fallback_jobs) else None
        if (job.resume_status or job.step_statuses) and candidate and _job_has_source_config(candidate):
            if _jobs_look_compatible(job, candidate):
                repaired_jobs.append(_overlay_resume_state(WorkflowJob.from_dict(candidate.to_dict()), job))
                repaired_count += 1
                continue

        if job.resume_status or job.step_statuses:
            job.resume_status = ""
            job.step_statuses = {}
            job.step_details = {}
            job.progress_pct = 0
            job.overall_progress_pct = 0
            job.current_step_key = ""
            dropped_resume_state += 1
        repaired_jobs.append(job)

    restored.jobs = repaired_jobs
    if repaired_count and fallback is not None:
        if not normalize_workflow_name(restored.name) and normalize_workflow_name(fallback.name):
            restored.name = fallback.name
        if not restored.shutdown_after and fallback.shutdown_after:
            restored.shutdown_after = fallback.shutdown_after
    return restored, repaired_count, dropped_resume_state