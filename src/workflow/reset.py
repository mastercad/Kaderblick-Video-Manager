from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..integrations.kaderblick import clear_recorded_kaderblick_id
from ..integrations.youtube import clear_registry_entry_for_output, get_video_id_for_output
from ..settings import AppSettings
from ..workflow_steps.executor_support import ExecutorSupport
from ..workflow_steps.merge_group_step import MergeGroupStep
from .graph import graph_edge_defs, graph_has_post_merge_titlecard, graph_merge_precedes_convert, graph_node_id_for_type, graph_node_map, graph_reachable_types
from .model import WorkflowJob


_STEP_SEQUENCE = (
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
)
_SOURCE_NODE_TYPES = {"source_files", "source_folder_scan", "source_pi_download"}
_STEP_NODE_TYPES = set(_STEP_SEQUENCE) - {"transfer"}


@dataclass(frozen=True)
class ResetResult:
    requested_node_type: str | None
    effective_node_type: str
    cleared_steps: tuple[str, ...]
    deleted_paths: tuple[str, ...]
    cleared_upload_ids: tuple[str, ...]
    note: str = ""


def reset_job_for_rebuild(
    job: WorkflowJob,
    settings: AppSettings,
    *,
    node_type: str | None = None,
) -> ResetResult:
    effective_node_type, note = _resolve_effective_node_type(job, node_type)
    cleared_steps = _affected_steps(job, effective_node_type)
    deleted_paths, cleared_upload_ids = _clear_artifacts(job, settings, cleared_steps)
    _clear_runtime_state(job, cleared_steps)
    return ResetResult(
        requested_node_type=node_type,
        effective_node_type=effective_node_type,
        cleared_steps=tuple(sorted(cleared_steps, key=_step_sort_key)),
        deleted_paths=tuple(sorted(str(path) for path in deleted_paths)),
        cleared_upload_ids=tuple(sorted(cleared_upload_ids)),
        note=note,
    )


def describe_reset_target(job: WorkflowJob, node_type: str | None = None) -> tuple[str, str]:
    effective_node_type, note = _resolve_effective_node_type(job, node_type)
    label = "gesamten Workflow" if node_type is None else _label_for_node_type(node_type)
    return label, note or f"Start ab {_label_for_node_type(effective_node_type)}."


def _resolve_effective_node_type(job: WorkflowJob, node_type: str | None) -> tuple[str, str]:
    if not node_type:
        return "transfer", "Der Workflow wird vollständig zurückgesetzt."
    if node_type in _SOURCE_NODE_TYPES:
        return node_type, "Der Branch wird ab der Quelle neu aufgebaut."
    if node_type == "merge" and job.convert_enabled and not graph_merge_precedes_convert(job):
        return "convert", "Merge startet hier ab Konvertierung neu, weil die Merge-Eingänge aus Konvertierungsartefakten stammen."
    if node_type == "titlecard":
        if not getattr(job, "graph_nodes", None) and any(file.merge_group_id for file in job.files):
            return "merge", "Titelkarte setzt hier auf den Merge zurück, weil das klassische Workflow-Schema das Zielartefakt direkt überschreibt."
        if graph_has_post_merge_titlecard(job):
            return "merge", "Titelkarte setzt hier auf den Merge zurück, weil das Zielartefakt in-place überschrieben worden sein kann."
        if job.convert_enabled:
            return "convert", "Titelkarte setzt hier auf die Konvertierung zurück, weil vorgelagerte Artefakte bereits überschrieben oder entfernt sein können."
        return "transfer", "Titelkarte setzt hier auf den Transfer zurück, weil kein stabiles Zwischenartefakt garantiert ist."
    return node_type, ""


def _affected_steps(job: WorkflowJob, effective_node_type: str) -> set[str]:
    if effective_node_type == "transfer" and not getattr(job, "graph_nodes", None):
        return set(_planned_steps(job))

    if getattr(job, "graph_nodes", None):
        nodes = graph_node_map(job)
        outgoing: dict[str, list[str]] = {}
        for edge in graph_edge_defs(job):
            outgoing.setdefault(edge["source"], []).append(edge["target"])

        if effective_node_type == "transfer":
            return set(_planned_steps(job))

        start_node_id = graph_node_id_for_type(job, effective_node_type)
        if not start_node_id:
            return set(_planned_steps(job))

        affected: set[str] = set()
        stack = [start_node_id]
        visited: set[str] = set()
        while stack:
            node_id = stack.pop()
            if node_id in visited:
                continue
            visited.add(node_id)
            node_type = nodes.get(node_id, "")
            if node_type in _SOURCE_NODE_TYPES:
                affected.add("transfer")
            elif node_type in _STEP_NODE_TYPES:
                affected.add(node_type)
            stack.extend(outgoing.get(node_id, []))
        return affected or set(_planned_steps(job))

    planned = _planned_steps(job)
    if effective_node_type in _SOURCE_NODE_TYPES or effective_node_type == "transfer":
        return set(planned)
    if effective_node_type not in planned:
        return set(planned)
    start_index = planned.index(effective_node_type)
    return set(planned[start_index:])


def _clear_runtime_state(job: WorkflowJob, cleared_steps: set[str]) -> None:
    if not isinstance(job.step_statuses, dict):
        job.step_statuses = {}
    if not isinstance(job.step_details, dict):
        job.step_details = {}
    for step in cleared_steps:
        job.step_statuses.pop(step, None)
        job.step_details.pop(step, None)
    job.resume_status = ""
    job.status = "Wartend"
    job.progress_pct = 0
    job.overall_progress_pct = 0
    job.current_step_key = ""
    job.error_msg = ""
    if "transfer" in cleared_steps:
        job.transfer_status = ""
        job.transfer_progress_pct = 0
    job.run_started_at = ""
    job.run_finished_at = ""
    job.run_elapsed_seconds = 0.0


def _clear_artifacts(job: WorkflowJob, settings: AppSettings, cleared_steps: set[str]) -> tuple[set[Path], set[str]]:
    deleted_paths: set[Path] = set()
    cleared_upload_ids: set[str] = set()

    if "transfer" in cleared_steps:
        for path in _transfer_targets(job, settings):
            if _delete_file(path):
                deleted_paths.add(path)

    bundles = _artifact_bundles(job, settings)
    paths_to_delete: set[Path] = set()
    registry_roots: set[Path] = set()

    for bundle in bundles:
        if "convert" in cleared_steps:
            paths_to_delete.update(bundle.convert_family)
        if "merge" in cleared_steps:
            paths_to_delete.update(bundle.merge_family)
        if "titlecard" in cleared_steps:
            paths_to_delete.update(bundle.titlecard_family)
        if "repair" in cleared_steps:
            paths_to_delete.update(bundle.repair_family)
        if "yt_version" in cleared_steps:
            paths_to_delete.update(bundle.youtube_family)

        if cleared_steps & {"youtube_upload", "kaderblick", "yt_version", "repair", "validate_surface", "validate_deep", "cleanup", "merge", "convert", "titlecard"}:
            registry_roots.update(bundle.registry_roots)

    for output_path in sorted(registry_roots):
        video_id = get_video_id_for_output(output_path)
        if video_id:
            clear_recorded_kaderblick_id(video_id)
            cleared_upload_ids.add(video_id)
        clear_registry_entry_for_output(output_path)

    for path in sorted(paths_to_delete):
        if _delete_file(path):
            deleted_paths.add(path)

    return deleted_paths, cleared_upload_ids


def _delete_file(path: Path) -> bool:
    try:
        if not path.exists() or not path.is_file():
            return False
    except OSError:
        return False
    try:
        path.unlink()
        return True
    except OSError:
        return False


@dataclass(frozen=True)
class _ArtifactBundle:
    convert_family: tuple[Path, ...]
    merge_family: tuple[Path, ...]
    titlecard_family: tuple[Path, ...]
    repair_family: tuple[Path, ...]
    youtube_family: tuple[Path, ...]
    registry_roots: tuple[Path, ...]


def _artifact_bundles(job: WorkflowJob, settings: AppSettings) -> list[_ArtifactBundle]:
    bundles: list[_ArtifactBundle] = []
    source_paths = _job_source_paths(job, settings)
    grouped_merge_roots: dict[str, Path] = {}

    for source_path in source_paths:
        convert_job = ExecutorSupport.build_convert_job(None, job, str(source_path))
        base_output = convert_job.output_path
        if base_output is None:
            continue
        registry_roots = {base_output}
        convert_family = set(_artifact_family(base_output, job))
        titlecard_family = {path for path in convert_family if "_titlecard" in path.stem}
        repair_family = {path for path in convert_family if "_repaired" in path.stem}
        youtube_family = {path for path in convert_family if path.stem.endswith("_youtube")}

        merge_group_id = ExecutorSupport.get_merge_group_id(job, str(source_path))
        merge_family: set[Path] = set()
        if merge_group_id:
            grouped_merge_roots.setdefault(merge_group_id, MergeGroupStep._expected_merged_path(job, convert_job))

        bundles.append(
            _ArtifactBundle(
                convert_family=tuple(sorted(convert_family)),
                merge_family=tuple(),
                titlecard_family=tuple(sorted(titlecard_family)),
                repair_family=tuple(sorted(repair_family)),
                youtube_family=tuple(sorted(youtube_family)),
                registry_roots=tuple(sorted(registry_roots)),
            )
        )

    for merge_root in grouped_merge_roots.values():
        merge_family = set(_artifact_family(merge_root, job))
        titlecard_family = {path for path in merge_family if "_titlecard" in path.stem or path == merge_root}
        repair_family = {path for path in merge_family if "_repaired" in path.stem}
        youtube_family = {path for path in merge_family if path.stem.endswith("_youtube")}
        bundles.append(
            _ArtifactBundle(
                convert_family=tuple(),
                merge_family=tuple(sorted(merge_family)),
                titlecard_family=tuple(sorted(titlecard_family)),
                repair_family=tuple(sorted(repair_family)),
                youtube_family=tuple(sorted(youtube_family)),
                registry_roots=(merge_root,),
            )
        )
    return bundles


def _artifact_family(root: Path, job: WorkflowJob) -> set[Path]:
    titlecard = _suffix_path(root, "_titlecard")
    repaired = _suffix_path(root, "_repaired", ".mp4")
    repaired_titlecard = _suffix_path(titlecard, "_repaired", ".mp4")
    youtube_extension = ExecutorSupport.resolve_container_extension(job.yt_version_output_format, root)
    youtube_variants = {
        _suffix_path(root, "_youtube", youtube_extension),
        _suffix_path(titlecard, "_youtube", youtube_extension),
        _suffix_path(repaired, "_youtube", youtube_extension),
        _suffix_path(repaired_titlecard, "_youtube", youtube_extension),
    }
    return {
        root,
        titlecard,
        repaired,
        repaired_titlecard,
        *youtube_variants,
    }


def _suffix_path(path: Path, suffix: str, extension: str | None = None) -> Path:
    return path.with_name(f"{path.stem}{suffix}{path.suffix if extension is None else extension}")


def _transfer_targets(job: WorkflowJob, settings: AppSettings) -> set[Path]:
    targets: set[Path] = set()
    if job.source_mode in {"files", "folder_scan"}:
        target_dir = ExecutorSupport.resolve_copy_destination(settings, job)
    elif job.source_mode == "pi_download":
        target_dir = ExecutorSupport.resolve_download_destination(settings, job)
    else:
        target_dir = None
    if target_dir is None:
        return targets

    if job.source_mode == "folder_scan" and not job.files:
        pattern = job.file_pattern or "*.mp4"
        try:
            targets.update(path for path in target_dir.glob(pattern) if path.is_file())
        except OSError:
            return targets
        return targets

    if job.source_mode == "pi_download" and not job.files:
        try:
            targets.update(path for path in target_dir.glob("*.mjpg") if path.is_file())
        except OSError:
            return targets
        return targets

    for source_path in _job_source_paths(job, settings):
        targets.add(target_dir / source_path.name)
    return targets


def _job_source_paths(job: WorkflowJob, settings: AppSettings) -> list[Path]:
    result: list[Path] = []
    seen: set[str] = set()
    runtime_target_dir = None
    if job.source_mode in {"files", "folder_scan"}:
        runtime_target_dir = ExecutorSupport.resolve_copy_destination(settings, job)
    elif job.source_mode == "pi_download":
        runtime_target_dir = ExecutorSupport.resolve_download_destination(settings, job)

    for entry in job.files:
        source_path = str(getattr(entry, "source_path", "") or "").strip()
        if not source_path or source_path in seen:
            continue
        seen.add(source_path)
        path = Path(source_path)
        if runtime_target_dir is not None:
            result.append(runtime_target_dir / path.name)
        else:
            result.append(path)
    return result


def _planned_steps(job: WorkflowJob) -> list[str]:
    graph_types = {
        str(node.get("type", ""))
        for node in getattr(job, "graph_nodes", [])
        if isinstance(node, dict)
    }
    has_merge = any(file.merge_group_id for file in job.files) or "merge" in graph_types
    has_graph = bool(getattr(job, "graph_nodes", None))
    reachable_types = graph_reachable_types(job) if has_graph else set()
    convert_enabled = "convert" in reachable_types if has_graph else job.convert_enabled
    titlecard_enabled = "titlecard" in reachable_types if has_graph else job.title_card_enabled
    youtube_version_enabled = "yt_version" in reachable_types if has_graph else job.create_youtube_version
    youtube_upload_enabled = "youtube_upload" in reachable_types if has_graph else job.upload_youtube
    kaderblick_enabled = "kaderblick" in reachable_types if has_graph else (job.upload_youtube and job.upload_kaderblick)

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
    if titlecard_enabled:
        steps.append("titlecard")
    for step in ("validate_surface", "validate_deep", "cleanup", "repair"):
        if step in reachable_types:
            steps.append(step)
    if youtube_version_enabled:
        steps.append("yt_version")
    if "stop" in reachable_types:
        steps.append("stop")
    if youtube_upload_enabled:
        steps.append("youtube_upload")
    if kaderblick_enabled:
        steps.append("kaderblick")
    seen: set[str] = set()
    result: list[str] = []
    for step in steps:
        if step in seen:
            continue
        seen.add(step)
        result.append(step)
    return result


def _step_sort_key(step: str) -> int:
    try:
        return _STEP_SEQUENCE.index(step)
    except ValueError:
        return len(_STEP_SEQUENCE)


def _label_for_node_type(node_type: str) -> str:
    labels = {
        "transfer": "Transfer",
        "source_files": "Dateiquelle",
        "source_folder_scan": "Ordner-Scan",
        "source_pi_download": "Pi-Download",
        "convert": "Konvertierung",
        "merge": "Merge",
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
    return labels.get(node_type, node_type)