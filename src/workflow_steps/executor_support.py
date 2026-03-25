from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from ..media.converter import ConvertJob
from ..integrations.youtube_title_editor import (
    MatchData,
    SegmentData,
    build_playlist_title,
    build_video_description,
    build_video_tags,
    build_video_title,
)
from ..settings import AppSettings
from ..integrations.youtube_title_editor import build_output_filename_from_title
from ..workflow import (
    FileEntry,
    WorkflowJob,
    graph_node_branch_has_targets,
    graph_node_id_for_type,
    graph_merge_reaches_type,
    graph_merge_precedes_convert,
    graph_reachable_types,
    graph_source_has_pre_merge_titlecard,
    graph_source_nodes,
    graph_source_reaches_merge,
    graph_source_reaches_type,
    workflow_output_device_name,
)


class ExecutorSupport:
    @staticmethod
    def allow_reuse_existing(executor: Any) -> bool:
        return bool(getattr(executor, "_allow_reuse_existing", True))

    @staticmethod
    def resolve_copy_destination(settings: AppSettings, job: WorkflowJob) -> Path | None:
        explicit = (job.copy_destination or "").strip()
        if explicit:
            return Path(explicit)
        if job.source_mode == "files":
            default = settings.workflow_raw_dir_for(job.name, workflow_output_device_name(job))
            return Path(default) if default else None
        return None

    @staticmethod
    def resolve_download_destination(settings: AppSettings, job: WorkflowJob) -> Path | None:
        explicit = (job.download_destination or "").strip()
        if explicit:
            return Path(explicit)
        default = settings.workflow_raw_dir_for(job.name, job.device_name)
        return Path(default) if default else None

    @staticmethod
    def resolve_processed_destination(file_path: str | Path) -> Path:
        source_path = Path(file_path)
        source_dir = source_path.parent
        stage_name = source_dir.name.lower()
        if stage_name == "raw":
            return source_dir.parent / "processed"
        if stage_name == "processed":
            return source_dir
        return source_dir / "processed"

    @staticmethod
    def assign_derived_output_dir(cv_job: ConvertJob, directory: Path | None) -> None:
        setattr(cv_job, "derived_output_dir", str(directory) if directory is not None else "")

    @staticmethod
    def derived_output_dir(cv_job: ConvertJob) -> Path | None:
        raw = str(getattr(cv_job, "derived_output_dir", "") or "").strip()
        return Path(raw) if raw else None

    @classmethod
    def derived_output_path(
        cls,
        cv_job: ConvertJob,
        source_path: Path,
        *,
        stem: str | None = None,
        suffix: str = "",
        extension: str | None = None,
    ) -> Path:
        target_stem = stem or source_path.stem
        target_ext = source_path.suffix if extension is None else extension
        derived_dir = cls.derived_output_dir(cv_job)
        if derived_dir is not None:
            return derived_dir / f"{target_stem}{suffix}{target_ext}"
        return source_path.with_name(f"{target_stem}{suffix}{target_ext}")

    @staticmethod
    def resolve_container_extension(selected_format: str, reference_path: Path, *, fallback: str = "mp4") -> str:
        raw = str(selected_format or "").strip().lower()
        if raw in {"source", "original", "originalformat", ""}:
            ext = reference_path.suffix.lower().lstrip(".")
            raw = ext or fallback
        if raw not in {"mp4", "avi"}:
            raw = fallback
        return f".{raw}"

    @staticmethod
    def _has_graph(job: WorkflowJob) -> bool:
        return bool(getattr(job, "graph_nodes", []))

    @staticmethod
    def _fallback_step_enabled(job: WorkflowJob, target_type: str) -> bool:
        if target_type == "convert":
            return bool(job.convert_enabled)
        if target_type == "titlecard":
            return bool(job.title_card_enabled)
        if target_type == "cleanup":
            return False
        if target_type == "repair":
            return False
        if target_type == "yt_version":
            return bool(job.create_youtube_version)
        if target_type == "stop":
            return False
        if target_type == "youtube_upload":
            return bool(job.upload_youtube)
        if target_type == "kaderblick":
            return bool(job.upload_youtube and job.upload_kaderblick)
        if target_type == "merge":
            return any(getattr(entry, "merge_group_id", "") for entry in job.files)
        return False

    @staticmethod
    def files_for_source(job: WorkflowJob, source_node_id: str) -> list[FileEntry]:
        matching = [entry for entry in job.files if entry.graph_source_id == source_node_id]
        if matching:
            return matching
        source_nodes = graph_source_nodes(job)
        if len(source_nodes) == 1 and source_nodes[0][0] == source_node_id:
            return list(job.files)
        return []

    @staticmethod
    def build_convert_job(executor: Any, job: WorkflowJob, file_path: str) -> ConvertJob:
        entry = ExecutorSupport.find_file_entry(job, file_path)
        metadata = ExecutorSupport.resolve_youtube_metadata(job, file_path)
        youtube_title = metadata["title"]
        youtube_playlist = metadata["playlist"]
        youtube_description = metadata["description"]
        youtube_tags = metadata["tags"]

        convert_job = ConvertJob(
            source_path=Path(file_path),
            job_type="convert",
            youtube_title=youtube_title,
            youtube_description=youtube_description,
            youtube_playlist=youtube_playlist,
            youtube_tags=youtube_tags,
        )
        processed_dir = ExecutorSupport.resolve_processed_destination(file_path)
        ExecutorSupport.assign_derived_output_dir(convert_job, processed_dir)
        if entry and entry.output_filename:
            convert_job.output_path = ExecutorSupport.derived_output_path(
                convert_job,
                Path(file_path),
                stem=entry.output_filename,
                extension=f".{job.output_format}",
            )
        elif youtube_title or processed_dir is not None:
            convert_job.output_path = ExecutorSupport.derived_output_path(
                convert_job,
                Path(file_path),
                stem=build_output_filename_from_title(youtube_title, fallback=Path(file_path).stem),
                extension=f".{job.output_format}",
            )
        return convert_job

    @staticmethod
    def find_file_entry(job: WorkflowJob, file_path: str) -> FileEntry | None:
        exact_match = None
        for entry in job.files:
            if entry.source_path == file_path:
                exact_match = entry
                break
        if exact_match is not None:
            return exact_match

        target = Path(file_path)
        try:
            target_resolved = target.resolve(strict=False)
        except Exception:
            target_resolved = target

        resolved_matches: list[FileEntry] = []
        name_matches: list[FileEntry] = []

        for entry in job.files:
            entry_path = Path(entry.source_path)
            try:
                entry_resolved = entry_path.resolve(strict=False)
            except Exception:
                entry_resolved = entry_path

            if entry_resolved == target_resolved:
                resolved_matches.append(entry)
                continue

            if entry_path.name and entry_path.name == target.name:
                name_matches.append(entry)

        if len(resolved_matches) == 1:
            return resolved_matches[0]
        if len(name_matches) == 1:
            return name_matches[0]
        return None

    @classmethod
    def get_merge_group_id(cls, job: WorkflowJob, file_path: str) -> str:
        entry = cls.find_file_entry(job, file_path)
        if entry and entry.merge_group_id:
            return entry.merge_group_id
        source_node_id = cls.source_node_id_for_file(job, file_path)
        if source_node_id and graph_source_reaches_merge(job, source_node_id):
            return "graph-merge"
        return ""

    @classmethod
    def source_node_id_for_file(cls, job: WorkflowJob, file_path: str) -> str:
        entry = cls.find_file_entry(job, file_path)
        if entry and entry.graph_source_id:
            return entry.graph_source_id
        source_nodes = graph_source_nodes(job)
        if len(source_nodes) == 1:
            return source_nodes[0][0]
        return ""

    @classmethod
    def register_runtime_file_entry(cls, job: WorkflowJob, source_node_id: str, file_path: str) -> FileEntry:
        entry = cls.find_file_entry(job, file_path)
        if entry is None:
            entry = FileEntry(source_path=file_path)
            job.files.append(entry)
        entry.source_path = file_path
        entry.graph_source_id = source_node_id
        entry.merge_group_id = "graph-merge" if graph_source_reaches_merge(job, source_node_id) else ""
        entry.title_card_before_merge = graph_source_has_pre_merge_titlecard(job, source_node_id)
        if not entry.title_card_subtitle:
            entry.title_card_subtitle = Path(file_path).stem
        return entry

    @classmethod
    def resolve_youtube_title(cls, job: WorkflowJob, file_path: str) -> str:
        return str(cls.resolve_youtube_metadata(job, file_path)["title"])

    @classmethod
    def resolve_youtube_playlist(cls, job: WorkflowJob, file_path: str) -> str:
        return str(cls.resolve_youtube_metadata(job, file_path)["playlist"])

    @classmethod
    def resolve_youtube_description(cls, job: WorkflowJob, file_path: str) -> str:
        return str(cls.resolve_youtube_metadata(job, file_path)["description"])

    @classmethod
    def resolve_youtube_tags(cls, job: WorkflowJob, file_path: str) -> list[str]:
        return list(cls.resolve_youtube_metadata(job, file_path)["tags"])

    @classmethod
    def resolve_youtube_metadata(cls, job: WorkflowJob, file_path: str) -> dict[str, object]:
        entry = cls.find_file_entry(job, file_path)
        fallback_stem = Path(file_path).stem
        title, playlist, description, tags = cls._default_youtube_metadata(job, fallback_stem)

        entry_title = str(entry.youtube_title or "").strip() if entry else ""
        if entry_title:
            placeholder_title = entry_title == fallback_stem
            default_title_is_richer = bool(title and title != fallback_stem)
            if not (placeholder_title and default_title_is_richer):
                title = entry_title
        if entry and entry.youtube_playlist:
            playlist = entry.youtube_playlist
        if entry and entry.youtube_description:
            description = entry.youtube_description

        if not tags:
            tags = cls._tags_from_title(title)

        return {
            "title": title or fallback_stem,
            "playlist": playlist,
            "description": description,
            "tags": tags,
        }

    @staticmethod
    def _default_youtube_metadata(job: WorkflowJob, fallback_stem: str) -> tuple[str, str, str, list[str]]:
        match = MatchData(**job.youtube_match_data) if job.youtube_match_data else None
        segment = SegmentData(**job.youtube_segment_data) if job.youtube_segment_data else None
        if match is not None and segment is not None:
            return (
                build_video_title(match, segment),
                build_playlist_title(match),
                build_video_description(match, segment),
                build_video_tags(match, segment),
            )
        title = job.default_youtube_title or fallback_stem
        return (
            title,
            job.default_youtube_playlist,
            job.default_youtube_description,
            ExecutorSupport._tags_from_title(title),
        )

    @staticmethod
    def _tags_from_title(youtube_title: str) -> list[str]:
        youtube_tags: list[str] = []
        if youtube_title and " | " in youtube_title:
            parts = [part.strip() for part in youtube_title.split(" | ")]
            youtube_tags = [tag for tag in parts if tag and len(tag) < 50]
        return list(dict.fromkeys(["Fußball", "Sport"] + youtube_tags))

    @classmethod
    def source_reaches_type(cls, job: WorkflowJob, file_path: str, target_type: str) -> bool:
        if not cls._has_graph(job):
            return cls._fallback_step_enabled(job, target_type)
        source_node_id = cls.source_node_id_for_file(job, file_path)
        if source_node_id:
            return graph_source_reaches_type(job, source_node_id, target_type)
        return target_type in graph_reachable_types(job)

    @staticmethod
    def merge_reaches_type(job: WorkflowJob, target_type: str) -> bool:
        if not ExecutorSupport._has_graph(job):
            return ExecutorSupport._fallback_step_enabled(job, target_type)
        return graph_merge_reaches_type(job, target_type)

    @staticmethod
    def job_reaches_type(job: WorkflowJob, target_type: str) -> bool:
        if not ExecutorSupport._has_graph(job):
            return ExecutorSupport._fallback_step_enabled(job, target_type)
        return target_type in graph_reachable_types(job)

    @staticmethod
    def build_job_settings(executor: Any, job: WorkflowJob) -> AppSettings:
        settings = AppSettings(
            video=replace(executor._settings.video),
            audio=replace(executor._settings.audio),
            youtube=replace(executor._settings.youtube),
            cameras=executor._settings.cameras,
            workflow_output_root=executor._settings.workflow_output_root,
            last_directory=executor._settings.last_directory,
        )
        settings.video.encoder = job.encoder
        settings.video.crf = job.crf
        settings.video.preset = job.preset
        settings.video.fps = job.fps
        settings.video.output_format = job.output_format
        settings.video.output_resolution = job.output_resolution
        settings.video.overwrite = job.overwrite
        settings.video.audio_sync = job.audio_sync
        settings.video.no_bframes = job.no_bframes
        settings.video.keyframe_interval = max(1, int(settings.video.keyframe_interval or 0))
        if not ExecutorSupport.allow_reuse_existing(executor):
            settings.video.overwrite = True
        settings.audio.include_audio = job.merge_audio
        settings.audio.amplify_audio = job.amplify_audio
        settings.audio.amplify_db = job.amplify_db
        settings.youtube.create_youtube = job.create_youtube_version
        settings.youtube.upload_to_youtube = job.upload_youtube
        settings.kaderblick = executor._settings.kaderblick
        return settings

    @staticmethod
    def merge_precedes_convert(job: WorkflowJob) -> bool:
        return graph_merge_precedes_convert(job)

    @classmethod
    def prepared_output_reaches_type(cls, prepared: Any, target_type: str) -> bool:
        job = prepared.job
        if not cls._has_graph(job):
            return cls._fallback_step_enabled(job, target_type)
        branch_results = getattr(prepared, "validation_results", {}) or {}
        if getattr(prepared, "graph_origin_kind", "source") == "merge":
            return graph_merge_reaches_type(job, target_type, branch_results)
        origin = getattr(prepared, "graph_origin_node_id", "")
        if origin:
            return graph_source_reaches_type(job, origin, target_type, branch_results)
        return target_type in graph_reachable_types(job)

    @staticmethod
    def graph_node_id_for_type(job: WorkflowJob, node_type: str) -> str:
        return graph_node_id_for_type(job, node_type)

    @staticmethod
    def validation_branch_has_targets(prepared: Any, node_type: str, branch: str) -> bool:
        node_id = graph_node_id_for_type(prepared.job, node_type)
        if not node_id:
            return False
        return graph_node_branch_has_targets(
            prepared.job,
            node_id,
            branch,
            getattr(prepared, "validation_results", {}) or {},
        )
