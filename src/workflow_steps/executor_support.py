from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from ..media.converter import ConvertJob
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
)


class ExecutorSupport:
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
        youtube_title = (
            entry.youtube_title if entry and entry.youtube_title else job.default_youtube_title or Path(file_path).stem
        )
        youtube_playlist = (
            entry.youtube_playlist if entry and entry.youtube_playlist else job.default_youtube_playlist
        )
        youtube_description = (
            entry.youtube_description if entry and entry.youtube_description else ""
        )

        youtube_tags: list[str] = []
        if youtube_title and " | " in youtube_title:
            parts = [part.strip() for part in youtube_title.split(" | ")]
            youtube_tags = [tag for tag in parts if tag and len(tag) < 50]
        youtube_tags = list(dict.fromkeys(["Fußball", "Sport"] + youtube_tags))

        convert_job = ConvertJob(
            source_path=Path(file_path),
            job_type="convert",
            youtube_title=youtube_title,
            youtube_description=youtube_description,
            youtube_playlist=youtube_playlist,
            youtube_tags=youtube_tags,
        )
        if entry and entry.output_filename:
            out_dir = Path(file_path).parent
            convert_job.output_path = out_dir / f"{entry.output_filename}.{job.output_format}"
        elif youtube_title:
            out_dir = Path(file_path).parent
            convert_job.output_path = out_dir / f"{build_output_filename_from_title(youtube_title, fallback=Path(file_path).stem)}.{job.output_format}"
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
        if not entry.youtube_title:
            entry.youtube_title = Path(file_path).stem
        if not entry.title_card_subtitle:
            entry.title_card_subtitle = Path(file_path).stem
        return entry

    @classmethod
    def resolve_youtube_title(cls, job: WorkflowJob, file_path: str) -> str:
        entry = cls.find_file_entry(job, file_path)
        if entry and entry.youtube_title:
            return entry.youtube_title
        return job.default_youtube_title or Path(file_path).stem

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
            last_directory=executor._settings.last_directory,
        )
        settings.video.encoder = job.encoder
        settings.video.crf = job.crf
        settings.video.preset = job.preset
        settings.video.fps = job.fps
        settings.video.output_format = job.output_format
        settings.video.overwrite = job.overwrite
        settings.video.audio_sync = job.audio_sync
        settings.video.no_bframes = True
        settings.video.keyframe_interval = max(1, int(settings.video.keyframe_interval or 0))
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
