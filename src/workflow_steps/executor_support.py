from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from ..converter import ConvertJob
from ..settings import AppSettings
from ..workflow import FileEntry, WorkflowJob


class ExecutorSupport:
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
        return entry.merge_group_id if entry and entry.merge_group_id else ""

    @classmethod
    def resolve_youtube_title(cls, job: WorkflowJob, file_path: str) -> str:
        entry = cls.find_file_entry(job, file_path)
        if entry and entry.youtube_title:
            return entry.youtube_title
        return job.default_youtube_title or Path(file_path).stem

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
        settings.audio.include_audio = job.merge_audio
        settings.audio.amplify_audio = job.amplify_audio
        settings.audio.amplify_db = job.amplify_db
        settings.youtube.create_youtube = job.create_youtube_version
        settings.youtube.upload_to_youtube = job.upload_youtube
        settings.kaderblick = executor._settings.kaderblick
        return settings
