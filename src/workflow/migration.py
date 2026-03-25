"""Migration alter Workflow-Formate."""

from __future__ import annotations

from pathlib import Path
import uuid


def _migrate_source_to_job(source: dict) -> dict:
    """Konvertiert ein altes WorkflowSource-dict in das neue WorkflowJob-Format."""

    src_type = source.get("source_type", "local")
    src_path = source.get("source_path", "")

    if src_type == "pi_camera":
        mode = "pi_download"
    elif src_path and Path(src_path).is_file():
        mode = "files"
    else:
        mode = "folder_scan"

    files = []
    if mode == "files":
        files = [
            {
                "source_path": src_path,
                "output_filename": source.get("output_filename", ""),
                "youtube_title": source.get("youtube_title", ""),
                "youtube_playlist": source.get("youtube_playlist", ""),
            }
        ]

    return {
        "id": source.get("id", uuid.uuid4().hex[:8]),
        "enabled": source.get("enabled", True),
        "name": source.get("name", ""),
        "source_mode": mode,
        "files": files,
        "device_name": source.get("device_name", ""),
        "download_destination": source.get("destination_path", ""),
        "delete_after_download": source.get("delete_source", False),
        "source_folder": src_path if mode == "folder_scan" else "",
        "file_pattern": source.get("file_extensions", "*.mp4"),
        "copy_destination": source.get("destination_path", "") if source.get("move_to_destination") else "",
        "move_files": source.get("move_to_destination", False),
        "convert_enabled": True,
        "encoder": source.get("encoder", "auto"),
        "crf": source.get("crf", 18),
        "preset": source.get("preset", "medium"),
        "no_bframes": source.get("no_bframes", True),
        "fps": source.get("fps", 25),
        "output_format": source.get("output_format", "mp4"),
        "output_resolution": source.get("output_resolution", "source"),
        "merge_audio": source.get("merge_audio_video", False),
        "amplify_audio": source.get("amplify_audio", False),
        "amplify_db": source.get("amplify_db", 6.0),
        "audio_sync": source.get("audio_sync", False),
        "create_youtube_version": source.get("create_youtube", False),
        "yt_version_encoder": source.get("encoder", "inherit"),
        "yt_version_crf": source.get("yt_version_crf", 0),
        "yt_version_preset": source.get("preset", "medium"),
        "yt_version_no_bframes": source.get("no_bframes", True),
        "yt_version_fps": source.get("yt_version_fps", 0),
        "yt_version_output_format": "source",
        "yt_version_output_resolution": source.get("yt_version_output_resolution", "source"),
        "upload_youtube": source.get("upload_youtube", False),
        "default_youtube_title": source.get("youtube_title", ""),
        "default_youtube_playlist": source.get("youtube_playlist", ""),
        "merge_encoder": source.get("encoder", "inherit"),
        "merge_crf": source.get("merge_crf", 0),
        "merge_preset": source.get("preset", "medium"),
        "merge_no_bframes": source.get("no_bframes", True),
        "merge_fps": source.get("merge_fps", 0),
        "merge_output_format": "source",
        "merge_output_resolution": source.get("merge_output_resolution", "source"),
    }