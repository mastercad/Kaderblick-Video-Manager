from collections import defaultdict
from pathlib import Path

from ...media.converter import ConvertJob
from ...settings import AppSettings
from ...workflow import FileEntry, WorkflowJob, graph_has_multiple_sources, graph_source_nodes
from ...workflow_steps import ExecutorSupport, PreparedOutput


class WorkflowExecutorSupportMixin:
    @staticmethod
    def _needs_delivery(job: WorkflowJob) -> bool:
        return bool(job.upload_youtube or job.upload_kaderblick)

    def _build_pipeline_kaderblick_sort_index(
        self,
        active: list[tuple[int, WorkflowJob]],
    ) -> dict[tuple[str, str], int]:
        kb_by_game: dict[str, list[str]] = defaultdict(list)
        for _index, job in active:
            if not (job.upload_kaderblick and job.upload_youtube):
                continue

            candidates: list[tuple[str, str]] = []
            if job.files:
                for entry in job.files:
                    game_id = entry.kaderblick_game_id or job.default_kaderblick_game_id
                    name = Path(entry.source_path).name if entry.source_path else ""
                    if game_id and name:
                        candidates.append((game_id, name))
            elif job.source_mode == "folder_scan" and job.source_folder:
                src_dir = Path(job.source_folder)
                pattern = job.file_pattern or "*.mp4"
                if src_dir.exists():
                    for path in sorted(src_dir.glob(pattern)):
                        if not path.is_file():
                            continue
                        game_id = job.default_kaderblick_game_id
                        if game_id:
                            candidates.append((game_id, path.name))

            for game_id, name in candidates:
                kb_by_game[game_id].append(name)

        kb_sort_index: dict[tuple[str, str], int] = {}
        for game_id, names in kb_by_game.items():
            for pos, name in enumerate(sorted(set(names)), start=1):
                kb_sort_index[(game_id, name)] = pos
        return kb_sort_index

    def _estimate_pipeline_total_steps(self, active: list[tuple[int, WorkflowJob]]) -> int:
        total = len(active)
        for _index, job in active:
            file_count = self._estimate_job_file_count(job)
            merge_groups = {
                entry.merge_group_id
                for entry in job.files
                if getattr(entry, "merge_group_id", "")
            }
            merge_member_count = len([
                entry for entry in job.files if getattr(entry, "merge_group_id", "")
            ])
            merge_before_convert = self._merge_precedes_convert(job)

            if job.convert_enabled:
                if merge_before_convert and merge_groups:
                    total += max(file_count - merge_member_count, 0)
                    total += len(merge_groups)
                else:
                    total += file_count
            total += len(merge_groups)

            if self._support.job_reaches_type(job, "repair"):
                total += len(merge_groups) if merge_groups else max(1, file_count)

            if job.upload_youtube:
                total += len(merge_groups) if merge_groups else max(1, file_count)
        return total

    @staticmethod
    def _estimate_job_file_count(job: WorkflowJob) -> int:
        if graph_has_multiple_sources(job):
            total = 0
            source_ids = {node_id for node_id, _node_type in graph_source_nodes(job)}
            total += len([
                entry for entry in job.files if not entry.graph_source_id or entry.graph_source_id in source_ids
            ])
            if any(node_type == "source_folder_scan" for _node_id, node_type in graph_source_nodes(job)) and job.source_folder:
                src_dir = Path(job.source_folder)
                pattern = job.file_pattern or "*.mp4"
                if src_dir.exists():
                    total += len([path for path in src_dir.glob(pattern) if path.is_file()])
            if any(node_type == "source_pi_download" for _node_id, node_type in graph_source_nodes(job)) and job.files:
                total += len([entry for entry in job.files if entry.graph_source_id])
            return max(total, 1)
        if job.files:
            return len(job.files)
        if job.source_mode == "folder_scan" and job.source_folder:
            src_dir = Path(job.source_folder)
            pattern = job.file_pattern or "*.mp4"
            if src_dir.exists():
                return len([path for path in src_dir.glob(pattern) if path.is_file()])
        return 1

    def _build_convert_job(self, job: WorkflowJob, file_path: str) -> ConvertJob:
        return self._support.build_convert_job(self, job, file_path)

    @staticmethod
    def _find_file_entry(job: WorkflowJob, file_path: str) -> FileEntry | None:
        return ExecutorSupport.find_file_entry(job, file_path)

    @staticmethod
    def _register_runtime_file_entry(job: WorkflowJob, source_node_id: str, file_path: str) -> FileEntry:
        return ExecutorSupport.register_runtime_file_entry(job, source_node_id, file_path)

    @classmethod
    def _get_merge_group_id(cls, job: WorkflowJob, file_path: str) -> str:
        return ExecutorSupport.get_merge_group_id(job, file_path)

    @staticmethod
    def _resolve_youtube_title(job: WorkflowJob, file_path: str) -> str:
        return ExecutorSupport.resolve_youtube_title(job, file_path)

    @staticmethod
    def _resolve_youtube_playlist(job: WorkflowJob, file_path: str) -> str:
        return ExecutorSupport.resolve_youtube_playlist(job, file_path)

    @staticmethod
    def _resolve_youtube_description(job: WorkflowJob, file_path: str) -> str:
        return ExecutorSupport.resolve_youtube_description(job, file_path)

    @staticmethod
    def _resolve_youtube_tags(job: WorkflowJob, file_path: str) -> list[str]:
        return ExecutorSupport.resolve_youtube_tags(job, file_path)

    def _build_job_settings(self, job: WorkflowJob) -> AppSettings:
        return self._support.build_job_settings(self, job)

    def _merge_precedes_convert(self, job: WorkflowJob) -> bool:
        return self._support.merge_precedes_convert(job)

    def _run_output_steps(
        self,
        prepared: PreparedOutput,
        yt_service,
        kb_sort_index: dict[tuple[str, str], int],
        *,
        include_title_card: bool = True,
        include_repair: bool = True,
        include_youtube_version: bool = True,
    ) -> int:
        return self._output_step_stack.execute(
            self,
            prepared,
            yt_service,
            kb_sort_index,
            include_title_card=include_title_card,
            include_repair=include_repair,
            include_youtube_version=include_youtube_version,
        )

    def _prepared_output_reaches_type(self, prepared: PreparedOutput, target_type: str) -> bool:
        return self._support.prepared_output_reaches_type(prepared, target_type)

    def _graph_node_id_for_type(self, job: WorkflowJob, node_type: str) -> str:
        return self._support.graph_node_id_for_type(job, node_type)

    def _validation_branch_has_targets(self, prepared: PreparedOutput, node_type: str, branch: str) -> bool:
        return self._support.validation_branch_has_targets(prepared, node_type, branch)

    def _get_youtube_service(self):
        from . import get_youtube_service

        return get_youtube_service(log_callback=self.log_message.emit)

    def _set_job_status(self, orig_idx: int, status: str) -> None:
        self.job_status.emit(orig_idx, status)
        if status.startswith("Transfer"):
            self.source_status.emit(orig_idx, status)

    @staticmethod
    def _set_step_status(job: WorkflowJob, step: str, status: str) -> None:
        if not isinstance(job.step_statuses, dict):
            job.step_statuses = {}
        job.step_statuses[step] = status
        job.current_step_key = step

    @staticmethod
    def _set_step_detail(job: WorkflowJob, step: str, detail: str) -> None:
        if not isinstance(job.step_details, dict):
            job.step_details = {}
        job.step_details[step] = detail