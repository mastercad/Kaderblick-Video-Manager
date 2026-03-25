from queue import Queue
from typing import Any

from ...settings import AppSettings
from ...workflow import FileEntry, WorkflowJob
from ...workflow_steps import PreparedOutput


class _QueuedSignalEmitter:
    def __init__(
        self,
        event_queue: Queue[tuple[str, tuple[Any, ...]]],
        event_name: str,
        flush_callback=None,
    ):
        self._event_queue = event_queue
        self._event_name = event_name
        self._flush_callback = flush_callback

    def emit(self, *args: Any) -> None:
        self._event_queue.put((self._event_name, args))
        if self._flush_callback is not None:
            self._flush_callback(force=True)


class _PipelineWorkerView:
    def __init__(self, owner: Any, event_queue: Queue[tuple[str, tuple[Any, ...]]]):
        self._owner = owner
        self._event_queue = event_queue
        self._cancel = owner._cancel
        self._convert_func = owner._convert_func
        self._concat_func = owner._concat_func
        self._youtube_convert_func = owner._youtube_convert_func
        flush_callback = owner._pump_pipeline_events
        self.log_message = _QueuedSignalEmitter(event_queue, "log_message", flush_callback)
        self.job_progress = _QueuedSignalEmitter(event_queue, "job_progress", flush_callback)
        self.phase_changed = _QueuedSignalEmitter(event_queue, "phase_changed", flush_callback)
        self.file_progress = _QueuedSignalEmitter(event_queue, "file_progress", flush_callback)
        self.convert_progress = _QueuedSignalEmitter(event_queue, "convert_progress", flush_callback)
        self.source_status = _QueuedSignalEmitter(event_queue, "source_status", flush_callback)
        self.source_progress = _QueuedSignalEmitter(event_queue, "source_progress", flush_callback)

    def _set_job_status(self, orig_idx: int, status: str) -> None:
        self._event_queue.put(("job_status", (orig_idx, status)))

    def _set_step_status(self, job: WorkflowJob, step: str, status: str) -> None:
        self._owner._set_step_status(job, step, status)

    def _set_step_detail(self, job: WorkflowJob, step: str, detail: str) -> None:
        self._owner._set_step_detail(job, step, detail)

    def _find_file_entry(self, job: WorkflowJob, file_path: str) -> FileEntry | None:
        return self._owner._find_file_entry(job, file_path)

    def _register_runtime_file_entry(self, job: WorkflowJob, source_node_id: str, file_path: str) -> FileEntry:
        return self._owner._register_runtime_file_entry(job, source_node_id, file_path)

    def _build_job_settings(self, job: WorkflowJob) -> AppSettings:
        return self._owner._build_job_settings(job)

    def _merge_precedes_convert(self, job: WorkflowJob) -> bool:
        return self._owner._merge_precedes_convert(job)

    def _prepared_output_reaches_type(self, prepared: PreparedOutput, target_type: str) -> bool:
        return self._owner._prepared_output_reaches_type(prepared, target_type)

    def _graph_node_id_for_type(self, job: WorkflowJob, node_type: str) -> str:
        return self._owner._graph_node_id_for_type(job, node_type)

    def _validation_branch_has_targets(self, prepared: PreparedOutput, node_type: str, branch: str) -> bool:
        return self._owner._validation_branch_has_targets(prepared, node_type, branch)

    def _get_merge_group_id(self, job: WorkflowJob, file_path: str) -> str:
        return self._owner._get_merge_group_id(job, file_path)

    def _resolve_youtube_title(self, job: WorkflowJob, file_path: str) -> str:
        return self._owner._resolve_youtube_title(job, file_path)