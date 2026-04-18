from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from typing import Any

from .cleanup_output_step import CleanupOutputStep
from .executor_support import ExecutorSupport
from .kaderblick_post_step import KaderblickPostStep
from .models import PreparedOutput
from .output_validation_step import OutputValidationStep
from .repair_output_step import RepairOutputStep
from .stop_output_step import StopOutputStep
from .title_card_step import TitleCardStep
from .youtube_upload_step import YoutubeUploadStep
from .youtube_version_step import YoutubeVersionStep


class OutputStepStack:
    name = "output-stack"
    _STEP_ORDER = (
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

    def __init__(self):
        self._title_card_step = TitleCardStep()
        self._surface_validation_step = OutputValidationStep("validate_surface", deep_scan=False)
        self._deep_validation_step = OutputValidationStep("validate_deep", deep_scan=True)
        self._cleanup_output_step = CleanupOutputStep()
        self._repair_output_step = RepairOutputStep()
        self._youtube_version_step = YoutubeVersionStep()
        self._stop_output_step = StopOutputStep()
        self._youtube_upload_step = YoutubeUploadStep()
        self._kaderblick_post_step = KaderblickPostStep()
        self._processing_steps = {
            "titlecard": self._title_card_step,
            "validate_surface": self._surface_validation_step,
            "validate_deep": self._deep_validation_step,
            "cleanup": self._cleanup_output_step,
            "repair": self._repair_output_step,
            "yt_version": self._youtube_version_step,
            "stop": self._stop_output_step,
        }
        self._delivery_steps = {
            "youtube_upload": self._youtube_upload_step,
            "kaderblick": self._kaderblick_post_step,
        }

    def execute(
        self,
        executor: Any,
        prepared: PreparedOutput,
        yt_service: Any,
        kb_sort_index: dict[tuple[str, str], int],
        *,
        include_title_card: bool = True,
        include_repair: bool = True,
        include_youtube_version: bool = True,
        start_at_step: str = "",
    ) -> int:
        if getattr(prepared.job, "graph_nodes", None):
            prepared.delivery_branches = []
        failures = self.execute_processing_steps(
            executor,
            prepared,
            include_title_card=include_title_card,
            include_repair=include_repair,
            include_youtube_version=include_youtube_version,
            start_at_step=start_at_step,
        )
        if failures or executor._cancel.is_set():
            return failures
        return self.execute_delivery_steps(executor, prepared, yt_service, kb_sort_index, start_at_step=start_at_step)

    def execute_processing_steps(
        self,
        executor: Any,
        prepared: PreparedOutput,
        *,
        include_title_card: bool = True,
        include_repair: bool = True,
        include_youtube_version: bool = True,
        start_at_step: str = "",
        stop_before_merge: bool = False,
    ) -> int:
        if getattr(prepared.job, "graph_nodes", None):
            return self._execute_graph_processing(
                executor,
                prepared,
                include_title_card=include_title_card,
                include_repair=include_repair,
                include_youtube_version=include_youtube_version,
                start_at_step=start_at_step,
                stop_before_merge=stop_before_merge,
            )

        failures = 0
        ordered_steps = [
            (self._title_card_step, include_title_card),
            (self._surface_validation_step, True),
            (self._deep_validation_step, True),
            (self._cleanup_output_step, True),
            (self._repair_output_step, include_repair),
            (self._youtube_version_step, include_youtube_version),
            (self._stop_output_step, True),
        ]

        for step, enabled in ordered_steps:
            if executor._cancel.is_set():
                break
            if not enabled:
                continue
            if self._skip_before_start(step.name, start_at_step):
                continue
            if not executor._prepared_output_reaches_type(prepared, step.name):
                continue
            failures += step.execute(executor, prepared)
            if failures:
                break
            advance_cursor = getattr(executor, "_advance_prepared_output_cursor", None)
            if callable(advance_cursor):
                advance_cursor(prepared, step.name)

        return failures

    def execute_delivery_steps(
        self,
        executor: Any,
        prepared: PreparedOutput,
        yt_service: Any,
        kb_sort_index: dict[tuple[str, str], int],
        *,
        start_at_step: str = "",
    ) -> int:
        if getattr(prepared.job, "graph_nodes", None):
            return self._execute_graph_delivery(
                executor,
                prepared,
                yt_service,
                kb_sort_index,
                start_at_step=start_at_step,
            )

        failures = 0
        for step in (self._youtube_upload_step, self._kaderblick_post_step):
            if executor._cancel.is_set():
                break
            if self._skip_before_start(step.name, start_at_step):
                continue
            if not executor._prepared_output_reaches_type(prepared, step.name):
                continue
            if isinstance(step, YoutubeUploadStep):
                failures += step.execute(executor, prepared, yt_service)
            elif isinstance(step, KaderblickPostStep):
                failures += step.execute(executor, prepared, kb_sort_index)
            if failures:
                break
            advance_cursor = getattr(executor, "_advance_prepared_output_cursor", None)
            if callable(advance_cursor):
                advance_cursor(prepared, step.name)

        if prepared.mark_finished and not failures:
            executor._set_job_status(prepared.orig_idx, "Fertig")
        return failures

    def _execute_graph_processing(
        self,
        executor: Any,
        prepared: PreparedOutput,
        *,
        include_title_card: bool,
        include_repair: bool,
        include_youtube_version: bool,
        start_at_step: str,
        stop_before_merge: bool,
    ) -> int:
        start_node_id = self._start_node_id(executor, prepared)
        if not start_node_id:
            return 0
        prepared.delivery_branches = []
        return self._walk_processing_branch(
            executor,
            root_prepared=prepared,
            branch_prepared=prepared,
            node_id=start_node_id,
            include_title_card=include_title_card,
            include_repair=include_repair,
            include_youtube_version=include_youtube_version,
            start_at_step=start_at_step,
            stop_before_merge=stop_before_merge,
            visited=set(),
        )

    def _execute_graph_delivery(
        self,
        executor: Any,
        prepared: PreparedOutput,
        yt_service: Any,
        kb_sort_index: dict[tuple[str, str], int],
        *,
        start_at_step: str,
    ) -> int:
        branches = list(prepared.delivery_branches or [])
        failures = 0
        finished_branch_exists = False

        for branch in branches:
            if executor._cancel.is_set():
                break
            start_node_id = self._start_node_id(executor, branch)
            if not start_node_id:
                finished_branch_exists = finished_branch_exists or branch.mark_finished
                continue
            failures += self._walk_delivery_branch(
                executor,
                branch_prepared=branch,
                node_id=start_node_id,
                yt_service=yt_service,
                kb_sort_index=kb_sort_index,
                start_at_step=start_at_step,
                visited=set(),
            )
            if failures:
                break
            finished_branch_exists = finished_branch_exists or branch.mark_finished

        if not branches:
            finished_branch_exists = prepared.mark_finished

        if prepared.mark_finished and not failures and finished_branch_exists:
            executor._set_job_status(prepared.orig_idx, "Fertig")
        return failures

    def _walk_processing_branch(
        self,
        executor: Any,
        *,
        root_prepared: PreparedOutput,
        branch_prepared: PreparedOutput,
        node_id: str,
        include_title_card: bool,
        include_repair: bool,
        include_youtube_version: bool,
        start_at_step: str,
        stop_before_merge: bool,
        visited: set[str],
    ) -> int:
        if executor._cancel.is_set() or node_id in visited:
            return 0

        local_visited = set(visited)
        local_visited.add(node_id)
        node_type = ExecutorSupport.node_type(branch_prepared.job, node_id)
        branch_prepared.graph_cursor_node_id = node_id

        if stop_before_merge and node_type == "merge":
            return 0

        if node_type in self._delivery_steps:
            branch_prepared.graph_cursor_node_id = node_id
            root_prepared.delivery_branches.append(self._clone_prepared(branch_prepared))
            return 0

        step = self._processing_steps.get(node_type)
        if step is not None and self._processing_step_enabled(node_type, include_title_card, include_repair, include_youtube_version):
            if not self._skip_before_start(node_type, start_at_step):
                failures = step.execute(executor, branch_prepared)
                if failures:
                    return failures

        next_nodes = ExecutorSupport.direct_targets(
            branch_prepared.job,
            node_id,
            getattr(branch_prepared, "validation_results", {}) or {},
        )
        for next_node_id in next_nodes:
            next_prepared = self._clone_prepared(branch_prepared)
            failures = self._walk_processing_branch(
                executor,
                root_prepared=root_prepared,
                branch_prepared=next_prepared,
                node_id=next_node_id,
                include_title_card=include_title_card,
                include_repair=include_repair,
                include_youtube_version=include_youtube_version,
                start_at_step=start_at_step,
                stop_before_merge=stop_before_merge,
                visited=local_visited,
            )
            if failures:
                return failures
        return 0

    def _walk_delivery_branch(
        self,
        executor: Any,
        *,
        branch_prepared: PreparedOutput,
        node_id: str,
        yt_service: Any,
        kb_sort_index: dict[tuple[str, str], int],
        start_at_step: str,
        visited: set[str],
    ) -> int:
        if executor._cancel.is_set() or node_id in visited:
            return 0

        local_visited = set(visited)
        local_visited.add(node_id)
        node_type = ExecutorSupport.node_type(branch_prepared.job, node_id)
        branch_prepared.graph_cursor_node_id = node_id

        step = self._delivery_steps.get(node_type)
        if step is not None and not self._skip_before_start(node_type, start_at_step):
            if isinstance(step, YoutubeUploadStep):
                failures = step.execute(executor, branch_prepared, yt_service)
            elif isinstance(step, KaderblickPostStep):
                failures = step.execute(executor, branch_prepared, kb_sort_index)
            else:
                failures = 0
            if failures:
                return failures

        next_nodes = ExecutorSupport.direct_targets(
            branch_prepared.job,
            node_id,
            getattr(branch_prepared, "validation_results", {}) or {},
        )
        for next_node_id in next_nodes:
            next_prepared = self._clone_prepared(branch_prepared)
            failures = self._walk_delivery_branch(
                executor,
                branch_prepared=next_prepared,
                node_id=next_node_id,
                yt_service=yt_service,
                kb_sort_index=kb_sort_index,
                start_at_step=start_at_step,
                visited=local_visited,
            )
            if failures:
                return failures
        return 0

    @staticmethod
    def _clone_prepared(prepared: PreparedOutput) -> PreparedOutput:
        cv_job = replace(
            prepared.cv_job,
            youtube_tags=list(getattr(prepared.cv_job, "youtube_tags", [])),
        )
        for key, value in prepared.cv_job.__dict__.items():
            if key not in cv_job.__dict__:
                setattr(cv_job, key, deepcopy(value))
        return replace(
            prepared,
            cv_job=cv_job,
            validation_results=dict(getattr(prepared, "validation_results", {}) or {}),
            delivery_branches=[],
        )

    @staticmethod
    def _start_node_id(executor: Any, prepared: PreparedOutput) -> str:
        cursor_node_id = str(getattr(prepared, "graph_cursor_node_id", "") or "")
        if cursor_node_id:
            return cursor_node_id
        origin_node_id = str(getattr(prepared, "graph_origin_node_id", "") or "")
        if origin_node_id:
            return origin_node_id
        if getattr(prepared, "graph_origin_kind", "source") == "merge":
            return executor._graph_node_id_for_type(prepared.job, "merge")
        return ""

    @staticmethod
    def _processing_step_enabled(
        node_type: str,
        include_title_card: bool,
        include_repair: bool,
        include_youtube_version: bool,
    ) -> bool:
        if node_type == "titlecard":
            return include_title_card
        if node_type == "repair":
            return include_repair
        if node_type == "yt_version":
            return include_youtube_version
        return node_type in {"validate_surface", "validate_deep", "cleanup", "stop"}

    @classmethod
    def _skip_before_start(cls, step_name: str, start_at_step: str) -> bool:
        if not start_at_step or start_at_step not in cls._STEP_ORDER or step_name not in cls._STEP_ORDER:
            return False
        return cls._STEP_ORDER.index(step_name) < cls._STEP_ORDER.index(start_at_step)
