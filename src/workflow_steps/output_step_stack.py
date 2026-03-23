from __future__ import annotations

from typing import Any

from .cleanup_output_step import CleanupOutputStep
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
    ) -> int:
        failures = self.execute_processing_steps(
            executor,
            prepared,
            include_title_card=include_title_card,
            include_repair=include_repair,
            include_youtube_version=include_youtube_version,
        )
        if failures or executor._cancel.is_set():
            return failures
        return self.execute_delivery_steps(executor, prepared, yt_service, kb_sort_index)

    def execute_processing_steps(
        self,
        executor: Any,
        prepared: PreparedOutput,
        *,
        include_title_card: bool = True,
        include_repair: bool = True,
        include_youtube_version: bool = True,
    ) -> int:
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
            if not executor._prepared_output_reaches_type(prepared, step.name):
                continue
            failures += step.execute(executor, prepared)
            if failures:
                break

        return failures

    def execute_delivery_steps(
        self,
        executor: Any,
        prepared: PreparedOutput,
        yt_service: Any,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> int:
        failures = 0
        for step in (self._youtube_upload_step, self._kaderblick_post_step):
            if executor._cancel.is_set():
                break
            if not executor._prepared_output_reaches_type(prepared, step.name):
                continue
            if isinstance(step, YoutubeUploadStep):
                failures += step.execute(executor, prepared, yt_service)
            elif isinstance(step, KaderblickPostStep):
                failures += step.execute(executor, prepared, kb_sort_index)
            if failures:
                break

        if prepared.mark_finished and not failures:
            executor._set_job_status(prepared.orig_idx, "Fertig")
        return failures
