from __future__ import annotations

from typing import Any

from .kaderblick_post_step import KaderblickPostStep
from .models import PreparedOutput
from .title_card_step import TitleCardStep
from .youtube_upload_step import YoutubeUploadStep
from .youtube_version_step import YoutubeVersionStep


class OutputStepStack:
    name = "output-stack"

    def __init__(self):
        self._title_card_step = TitleCardStep()
        self._youtube_version_step = YoutubeVersionStep()
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
        include_youtube_version: bool = True,
    ) -> int:
        failures = 0
        steps = []
        if include_title_card:
            steps.append(self._title_card_step)
        if include_youtube_version:
            steps.append(self._youtube_version_step)
        steps.append(self._youtube_upload_step)
        steps.append(self._kaderblick_post_step)

        for step in steps:
            if executor._cancel.is_set():
                break
            if isinstance(step, YoutubeUploadStep):
                failures += step.execute(executor, prepared, yt_service)
            elif isinstance(step, KaderblickPostStep):
                failures += step.execute(executor, prepared, kb_sort_index)
            else:
                failures += step.execute(executor, prepared)
            if failures:
                break

        if prepared.mark_finished and not failures:
            executor._set_job_status(prepared.orig_idx, "Fertig")
        return failures
