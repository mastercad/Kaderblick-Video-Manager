from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from ..media.merge import generate_title_card
from ..media.ffmpeg_runner import get_resolution
from ..media.step_reporting import format_media_artifact
from ..workflow.defaults import titlecard_match_data
from .executor_support import ExecutorSupport
from .models import PreparedOutput


class TitleCardStep:
    name = "titlecard"

    def execute(self, executor: Any, prepared: PreparedOutput) -> int:
        title_card_enabled = prepared.title_card_enabled_override
        if not (title_card_enabled and prepared.cv_job.output_path):
            return 0
        reused_path = self._find_existing_titlecard_output(prepared)
        if reused_path is not None and reused_path.exists() and not prepared.per_settings.video.overwrite:
            prepared.cv_job.output_path = reused_path
            executor._set_step_status(prepared.job, "titlecard", "reused-target")
            executor._set_step_detail(prepared.job, "titlecard", format_media_artifact(reused_path))
            executor._set_job_status(prepared.orig_idx, f"Titelkarte OK (vorhanden): {reused_path.name}")
            executor.job_progress.emit(prepared.orig_idx, 100, "titlecard")
            return 0
        if not prepared.cv_job.output_path.exists():
            return 0
        executor._set_step_status(prepared.job, "titlecard", "running")
        executor._set_job_status(prepared.orig_idx, "Titelkarte erstellen …")
        executor.job_progress.emit(prepared.orig_idx, 0, "titlecard")
        prepared.cv_job.output_path, success = self._prepend_title_card(
            executor,
            prepared.orig_idx,
            prepared.cv_job,
            prepared.job,
            prepared.per_settings,
        )
        if ExecutorSupport.is_job_cancelled(executor, prepared.orig_idx):
            executor._set_step_status(prepared.job, "titlecard", "cancelled")
            executor._set_step_detail(prepared.job, "titlecard", f"Durch Benutzer abgebrochen: {prepared.cv_job.source_path.name}")
            executor._set_job_status(prepared.orig_idx, "Titelkarte abgebrochen")
            return 0
        if not success:
            executor._set_step_status(prepared.job, "titlecard", "error")
            executor._set_step_detail(prepared.job, "titlecard", f"Titelkarte fehlgeschlagen für {prepared.cv_job.source_path.name}")
            executor._set_job_status(prepared.orig_idx, "Titelkarte fehlgeschlagen")
            return 0
        executor._set_step_status(prepared.job, "titlecard", "done")
        executor._set_step_detail(prepared.job, "titlecard", format_media_artifact(prepared.cv_job.output_path))
        executor.job_progress.emit(prepared.orig_idx, 100, "titlecard")
        return 0

    def _prepend_title_card(self, executor: Any, orig_idx: int, cv_job, job, per_settings) -> tuple[Path, bool]:
        cancel_flag = ExecutorSupport.cancel_flag_for_job(executor, orig_idx)
        video_path = cv_job.output_path
        if not video_path:
            raise ValueError("cv_job.output_path ist None")
        preserve_original = video_path == cv_job.source_path

        entry = executor._find_file_entry(job, str(cv_job.source_path))

        match = titlecard_match_data(per_settings, job)
        home_team = (match.home_team or "").strip()
        away_team = (match.away_team or "").strip()

        title = ""
        if home_team and away_team:
            title = f"{home_team} vs {away_team}"
        elif home_team or away_team:
            title = home_team or away_team

        subtitle = (
            (entry.title_card_subtitle if entry and entry.title_card_subtitle else "")
            or video_path.stem
        )

        res = get_resolution(video_path)
        width, height = res if res else (1920, 1080)
        fps = per_settings.video.fps or 25

        tmpdir = Path(tempfile.mkdtemp(prefix="intro_"))
        card_path = tmpdir / "intro.mp4"

        executor.log_message.emit(f'  Erstelle Titelkarte: "{subtitle}"')
        ok = generate_title_card(
            card_path,
            subtitle=subtitle,
            duration=job.title_card_duration,
            width=width,
            height=height,
            fps=fps,
            title=title,
            logo_path=job.title_card_logo_path,
            bg_color=job.title_card_bg_color,
            fg_color=job.title_card_fg_color,
            encoder=per_settings.video.encoder,
            cancel_flag=cancel_flag,
            log_callback=executor.log_message.emit,
            progress_callback=lambda pct: executor.job_progress.emit(orig_idx, min(50, int(pct * 0.5)), "titlecard"),
            work_dir=tmpdir,
        )
        if not ok or cancel_flag.is_set():
            executor.log_message.emit("  ⚠ Titelkarte konnte nicht erstellt werden")
            self._cleanup_tmpdir(tmpdir)
            return video_path, False

        executor._set_job_status(orig_idx, "Titelkarte zusammenführen …")
        executor.job_progress.emit(orig_idx, 50, "titlecard")
        if preserve_original:
            with_intro_path = ExecutorSupport.derived_output_path(
                cv_job,
                video_path,
                suffix="_titlecard",
                extension=video_path.suffix,
            )
        else:
            with_intro_path = video_path.with_stem(video_path.stem + "_tmp_intro")
        with_intro_path.parent.mkdir(parents=True, exist_ok=True)
        concat_ok = executor._concat_func(
            [card_path, video_path],
            with_intro_path,
            cancel_flag=cancel_flag,
            log_callback=executor.log_message.emit,
            encoder=per_settings.video.encoder,
            progress_callback=lambda pct: executor.job_progress.emit(orig_idx, 50 + int(pct * 0.5), "titlecard"),
            overwrite=per_settings.video.overwrite,
        )

        self._cleanup_tmpdir(tmpdir)

        if not concat_ok or cancel_flag.is_set():
            executor.log_message.emit("  ⚠ Zusammenführen mit Titelkarte fehlgeschlagen")
            if with_intro_path.exists():
                with_intro_path.unlink(missing_ok=True)
            return video_path, False

        if preserve_original:
            return with_intro_path, True

        try:
            video_path.unlink()
        except OSError:
            pass
        with_intro_path.rename(video_path)
        return video_path, True

    @staticmethod
    def _find_existing_titlecard_output(prepared: PreparedOutput) -> Path | None:
        output_path = prepared.cv_job.output_path
        if output_path is None:
            return None

        titlecard_path = ExecutorSupport.derived_output_path(
            prepared.cv_job,
            output_path,
            suffix="_titlecard",
            extension=output_path.suffix,
        )
        if titlecard_path.exists():
            return titlecard_path

        if prepared.job.step_statuses.get("titlecard") in {"done", "reused-target"} and output_path.exists():
            return output_path
        return None

    @staticmethod
    def _cleanup_tmpdir(tmpdir: Path) -> None:
        try:
            import shutil as _sh
            _sh.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass
