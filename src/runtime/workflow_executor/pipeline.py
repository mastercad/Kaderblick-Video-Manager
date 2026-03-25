from collections import defaultdict
from pathlib import Path
from queue import Queue
import threading
import time
from typing import Any

from ...integrations.youtube import _youtube_variant_candidates
from ...media.converter import ConvertJob
from ...workflow import WorkflowJob
from ...workflow_steps import ConvertItem, PreparedOutput
from .helpers import _PipelineWorkerView


class WorkflowExecutorPipelineMixin:
    def _run_pipelined(self, active: list[tuple[int, WorkflowJob]]) -> tuple[int, int, int]:
        stats = {"ok": 0, "skip": 0, "fail": 0}
        stats_lock = threading.Lock()
        progress_lock = threading.Lock()
        merge_lock = threading.Lock()
        job_completion_lock = threading.Lock()
        process_queue: Queue[tuple[str, Any] | None] = Queue()
        upload_queue: Queue[PreparedOutput | None] = Queue()
        event_queue: Queue[tuple[str, tuple[Any, ...]]] = Queue()
        merge_groups: dict[tuple[int, str], list[ConvertItem]] = defaultdict(list)
        job_expected_outputs: dict[int, int] = defaultdict(int)
        job_completed_outputs: dict[int, int] = defaultdict(int)
        merge_output_keys: set[tuple[int, str]] = set()
        total_steps = max(1, self._estimate_pipeline_total_steps(active))
        progress_done = 0
        worker_executor = _PipelineWorkerView(self, event_queue)
        self._pipeline_event_queue = event_queue
        self._pipeline_last_drain = 0.0

        def _bump_stat(key: str, amount: int = 1) -> None:
            with stats_lock:
                stats[key] += amount

        def _advance_progress() -> None:
            nonlocal progress_done
            with progress_lock:
                progress_done += 1
                self.overall_progress.emit(min(progress_done, total_steps), total_steps)

        def _register_expected_output(orig_idx: int, merge_group_id: str = "") -> None:
            with job_completion_lock:
                if merge_group_id:
                    merge_key = (orig_idx, merge_group_id)
                    if merge_key in merge_output_keys:
                        return
                    merge_output_keys.add(merge_key)
                job_expected_outputs[orig_idx] += 1

        def _mark_output_completed(orig_idx: int) -> None:
            with job_completion_lock:
                total = int(job_expected_outputs.get(orig_idx, 0) or 0)
                if total <= 0:
                    return
                job_completed_outputs[orig_idx] += 1
                done = job_completed_outputs[orig_idx]
            if done >= total:
                event_queue.put(("job_progress", (orig_idx, 100)))
                event_queue.put(("job_status", (orig_idx, "Fertig")))
            else:
                event_queue.put(("job_status", (orig_idx, f"Fertig {done}/{total}")))

        self._pipeline_register_expected_output = _register_expected_output
        self._pipeline_mark_output_completed = _mark_output_completed

        needs_delivery_worker = any(
            self._support.job_reaches_type(job, "youtube_upload") or self._support.job_reaches_type(job, "kaderblick")
            for _, job in active
        )
        yt_service = None
        if any(self._support.job_reaches_type(job, "youtube_upload") for _, job in active):
            self.log_message.emit("YouTube-Anmeldung …")
            yt_service = self._get_youtube_service()
            if not yt_service:
                self.log_message.emit("⚠ YouTube-Upload deaktiviert (Anmeldung fehlgeschlagen)")

        kb_sort_index = self._build_pipeline_kaderblick_sort_index(active)

        def _process_worker() -> None:
            while True:
                task = process_queue.get()
                try:
                    if task is None:
                        return

                    if self._cancel.is_set():
                        continue

                    kind, payload = task
                    if kind == "item":
                        ok_inc, skip_inc, fail_inc = self._process_pipeline_item(
                            worker_executor,
                            payload,
                            upload_queue if needs_delivery_worker else None,
                            yt_service,
                            kb_sort_index,
                        )
                    elif kind == "merge":
                        gid, group = payload
                        ok_inc, skip_inc, fail_inc = self._process_pipeline_merge_group(
                            worker_executor,
                            gid,
                            group,
                            upload_queue if needs_delivery_worker else None,
                            yt_service,
                            kb_sort_index,
                        )
                    else:
                        raise ValueError(f"Unbekannter Pipeline-Task: {kind!r}")

                    if ok_inc:
                        _bump_stat("ok", ok_inc)
                    if skip_inc:
                        _bump_stat("skip", skip_inc)
                    if fail_inc:
                        _bump_stat("fail", fail_inc)
                    _advance_progress()
                except Exception as exc:
                    _bump_stat("fail", 1)
                    self.log_message.emit(f"❌ Pipeline-Verarbeitung fehlgeschlagen: {exc}")
                    _advance_progress()
                finally:
                    process_queue.task_done()

        def _upload_worker() -> None:
            while True:
                prepared = upload_queue.get()
                try:
                    if prepared is None:
                        return
                    if self._cancel.is_set():
                        continue
                    failures = self._output_step_stack.execute_delivery_steps(
                        worker_executor,
                        prepared,
                        yt_service,
                        kb_sort_index,
                        start_at_step=prepared.resume_from_step,
                    )
                    if failures:
                        _bump_stat("fail", failures)
                    else:
                        _mark_output_completed(prepared.orig_idx)
                    _advance_progress()
                except Exception as exc:
                    _bump_stat("fail", 1)
                    self.log_message.emit(f"❌ Upload fehlgeschlagen: {exc}")
                    _advance_progress()
                finally:
                    upload_queue.task_done()

        process_thread = threading.Thread(target=_process_worker, name="workflow-process", daemon=True)
        process_thread.start()
        upload_thread = None
        if needs_delivery_worker:
            upload_thread = threading.Thread(target=_upload_worker, name="workflow-upload", daemon=True)
            upload_thread.start()

        workflow_label = "1 aktiver Workflow" if len(active) == 1 else f"{len(active)} aktive Workflows"
        self.log_message.emit(
            f"\n{'═' * 60}"
            f"\n  🚦 Pipeline-Workflow  ({workflow_label})"
            f"\n{'═' * 60}"
        )

        for active_pos, (orig_idx, job) in enumerate(active):
            if self._cancel.is_set():
                break

            resume_step = self._first_pending_step(job)
            if resume_step is None:
                self._set_job_status(orig_idx, "Fertig")
                _bump_stat("skip", 1)
                _advance_progress()
                continue

            dispatched_paths: set[str] = set()

            def _on_file_ready(file_path: str) -> None:
                dispatched_paths.add(file_path)
                self._dispatch_pipeline_item(orig_idx, job, file_path, process_queue, merge_groups, merge_lock)

            skip_transfer = self._should_skip_step_for_resume(job, "transfer", resume_step)
            if skip_transfer:
                files = self._transfer_step.resume_inputs(self, orig_idx, job, on_file_ready=_on_file_ready)
            else:
                self._set_step_status(job, "transfer", "running")
                self._set_job_status(orig_idx, "Transfer …")
                try:
                    files = self._transfer_step.execute(self, orig_idx, job, on_file_ready=_on_file_ready)
                except Exception as exc:
                    self._set_step_status(job, "transfer", f"error: {exc}")
                    self._set_job_status(orig_idx, f"Fehler: {exc}")
                    job.error_msg = str(exc)
                    self.log_message.emit(f"❌ {job.name}: {exc}")
                    _bump_stat("fail", 1)
                    _advance_progress()
                    continue

            if self._cancel.is_set():
                break

            for file_path in files:
                if file_path in dispatched_paths:
                    continue
                self._dispatch_pipeline_item(orig_idx, job, file_path, process_queue, merge_groups, merge_lock)

            self._enqueue_ready_merge_groups(process_queue, merge_groups, merge_lock, orig_idx=orig_idx)

            if int(job_expected_outputs.get(orig_idx, 0) or 0) == 0:
                self.job_progress.emit(orig_idx, 100)
                self._set_job_status(orig_idx, "Fertig")

            if not skip_transfer:
                self._set_step_status(job, "transfer", "done")
                if int(job_expected_outputs.get(orig_idx, 0) or 0) > 0:
                    self._set_job_status(orig_idx, "Transfer OK")
            self._transfer_fail = stats["fail"]
            self.overall_progress.emit(active_pos + 1, max(len(active), total_steps))
            _advance_progress()
            self._drain_pipeline_events(event_queue)

        self._wait_for_queue(process_queue, event_queue)

        if not self._cancel.is_set():
            self._enqueue_ready_merge_groups(process_queue, merge_groups, merge_lock)
            self._wait_for_queue(process_queue, event_queue)

        process_queue.put(None)
        process_thread.join()
        self._drain_pipeline_events(event_queue)

        if upload_thread is not None:
            self._wait_for_queue(upload_queue, event_queue)
            upload_queue.put(None)
            upload_thread.join()
            self._drain_pipeline_events(event_queue)

        self._transfer_fail = stats["fail"]
        self._pipeline_event_queue = None
        self._pipeline_last_drain = 0.0
        self._pipeline_register_expected_output = None
        self._pipeline_mark_output_completed = None
        return stats["ok"], stats["skip"], stats["fail"]

    def _pump_pipeline_events(self, *, force: bool = False) -> None:
        event_queue = getattr(self, "_pipeline_event_queue", None)
        if event_queue is None:
            return
        now = time.monotonic()
        last_drain = float(getattr(self, "_pipeline_last_drain", 0.0) or 0.0)
        if not force and (now - last_drain) < 0.05:
            return
        self._pipeline_last_drain = now
        self._drain_pipeline_events(event_queue)

    @staticmethod
    def _merge_group_key(orig_idx: int, merge_group_id: str) -> tuple[int, str]:
        return orig_idx, merge_group_id

    def _enqueue_ready_merge_groups(
        self,
        process_queue: Queue[tuple[str, Any] | None],
        merge_groups: dict[tuple[int, str], list[ConvertItem]],
        merge_lock: threading.Lock,
        *,
        orig_idx: int | None = None,
    ) -> None:
        with merge_lock:
            ready_groups = [
                (key, group)
                for key, group in merge_groups.items()
                if orig_idx is None or key[0] == orig_idx
            ]
            for key, _group in ready_groups:
                merge_groups.pop(key, None)

        for (_group_orig_idx, merge_group_id), group in ready_groups:
            process_queue.put(("merge", (merge_group_id, group)))

    def _dispatch_pipeline_item(
        self,
        orig_idx: int,
        job: WorkflowJob,
        file_path: str,
        process_queue: Queue[tuple[str, Any] | None],
        merge_groups: dict[tuple[int, str], list[ConvertItem]],
        merge_lock: threading.Lock,
    ) -> None:
        if self._cancel.is_set():
            return

        resume_step = self._first_pending_step(job)
        merge_group_id = self._get_merge_group_id(job, file_path)
        merge_before_convert = self._merge_precedes_convert(job)
        convert_enabled = self._support.source_reaches_type(job, file_path, "convert")
        youtube_upload_enabled = self._support.source_reaches_type(job, file_path, "youtube_upload")
        youtube_playlist = self._resolve_youtube_playlist(job, file_path)
        youtube_description = self._resolve_youtube_description(job, file_path)
        youtube_tags = self._resolve_youtube_tags(job, file_path)

        if convert_enabled and not (merge_group_id and merge_before_convert):
            cv_job = self._build_convert_job(job, file_path)
        else:
            cv_job = ConvertJob(
                source_path=Path(file_path),
                job_type="convert",
                youtube_title=self._resolve_youtube_title(job, file_path),
                youtube_description=youtube_description,
                youtube_playlist=youtube_playlist,
                youtube_tags=youtube_tags,
            )
            self._support.assign_derived_output_dir(
                cv_job,
                self._support.resolve_processed_destination(file_path),
            )
            cv_job.status = "Fertig"
            cv_job.output_path = Path(file_path)

        item = ConvertItem(orig_idx=orig_idx, job=job, cv_job=cv_job, resume_from_step=resume_step or "")
        register_expected_output = getattr(self, "_pipeline_register_expected_output", None)
        if merge_group_id:
            merge_key = self._merge_group_key(orig_idx, merge_group_id)
            with merge_lock:
                merge_groups[merge_key].append(item)
            if register_expected_output is not None:
                register_expected_output(orig_idx, merge_group_id)
            if convert_enabled and not merge_before_convert and not self._should_skip_step_for_resume(job, "convert", resume_step):
                process_queue.put(("item", item))
            return

        if register_expected_output is not None:
            register_expected_output(orig_idx)
        process_queue.put(("item", item))

    def _process_pipeline_item(
        self,
        executor_view: Any,
        item: ConvertItem,
        upload_queue: Queue[PreparedOutput | None] | None,
        yt_service,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> tuple[int, int, int]:
        if self._cancel.is_set():
            return 0, 0, 0
        mark_output_completed = getattr(self, "_pipeline_mark_output_completed", None)
        merge_group_id = self._get_merge_group_id(item.job, str(item.cv_job.source_path))
        resume_step = item.resume_from_step or self._first_pending_step(item.job) or ""
        per_settings = self._build_job_settings(item.job)
        source_path = str(item.cv_job.source_path)
        include_title_card = self._support.source_reaches_type(item.job, source_path, "titlecard")
        include_repair = self._support.source_reaches_type(item.job, source_path, "repair")
        include_youtube_version = self._support.source_reaches_type(item.job, source_path, "yt_version")
        youtube_upload_enabled = self._support.source_reaches_type(item.job, source_path, "youtube_upload")
        kaderblick_enabled = youtube_upload_enabled and self._support.source_reaches_type(item.job, source_path, "kaderblick")
        per_settings.youtube.create_youtube = include_youtube_version
        per_settings.youtube.upload_to_youtube = youtube_upload_enabled

        if self._support.source_reaches_type(item.job, source_path, "convert"):
            can_skip_convert = self._should_skip_step_for_resume(item.job, "convert", resume_step) and (
                (item.cv_job.output_path is not None and item.cv_job.output_path.exists())
                or (resume_step in {"youtube_upload", "kaderblick"} and self._has_existing_youtube_artifact(item.cv_job))
            )
            if can_skip_convert:
                item.cv_job.status = "Fertig"
                result = "ready"
            else:
                result = self._convert_step.execute(executor_view, item.orig_idx, item.job, item.cv_job, per_settings, 0, 1)
        else:
            result = "ready"

        if self._cancel.is_set():
            return 0, 0, 0
        if result == "skipped":
            return 0, 1, 0
        if result not in {"ok", "ready"}:
            self._set_job_status(item.orig_idx, f"Fehler: {item.cv_job.error_msg[:60]}")
            self.job_progress.emit(item.orig_idx, 0)
            return 0, 0, 1

        if merge_group_id:
            entry = self._find_file_entry(item.job, str(item.cv_job.source_path))
            if entry is not None and entry.title_card_before_merge:
                prepared = PreparedOutput(
                    orig_idx=item.orig_idx,
                    job=item.job,
                    cv_job=item.cv_job,
                    per_settings=per_settings,
                    resume_from_step=resume_step,
                    graph_origin_node_id=self._support.source_node_id_for_file(item.job, str(item.cv_job.source_path)),
                    mark_finished=False,
                    title_card_enabled_override=True,
                    repair_enabled_override=False,
                    youtube_version_enabled_override=False,
                    youtube_upload_enabled_override=False,
                    kaderblick_enabled_override=False,
                )
                failures = self._output_step_stack.execute_processing_steps(
                    executor_view,
                    prepared,
                    include_title_card=True,
                    include_repair=False,
                    include_youtube_version=False,
                    start_at_step=resume_step,
                )
                if failures:
                    return 0, 0, failures
            return 1, 0, 0

        prepared = PreparedOutput(
            orig_idx=item.orig_idx,
            job=item.job,
            cv_job=item.cv_job,
            per_settings=per_settings,
            resume_from_step=resume_step,
            graph_origin_node_id=self._support.source_node_id_for_file(item.job, str(item.cv_job.source_path)),
            mark_finished=False,
            title_card_enabled_override=include_title_card,
            repair_enabled_override=include_repair,
            youtube_version_enabled_override=include_youtube_version,
            youtube_upload_enabled_override=youtube_upload_enabled,
            kaderblick_enabled_override=kaderblick_enabled,
        )
        failures = self._output_step_stack.execute_processing_steps(
            executor_view,
            prepared,
            include_title_card=include_title_card,
            include_repair=include_repair,
            include_youtube_version=include_youtube_version,
            start_at_step=resume_step,
        )
        if failures:
            return 0, 0, failures

        if self._cancel.is_set():
            return 0, 0, 0

        if youtube_upload_enabled or kaderblick_enabled:
            if upload_queue is not None:
                if not self._cancel.is_set():
                    upload_queue.put(prepared)
            else:
                failures = self._output_step_stack.execute_delivery_steps(
                    executor_view,
                    prepared,
                    yt_service,
                    kb_sort_index,
                    start_at_step=resume_step,
                )
                if failures:
                    return 0, 0, failures
                if mark_output_completed is not None:
                    mark_output_completed(item.orig_idx)
        else:
            failures = self._output_step_stack.execute_delivery_steps(
                executor_view,
                prepared,
                yt_service,
                kb_sort_index,
                start_at_step=resume_step,
            )
            if failures:
                return 0, 0, failures
            if mark_output_completed is not None:
                mark_output_completed(item.orig_idx)
        return 1, 0, 0

    def _process_pipeline_merge_group(
        self,
        executor_view: Any,
        merge_group_id: str,
        group: list[ConvertItem],
        upload_queue: Queue[PreparedOutput | None] | None,
        yt_service,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> tuple[int, int, int]:
        if self._cancel.is_set():
            return 0, 0, 0
        mark_output_completed = getattr(self, "_pipeline_mark_output_completed", None)
        resume_step = group[0].resume_from_step or self._first_pending_step(group[0].job) or ""
        if self._should_skip_step_for_resume(group[0].job, "merge", resume_step):
            prepared = self._resume_existing_merge_group(group, resume_step)
            merge_fail = 0
        else:
            prepared, merge_fail = self._merge_step.execute(executor_view, merge_group_id, group)
        if merge_fail:
            return 0, 0, merge_fail
        if prepared is None:
            return 0, 0, 0
        prepared.resume_from_step = resume_step

        if self._support.merge_reaches_type(prepared.job, "convert") and self._merge_precedes_convert(prepared.job):
            merged_source = prepared.cv_job.output_path or prepared.cv_job.source_path
            merged_cv = ConvertJob(
                source_path=merged_source,
                job_type="convert",
                youtube_title=prepared.cv_job.youtube_title,
                youtube_description=prepared.cv_job.youtube_description,
                youtube_playlist=prepared.cv_job.youtube_playlist,
                youtube_tags=list(prepared.cv_job.youtube_tags),
            )
            existing_output = self._convert_step._find_existing_output(merged_cv, prepared.job, prepared.per_settings)
            if self._should_skip_step_for_resume(prepared.job, "convert", resume_step) and (
                (existing_output is not None and existing_output.exists())
                or (resume_step in {"youtube_upload", "kaderblick"} and self._has_existing_youtube_artifact(merged_cv))
            ):
                merged_cv.output_path = existing_output or Path(merged_source)
                merged_cv.status = "Fertig"
            else:
                result = self._convert_step.execute(executor_view, prepared.orig_idx, prepared.job, merged_cv, prepared.per_settings, 0, 1)
                if result not in {"ok", "ready"}:
                    return 0, 0, 1
            prepared.cv_job = merged_cv

        include_title_card = self._support.merge_reaches_type(prepared.job, "titlecard")
        include_repair = self._support.merge_reaches_type(prepared.job, "repair")
        include_youtube_version = self._support.merge_reaches_type(prepared.job, "yt_version")
        youtube_upload_enabled = self._support.merge_reaches_type(prepared.job, "youtube_upload")
        kaderblick_enabled = youtube_upload_enabled and self._support.merge_reaches_type(prepared.job, "kaderblick")
        prepared.per_settings.youtube.create_youtube = include_youtube_version
        prepared.per_settings.youtube.upload_to_youtube = youtube_upload_enabled
        prepared.mark_finished = False
        prepared.title_card_enabled_override = include_title_card
        prepared.repair_enabled_override = include_repair
        prepared.youtube_version_enabled_override = include_youtube_version
        prepared.youtube_upload_enabled_override = youtube_upload_enabled
        prepared.kaderblick_enabled_override = kaderblick_enabled
        failures = self._output_step_stack.execute_processing_steps(
            executor_view,
            prepared,
            include_title_card=include_title_card,
            include_repair=include_repair,
            include_youtube_version=include_youtube_version,
            start_at_step=resume_step,
        )
        if failures:
            return 0, 0, failures

        if self._cancel.is_set():
            return 0, 0, 0

        if youtube_upload_enabled or kaderblick_enabled:
            if upload_queue is not None:
                if not self._cancel.is_set():
                    upload_queue.put(prepared)
            else:
                failures = self._output_step_stack.execute_delivery_steps(
                    executor_view,
                    prepared,
                    yt_service,
                    kb_sort_index,
                    start_at_step=resume_step,
                )
                if failures:
                    return 0, 0, failures
                if mark_output_completed is not None:
                    mark_output_completed(prepared.orig_idx)
        else:
            failures = self._output_step_stack.execute_delivery_steps(
                executor_view,
                prepared,
                yt_service,
                kb_sort_index,
                start_at_step=resume_step,
            )
            if failures:
                return 0, 0, failures
            if mark_output_completed is not None:
                mark_output_completed(prepared.orig_idx)
        return 1, 0, 0

    def _resume_existing_merge_group(
        self,
        group: list[ConvertItem],
        resume_step: str,
    ) -> PreparedOutput | None:
        if not group:
            return None
        first_item = group[0]
        first_job = first_item.job
        first_cv = first_item.cv_job
        per_settings = self._build_job_settings(first_job)
        self._merge_step._apply_merge_output_metadata(first_job, first_cv)
        merged_path = self._merge_step._expected_merged_path(first_job, first_cv)
        if not merged_path.exists():
            return None
        first_cv.output_path = merged_path
        return PreparedOutput(
            first_item.orig_idx,
            first_job,
            first_cv,
            per_settings,
            resume_from_step=resume_step,
            graph_origin_kind="merge",
        )

    @staticmethod
    def _has_existing_youtube_artifact(cv_job: ConvertJob) -> bool:
        output_path = cv_job.output_path
        if output_path is None:
            return False
        derived_dir = str(getattr(cv_job, "derived_output_dir", "") or "")
        return any(candidate.exists() for candidate in _youtube_variant_candidates(output_path, derived_dir))

    def _drain_pipeline_events(self, event_queue: Queue[tuple[str, tuple[Any, ...]]]) -> None:
        while not event_queue.empty():
            event_name, args = event_queue.get()
            try:
                if event_name == "job_status":
                    self._set_job_status(*args)
                elif event_name == "job_progress":
                    self.job_progress.emit(*args)
                elif event_name == "phase_changed":
                    self.phase_changed.emit(*args)
                elif event_name == "log_message":
                    self.log_message.emit(*args)
                elif event_name == "file_progress":
                    self.file_progress.emit(*args)
                elif event_name == "convert_progress":
                    self.convert_progress.emit(*args)
                elif event_name == "source_status":
                    self.source_status.emit(*args)
                elif event_name == "source_progress":
                    self.source_progress.emit(*args)
            finally:
                event_queue.task_done()

    def _wait_for_queue(self, work_queue: Queue[Any], event_queue: Queue[tuple[str, tuple[Any, ...]]]) -> None:
        while work_queue.unfinished_tasks:
            self._drain_pipeline_events(event_queue)
            time.sleep(0.01)
        self._drain_pipeline_events(event_queue)