from __future__ import annotations

from collections import defaultdict
from typing import Any

from .models import ConvertItem, PreparedOutput, ProcessingResult


class ProcessingPhase:
    name = "processing-phase"

    def execute(self, executor: Any, convert_items: list[ConvertItem]) -> ProcessingResult:
        merge_items = [
            item for item in convert_items
            if executor._get_merge_group_id(item.job, str(item.cv_job.source_path))
        ]
        to_convert = [item for item in convert_items if item.job.convert_enabled]
        upload_only = [
            item for item in convert_items
            if not item.job.convert_enabled
            and item.job.upload_youtube
            and not executor._get_merge_group_id(item.job, str(item.cv_job.source_path))
        ]

        if not to_convert and not upload_only and not merge_items:
            executor.log_message.emit("\nKeine weiteren Verarbeitungsschritte.")
            return ProcessingResult(ok=0, skip=0, fail=0)

        if to_convert:
            executor.phase_changed.emit("Phase 2 – Konvertierung …")
            executor.log_message.emit(
                f"\n{'═' * 60}"
                f"\n  🔄 Phase 2: Konvertierung  ({len(to_convert)} Datei(en))"
                f"\n{'═' * 60}"
            )

        yt_service = self._get_youtube_service(executor, convert_items)
        kb_sort_index = self._build_kaderblick_sort_index(executor, convert_items)

        ok, skip, fail = self._run_conversion_items(executor, to_convert, yt_service, kb_sort_index)
        fail += self._run_merge_groups(executor, merge_items, yt_service, kb_sort_index)
        fail += self._run_upload_only(executor, upload_only, yt_service, kb_sort_index)

        return ProcessingResult(ok=ok, skip=skip, fail=fail)

    @staticmethod
    def _get_youtube_service(executor: Any, convert_items: list[ConvertItem]) -> Any:
        needs_youtube = any(item.job.upload_youtube for item in convert_items)
        yt_service = None
        if needs_youtube:
            executor.log_message.emit("YouTube-Anmeldung …")
            yt_service = executor._get_youtube_service()
            if not yt_service:
                executor.log_message.emit(
                    "⚠ YouTube-Upload deaktiviert (Anmeldung fehlgeschlagen)"
                )
        return yt_service

    @staticmethod
    def _build_kaderblick_sort_index(
        executor: Any,
        convert_items: list[ConvertItem],
    ) -> dict[tuple[str, str], int]:
        kb_by_game: dict[str, list[str]] = defaultdict(list)
        for item in convert_items:
            if not (item.job.upload_kaderblick and item.job.upload_youtube):
                continue
            entry = executor._find_file_entry(item.job, str(item.cv_job.source_path))
            explicit_game_id = entry.kaderblick_game_id if entry and entry.kaderblick_game_id else ""
            game_id = ExecutorSupport.resolve_kaderblick_game_id(executor._settings, item.job, explicit_game_id)
            if game_id:
                kb_by_game[game_id].append(item.cv_job.source_path.name)

        kb_sort_index: dict[tuple[str, str], int] = {}
        for game_id, names in kb_by_game.items():
            for pos, name in enumerate(sorted(set(names)), start=1):
                kb_sort_index[(game_id, name)] = pos
        return kb_sort_index

    def _run_conversion_items(
        self,
        executor: Any,
        to_convert: list[ConvertItem],
        yt_service: Any,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> tuple[int, int, int]:
        totals = self._build_job_totals(to_convert)
        ok = 0
        skip = 0
        fail = 0

        for conv_pos, item in enumerate(to_convert):
            if executor._cancel.is_set():
                executor.log_message.emit("Konvertierung abgebrochen.")
                break

            executor.log_message.emit(
                f"\n═══ [{conv_pos + 1}/{len(to_convert)}] {item.job.name}: "
                f"{item.cv_job.source_path.name} ═══"
            )

            per_settings = executor._build_job_settings(item.job)
            done_count, total_count = totals[item.orig_idx]
            result = executor._convert_step.execute(
                executor,
                item.orig_idx,
                item.job,
                item.cv_job,
                per_settings,
                done_count,
                total_count,
            )

            if executor._cancel.is_set():
                break

            if result in {"ok", "ready"}:
                ok += 1
                totals[item.orig_idx][0] += 1
                remaining = totals[item.orig_idx][1] - totals[item.orig_idx][0]
                self._update_job_progress(executor, item.orig_idx, totals[item.orig_idx])
                merge_group_id = executor._get_merge_group_id(item.job, str(item.cv_job.source_path))
                if merge_group_id:
                    entry = executor._find_file_entry(item.job, str(item.cv_job.source_path))
                    if entry is not None and entry.title_card_before_merge:
                        pre_merge_fail = executor._output_step_stack.execute_processing_steps(
                            executor,
                            PreparedOutput(
                                orig_idx=item.orig_idx,
                                job=item.job,
                                cv_job=item.cv_job,
                                per_settings=per_settings,
                                mark_finished=False,
                                title_card_enabled_override=True,
                                graph_origin_node_id=executor._support.source_node_id_for_file(item.job, str(item.cv_job.source_path)),
                            ),
                            include_title_card=True,
                            include_youtube_version=False,
                        )
                        if pre_merge_fail:
                            fail += pre_merge_fail
                    continue
                if not merge_group_id:
                    fail += executor._run_output_steps(
                        PreparedOutput(
                            orig_idx=item.orig_idx,
                            job=item.job,
                            cv_job=item.cv_job,
                            per_settings=per_settings,
                            graph_origin_node_id=executor._support.source_node_id_for_file(item.job, str(item.cv_job.source_path)),
                            status_prefix=(
                                f"YouTube-Upload {totals[item.orig_idx][0]}/{totals[item.orig_idx][1]} …"
                                if totals[item.orig_idx][1] > 1 else ""
                            ),
                            mark_finished=(remaining == 0),
                        ),
                        yt_service,
                        kb_sort_index,
                    )
            elif result == "skipped":
                skip += 1
                totals[item.orig_idx][0] += 1
            else:
                fail += 1
                executor._set_job_status(item.orig_idx, f"Fehler: {item.cv_job.error_msg[:60]}")
                executor.job_progress.emit(item.orig_idx, 0)

            executor.overall_progress.emit(conv_pos + 1, len(to_convert))

        return ok, skip, fail

    @staticmethod
    def _build_job_totals(items: list[ConvertItem]) -> dict[int, list[int]]:
        totals: dict[int, list[int]] = {}
        for item in items:
            if item.orig_idx not in totals:
                totals[item.orig_idx] = [0, 0]
            totals[item.orig_idx][1] += 1
        return totals

    @staticmethod
    def _update_job_progress(executor: Any, orig_idx: int, totals: list[int]) -> None:
        done, total = totals
        remaining = total - done
        if remaining == 0:
            executor._set_job_status(orig_idx, "Fertig")
            executor.job_progress.emit(orig_idx, 100)
        else:
            executor._set_job_status(orig_idx, f"Fertig {done}/{total}")

    def _run_merge_groups(
        self,
        executor: Any,
        merge_items: list[ConvertItem],
        yt_service: Any,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> int:
        grouped: dict[str, list[ConvertItem]] = defaultdict(list)
        for item in merge_items:
            merge_group_id = executor._get_merge_group_id(item.job, str(item.cv_job.source_path))
            if merge_group_id:
                grouped[merge_group_id].append(item)

        if grouped:
            executor.phase_changed.emit("Zusammenführen …")

        fail = 0
        for merge_group_id, group in grouped.items():
            prepared, merge_fail = executor._merge_step.execute(executor, merge_group_id, group)
            fail += merge_fail
            if prepared is None:
                continue
            fail += executor._run_output_steps(prepared, yt_service, kb_sort_index)
            executor.job_progress.emit(prepared.orig_idx, 100)
        return fail

    def _run_upload_only(
        self,
        executor: Any,
        upload_only: list[ConvertItem],
        yt_service: Any,
        kb_sort_index: dict[tuple[str, str], int],
    ) -> int:
        if not upload_only:
            return 0

        executor.phase_changed.emit("YouTube-Upload …")
        upload_totals = self._build_job_totals(upload_only)
        total_uploads = len(upload_only)
        executor.log_message.emit(
            f"\n{'═' * 60}"
            f"\n  ☁  YouTube-Upload  ({total_uploads} Datei(en))"
            f"\n{'═' * 60}"
        )

        fail = 0
        for upload_pos, item in enumerate(upload_only):
            if executor._cancel.is_set():
                break

            per_settings = executor._build_job_settings(item.job)
            done_count, total_count = upload_totals[item.orig_idx]
            executor.log_message.emit(
                f"\n═══ [{upload_pos + 1}/{total_uploads}] {item.job.name}: "
                f"{item.cv_job.source_path.name} ═══"
            )
            fail += executor._run_output_steps(
                PreparedOutput(
                    orig_idx=item.orig_idx,
                    job=item.job,
                    cv_job=item.cv_job,
                    per_settings=per_settings,
                    graph_origin_node_id=executor._support.source_node_id_for_file(item.job, str(item.cv_job.source_path)),
                    status_prefix=(
                        f"YouTube-Upload {done_count + 1}/{total_count} …"
                        if total_count > 1 else ""
                    ),
                    mark_finished=False,
                ),
                yt_service,
                kb_sort_index,
            )
            if executor._cancel.is_set():
                break

            upload_totals[item.orig_idx][0] += 1
            done_count = upload_totals[item.orig_idx][0]
            pct = int(done_count / total_count * 100) if total_count else 100
            executor.job_progress.emit(item.orig_idx, pct)
            if done_count >= total_count:
                executor._set_job_status(item.orig_idx, "Fertig")
            else:
                executor._set_job_status(item.orig_idx, f"Fertig {done_count}/{total_count}")
            executor.overall_progress.emit(upload_pos + 1, total_uploads)

        return fail
