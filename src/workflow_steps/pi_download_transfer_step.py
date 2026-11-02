from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ..workflow import WorkflowJob
from .executor_support import ExecutorSupport


class PiDownloadTransferStep:
    name = "pi-download-transfer"

    @staticmethod
    def _covers_selection(job: WorkflowJob, paths: list[str]) -> bool:
        if not job.files:
            return bool(paths)
        expected = {Path(entry.source_path).stem for entry in job.files}
        actual = {Path(path).stem for path in paths}
        return actual == expected

    def execute(
        self,
        executor: Any,
        orig_idx: int,
        job: WorkflowJob,
        on_file_ready: Callable[[str], None] | None = None,
    ) -> list[str]:
        cancel_flag = ExecutorSupport.cancel_flag_for_job(executor, orig_idx)
        executor.source_progress.emit(orig_idx, 0)
        device = next(
            (configured for configured in executor._settings.cameras.devices if configured.name == job.device_name),
            None,
        )
        if not device:
            raise RuntimeError(f"Gerät '{job.device_name}' nicht in den Einstellungen")

        executor.log_message.emit(f"\n⬇ {job.name}: Download von {device.ip} …")
        state = {"speed": 0.0, "last_t": 0.0, "last_b": 0, "file": ""}

        def _on_progress(device_name, filename, transferred, total):
            import time

            now = time.monotonic()
            if filename != state["file"]:
                state.update(speed=0.0, last_t=now, last_b=transferred, file=filename)
            if transferred < total and (now - state["last_t"]) < 0.25:
                return
            dt = now - state["last_t"]
            if dt >= 0.5 and state["last_t"] > 0:
                raw = (transferred - state["last_b"]) / dt
                state["speed"] = raw if state["speed"] == 0 else 0.3 * raw + 0.7 * state["speed"]
                state["last_b"] = transferred
                state["last_t"] = now
            elif state["last_t"] == 0:
                state.update(last_b=transferred, last_t=now)
            if total > 0:
                pct = int(transferred / total * 100)
                executor.source_progress.emit(orig_idx, pct)
            executor.file_progress.emit(
                device_name,
                filename,
                float(transferred),
                float(total),
                state["speed"],
            )
            if hasattr(executor, "_pump_pipeline_events"):
                executor._pump_pipeline_events()

        selective = {Path(entry.source_path).stem for entry in job.files} if job.files else None
        destination_root = ExecutorSupport.resolve_download_destination(executor._settings, job)

        try:
            results = executor._download_func(
                device=device,
                config=executor._settings.cameras,
                log_cb=executor.log_message.emit,
                progress_cb=_on_progress,
                cancel_flag=cancel_flag,
                allow_reuse_existing=ExecutorSupport.allow_reuse_existing(executor),
                destination_override=str(destination_root) if destination_root is not None else "",
                create_device_subdir=False,
                delete_after_download=job.delete_after_download,
                selective_bases=selective,
                raise_on_failure=True,
            )
        except Exception as exc:
            if cancel_flag.is_set():
                executor._set_step_status(job, "transfer", "cancelled")
                executor._set_step_detail(job, "transfer", f"Download von {device.name} ({device.ip}) | Durch Benutzer abgebrochen")
                executor._set_job_status(orig_idx, "Transfer abgebrochen")
                return []
            fallback = self._existing_targets(executor, job) if ExecutorSupport.allow_reuse_existing(executor) else []
            if self._covers_selection(job, fallback):
                executor.log_message.emit(
                    f"  ↩ Download nicht möglich ({exc}) – nutze {len(fallback)} vorhandene Datei(en) im Zielverzeichnis"
                )
                executor._set_step_status(job, "transfer", "reused-target")
                executor.source_progress.emit(orig_idx, 100)
                if on_file_ready is not None:
                    for path in fallback:
                        on_file_ready(path)
                return fallback
            raise

        paths = [result[2] for result in results]
        if cancel_flag.is_set():
            executor._set_step_status(job, "transfer", "cancelled")
            executor._set_step_detail(job, "transfer", f"Download von {device.name} ({device.ip}) | Durch Benutzer abgebrochen")
            executor._set_job_status(orig_idx, "Transfer abgebrochen")
            return []
        if self._covers_selection(job, paths):
            executor._set_step_detail(
                job,
                "transfer",
                f"Download von {device.name} ({device.ip}) | {len(paths)} Datei(en)",
            )
            if on_file_ready is not None:
                for path in paths:
                    on_file_ready(path)
            return paths

        fallback = self._existing_targets(executor, job) if ExecutorSupport.allow_reuse_existing(executor) else []
        if self._covers_selection(job, fallback):
            executor.log_message.emit(
                f"  ↩ Keine neue Übertragung – nutze {len(fallback)} vorhandene Datei(en) im Zielverzeichnis"
            )
            executor._set_step_status(job, "transfer", "reused-target")
            executor.source_progress.emit(orig_idx, 100)
            if on_file_ready is not None:
                for path in fallback:
                    on_file_ready(path)
            return fallback
        selected = len(job.files)
        selection = f"{selected} ausgewählte Aufnahme(n)" if selected else "keine vollständige Aufnahme"
        raise RuntimeError(
            f"Download von {device.name} ({device.ip}) fehlgeschlagen: {selection} vollständig übertragen"
        )

    @staticmethod
    def _existing_targets(executor: Any, job: WorkflowJob) -> list[str]:
        dest_root = ExecutorSupport.resolve_download_destination(executor._settings, job)
        if dest_root is None:
            return []
        local_dir = dest_root
        if not local_dir.exists():
            return []
        # Ein Dateiname allein belegt keinen erfolgreichen Download. Gerade
        # rsync behaelt partielle Dateien fuer das Fortsetzen. Deshalb darf
        # nur ein vollstaendiges MJPG/WAV-Paar mit exakt der beim Einlesen der
        # Kamera ermittelten Gesamtgroesse wiederverwendet werden.
        complete: list[str] = []
        for entry in job.files:
            expected_size = int(entry.source_size_bytes or 0)
            if expected_size <= 0:
                continue
            stem = Path(entry.source_path).stem
            mjpg_path = local_dir / f"{stem}.mjpg"
            wav_path = local_dir / f"{stem}.wav"
            if not mjpg_path.is_file() or not wav_path.is_file():
                continue
            try:
                actual_size = mjpg_path.stat().st_size + wav_path.stat().st_size
            except OSError:
                continue
            if actual_size == expected_size:
                complete.append(str(mjpg_path))
        return complete
