from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ..workflow import WorkflowJob


class PiDownloadTransferStep:
    name = "pi-download-transfer"

    def execute(
        self,
        executor: Any,
        orig_idx: int,
        job: WorkflowJob,
        on_file_ready: Callable[[str], None] | None = None,
    ) -> list[str]:
        executor.job_progress.emit(orig_idx, 0)
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
                executor.job_progress.emit(orig_idx, pct)
                executor.source_progress.emit(orig_idx, pct)
            executor.file_progress.emit(
                device_name,
                filename,
                float(transferred),
                float(total),
                state["speed"],
            )

        selective = {Path(entry.source_path).stem for entry in job.files} if job.files else None

        try:
            results = executor._download_func(
                device=device,
                config=executor._settings.cameras,
                log_cb=executor.log_message.emit,
                progress_cb=_on_progress,
                cancel_flag=executor._cancel,
                destination_override=job.download_destination,
                delete_after_download=job.delete_after_download,
                selective_bases=selective,
            )
        except Exception as exc:
            fallback = self._existing_targets(executor, job)
            if fallback:
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
        if paths:
            executor._set_step_detail(
                job,
                "transfer",
                f"Download von {device.name} ({device.ip}) | {len(paths)} Datei(en)",
            )
            if on_file_ready is not None:
                for path in paths:
                    on_file_ready(path)
            return paths

        fallback = self._existing_targets(executor, job)
        if fallback:
            executor.log_message.emit(
                f"  ↩ Keine neue Übertragung – nutze {len(fallback)} vorhandene Datei(en) im Zielverzeichnis"
            )
            executor._set_step_status(job, "transfer", "reused-target")
            executor.source_progress.emit(orig_idx, 100)
            if on_file_ready is not None:
                for path in fallback:
                    on_file_ready(path)
            return fallback
        return []

    @staticmethod
    def _existing_targets(executor: Any, job: WorkflowJob) -> list[str]:
        dest_root = Path(job.download_destination or executor._settings.cameras.destination)
        local_dir = dest_root / job.device_name if job.device_name else dest_root
        if not local_dir.exists():
            return []
        if job.files:
            wanted = {Path(entry.source_path).stem for entry in job.files}
            return [str(path) for path in sorted(local_dir.glob("*.mjpg")) if path.stem in wanted]
        return [str(path) for path in sorted(local_dir.glob("*.mjpg"))]
