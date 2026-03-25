from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, Callable

from .executor_support import ExecutorSupport


_CHUNK_SIZE = 8 * 1024 * 1024


def _same_directory(left: Path, right: Path) -> bool:
    try:
        return left.resolve(strict=False) == right.resolve(strict=False)
    except Exception:
        return left == right


def transfer_files(
    executor: Any,
    orig_idx: int,
    job: Any,
    paths: list[Path],
    dst_dir: Path,
    *,
    on_file_ready: Callable[[str], None] | None = None,
) -> list[str]:
    total_files = len(paths)
    total_bytes = sum(_path_size(path) for path in paths)
    transferred = 0
    result: list[str] = []

    executor.source_progress.emit(orig_idx, 0)

    for file_idx, source_path in enumerate(paths, start=1):
        if executor._cancel.is_set():
            break

        destination = dst_dir / source_path.name
        executor._set_job_status(orig_idx, f"Transfer {file_idx}/{total_files}: {source_path.name} …")

        if _same_directory(source_path.parent, dst_dir):
            transferred += _path_size(source_path)
            _emit_progress(executor, orig_idx, transferred, total_bytes, file_idx, total_files)
            result.append(str(source_path))
            if on_file_ready is not None:
                on_file_ready(str(source_path))
            continue

        if destination.exists() and ExecutorSupport.allow_reuse_existing(executor):
            executor.log_message.emit(f"  ⚠ Übersprungen (existiert): {source_path.name}")
            transferred += max(_path_size(source_path), _path_size(destination))
            _emit_progress(executor, orig_idx, transferred, total_bytes, file_idx, total_files)
            result.append(str(destination))
            if on_file_ready is not None:
                on_file_ready(str(destination))
            continue

        executor.log_message.emit(f"  → {source_path.name}")
        try:
            if job.move_files:
                _move_path_with_progress(executor, orig_idx, source_path, destination, transferred, total_bytes)
            else:
                _copy_path_with_progress(executor, orig_idx, source_path, destination, transferred, total_bytes)
            transferred += _path_size(destination)
            _emit_progress(executor, orig_idx, transferred, total_bytes, file_idx, total_files)
            result.append(str(destination))
            if on_file_ready is not None:
                on_file_ready(str(destination))
        except Exception as exc:
            executor.log_message.emit(f"  ❌ {source_path.name}: {exc}")

    if not executor._cancel.is_set():
        executor.source_progress.emit(orig_idx, 100)
    return result


def emit_item_progress(executor: Any, orig_idx: int, done: int, total: int) -> None:
    if total <= 0:
        executor.source_progress.emit(orig_idx, 100)
        return
    pct = int(done / total * 100)
    executor.source_progress.emit(orig_idx, pct)


def _move_path_with_progress(
    executor: Any,
    orig_idx: int,
    source_path: Path,
    destination: Path,
    transferred_before: int,
    total_bytes: int,
) -> None:
    try:
        source_path.replace(destination)
    except OSError:
        _copy_path_with_progress(
            executor,
            orig_idx,
            source_path,
            destination,
            transferred_before,
            total_bytes,
        )
        source_path.unlink(missing_ok=True)


def _copy_path_with_progress(
    executor: Any,
    orig_idx: int,
    source_path: Path,
    destination: Path,
    transferred_before: int,
    total_bytes: int,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    copied = 0
    file_size = _path_size(source_path)

    with source_path.open("rb") as src_handle, destination.open("wb") as dst_handle:
        while True:
            if executor._cancel.is_set():
                raise RuntimeError("Abgebrochen")
            chunk = src_handle.read(_CHUNK_SIZE)
            if not chunk:
                break
            dst_handle.write(chunk)
            copied += len(chunk)
            _emit_progress(
                executor,
                orig_idx,
                transferred_before + copied,
                total_bytes,
                None,
                None,
            )

    shutil.copystat(source_path, destination)
    if file_size == 0:
        _emit_progress(executor, orig_idx, transferred_before, total_bytes, None, None)


def _emit_progress(
    executor: Any,
    orig_idx: int,
    transferred: int,
    total_bytes: int,
    file_idx: int | None,
    total_files: int | None,
) -> None:
    if total_bytes > 0:
        pct = min(100, int(transferred / total_bytes * 100))
    elif file_idx is not None and total_files:
        pct = min(100, int(file_idx / total_files * 100))
    else:
        pct = 100
    executor.source_progress.emit(orig_idx, pct)
    if hasattr(executor, "_pump_pipeline_events"):
        executor._pump_pipeline_events()


def _path_size(path: Path) -> int:
    try:
        if path.is_file():
            return path.stat().st_size
    except OSError:
        return 0
    return 0