#!/usr/bin/env python3
"""Build a portable release archive for the current platform."""

from __future__ import annotations

import hashlib
import platform
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SPEC_FILE = ROOT / "video_manager.spec"
DIST_DIR = ROOT / "dist"
STAGE_DIR = DIST_DIR / "kaderblick-video-manager"
ARTIFACT_DIR = ROOT / "dist-artifacts"
MAX_RELEASE_ASSET_BYTES = 2_000_000_000
CHUNK_SIZE_BYTES = 1_900_000_000


def _platform_slug() -> str:
    value = platform.system().lower()
    return {
        "darwin": "macos",
    }.get(value, value)


def _arch_slug() -> str:
    machine = platform.machine().lower()
    return {
        "x86_64": "x64",
        "amd64": "x64",
        "aarch64": "arm64",
        "arm64": "arm64",
    }.get(machine, machine or "unknown")


def _copy_binary(name: str) -> None:
    source = shutil.which(name)
    if not source:
        print(f"[build] {name} nicht gefunden, wird nicht eingebettet")
        return

    target_dir = STAGE_DIR / "bin"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / Path(source).name
    shutil.copy2(source, target)
    print(f"[build] eingebettet: {target.relative_to(ROOT)}")


def _prepare_runtime_dirs() -> None:
    for relative in ("config", "data", "workflows"):
        (STAGE_DIR / relative).mkdir(parents=True, exist_ok=True)

    readme = ROOT / "README.md"
    if readme.exists():
        shutil.copy2(readme, STAGE_DIR / readme.name)


def _archive_name() -> str:
    return f"kaderblick-video-manager-{_platform_slug()}-{_arch_slug()}"


def _write_sha256(path: Path) -> Path:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    checksum_path = path.with_name(path.name + ".sha256")
    checksum_path.write_text(f"{digest.hexdigest()}  {path.name}\n", encoding="utf-8")
    return checksum_path


def _split_file(path: Path, chunk_size: int | None = None) -> list[Path]:
    if chunk_size is None:
        chunk_size = CHUNK_SIZE_BYTES
    parts: list[Path] = []
    with path.open("rb") as source:
        index = 0
        while True:
            payload = source.read(chunk_size)
            if not payload:
                break
            part_path = path.with_name(f"{path.name}.part-{index:02d}")
            part_path.write_bytes(payload)
            parts.append(part_path)
            index += 1
    return parts


def _write_split_instructions(parts: list[Path], original_name: str) -> Path:
    instruction_path = parts[0].with_name(f"{original_name}.parts.txt")
    joined_names = " ".join(part.name for part in parts)
    instruction_path.write_text(
        "Linux release asset was split to stay below the GitHub 2 GB per-asset limit.\n"
        "Reassemble with:\n"
        f"  cat {joined_names} > {original_name}\n"
        f"  sha256sum -c {original_name}.sha256\n",
        encoding="utf-8",
    )
    return instruction_path


def _build_linux_archive(base_path: Path) -> Path:
    archive_path = base_path.with_suffix(".tar.gz")
    with tarfile.open(archive_path, "w:gz", dereference=False) as archive:
        archive.add(STAGE_DIR, arcname=STAGE_DIR.name, recursive=True)
    return archive_path


def _build_standard_archive(base_path: Path) -> Path:
    archive_path = shutil.make_archive(str(base_path), "zip", root_dir=DIST_DIR, base_dir=STAGE_DIR.name)
    return Path(archive_path)


def _finalize_archive(archive_path: Path) -> list[Path]:
    checksum_path = _write_sha256(archive_path)
    produced: list[Path] = [archive_path, checksum_path]
    if archive_path.stat().st_size > MAX_RELEASE_ASSET_BYTES:
        parts = _split_file(archive_path)
        instruction_path = _write_split_instructions(parts, archive_path.name)
        archive_path.unlink()
        produced = [*parts, checksum_path, instruction_path]
    return produced


def main() -> int:
    build_cmd = [sys.executable, "-m", "PyInstaller", "--noconfirm", str(SPEC_FILE)]
    print("[build]", " ".join(build_cmd))
    subprocess.run(build_cmd, cwd=ROOT, check=True)

    _prepare_runtime_dirs()
    for binary in ("ffmpeg", "ffprobe"):
        _copy_binary(binary)

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    archive_base = ARTIFACT_DIR / _archive_name()
    if _platform_slug() == "linux":
        archive_path = _build_linux_archive(archive_base)
    else:
        archive_path = _build_standard_archive(archive_base)

    for produced in _finalize_archive(Path(archive_path)):
        print(f"[build] Artefakt erstellt: {produced}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())