#!/usr/bin/env python3
"""Build a portable release archive for the current platform."""

from __future__ import annotations

import platform
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SPEC_FILE = ROOT / "video_manager.spec"
DIST_DIR = ROOT / "dist"
STAGE_DIR = DIST_DIR / "kaderblick-video-manager"
ARTIFACT_DIR = ROOT / "dist-artifacts"


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


def main() -> int:
    build_cmd = [sys.executable, "-m", "PyInstaller", "--noconfirm", str(SPEC_FILE)]
    print("[build]", " ".join(build_cmd))
    subprocess.run(build_cmd, cwd=ROOT, check=True)

    _prepare_runtime_dirs()
    for binary in ("ffmpeg", "ffprobe"):
        _copy_binary(binary)

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    archive_base = ARTIFACT_DIR / _archive_name()
    archive_path = shutil.make_archive(str(archive_base), "zip", root_dir=DIST_DIR, base_dir=STAGE_DIR.name)
    print(f"[build] Archiv erstellt: {archive_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())