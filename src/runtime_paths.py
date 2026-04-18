"""Runtime path and platform helpers for source and bundled app execution."""

from __future__ import annotations

import os
import platform
import signal
import subprocess
import sys
from pathlib import Path


def is_frozen_app() -> bool:
    return bool(getattr(sys, "frozen", False))


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def portable_root() -> Path:
    if is_frozen_app():
        return Path(sys.executable).resolve().parent
    return project_root()


def bundled_resource_root() -> Path:
    meipass = getattr(sys, "_MEIPASS", "")
    if meipass:
        return Path(meipass)
    return project_root()


def asset_path(*parts: str) -> Path:
    return bundled_resource_root().joinpath("assets", *parts)


def config_dir() -> Path:
    return portable_root() / "config"


def data_dir() -> Path:
    return portable_root() / "data"


def workflows_dir() -> Path:
    return portable_root() / "workflows"


def bundled_binary_path(env_var: str, executable_name: str) -> str:
    configured = os.environ.get(env_var, "").strip()
    if configured:
        return configured

    suffix = ".exe" if platform.system() == "Windows" else ""
    candidate = bundled_resource_root() / "bin" / f"{executable_name}{suffix}"
    if candidate.exists():
        return str(candidate)

    return executable_name + suffix


def creationflags_for_new_process_group() -> int:
    if platform.system() == "Windows":
        return int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    return 0


def popen_process_group_kwargs() -> dict:
    if platform.system() == "Windows":
        return {"creationflags": creationflags_for_new_process_group()}
    return {"preexec_fn": os.setsid}


def terminate_process_tree(process: subprocess.Popen, *, force: bool = False) -> None:
    if process.poll() is not None:
        return

    if platform.system() == "Windows":
        sig = getattr(signal, "CTRL_BREAK_EVENT", signal.SIGTERM) if not force else signal.SIGTERM
        try:
            process.send_signal(sig)
        except Exception:
            if force:
                try:
                    process.kill()
                except Exception:
                    pass
        return

    sig = signal.SIGKILL if force else signal.SIGTERM
    try:
        os.killpg(os.getpgid(process.pid), sig)
    except (ProcessLookupError, OSError):
        pass


def null_device_path() -> str:
    return os.devnull


def supports_rsync() -> bool:
    return platform.system() != "Windows"


def shutdown_command() -> list[str] | None:
    system = platform.system()
    if system == "Linux":
        return ["shutdown", "now"]
    if system == "Darwin":
        return ["sudo", "shutdown", "-h", "now"]
    if system == "Windows":
        return ["shutdown", "/s", "/t", "0"]
    return None