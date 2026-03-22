from __future__ import annotations

from pathlib import Path
from typing import Any


class DeleteSourcesStep:
    name = "delete-sources"

    def execute(self, _executor: Any, source_paths: list[Path]) -> None:
        for src in source_paths:
            try:
                src.unlink()
            except OSError:
                pass
