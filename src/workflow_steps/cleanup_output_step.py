from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import PreparedOutput


class CleanupOutputStep:
    name = "cleanup"

    _NORMALIZED_SUFFIXES = ("_repaired", "_youtube", "_titlecard", "_tmp_intro", "_tmp")
    _DERIVED_SUFFIXES = (
        "_youtube",
        "_repaired",
        "_repaired_youtube",
        "_titlecard",
        "_titlecard_youtube",
        "_tmp_intro",
        "_repaired_tmp",
    )

    def execute(self, executor: Any, prepared: PreparedOutput) -> int:
        current_output = prepared.cv_job.output_path or prepared.cv_job.source_path
        if current_output is None:
            return 0

        current_output = Path(current_output)
        executor._set_step_status(prepared.job, self.name, "running")
        executor._set_job_status(prepared.orig_idx, "Bereinige Altdateien …")
        executor.job_progress.emit(prepared.orig_idx, 0)

        removed: list[Path] = []
        for candidate in self._candidate_paths(prepared, current_output):
            if candidate == current_output or not candidate.exists():
                continue
            try:
                candidate.unlink()
                removed.append(candidate)
            except OSError:
                executor.log_message.emit(f"⚠ Cleanup konnte Datei nicht löschen: {candidate.name}")

        if removed:
            executor.log_message.emit("Cleanup entfernt Altdateien: " + ", ".join(path.name for path in removed))
            executor._set_step_detail(
                prepared.job,
                self.name,
                f"Entfernt: {', '.join(path.name for path in removed[:6])}{' …' if len(removed) > 6 else ''}",
            )
            executor._set_job_status(prepared.orig_idx, f"Cleanup OK: {len(removed)} Altdatei(en) entfernt")
        else:
            executor._set_step_detail(prepared.job, self.name, "Keine Altdateien gefunden")
            executor._set_job_status(prepared.orig_idx, "Cleanup OK: keine Altdateien gefunden")

        executor._set_step_status(prepared.job, self.name, "done")
        executor.job_progress.emit(prepared.orig_idx, 100)
        return 0

    @classmethod
    def _candidate_paths(cls, prepared: PreparedOutput, current_output: Path) -> set[Path]:
        base_paths = {prepared.cv_job.source_path, current_output}
        stems: set[str] = set()
        parents: set[Path] = set()
        suffixes: set[str] = {".mp4"}
        derived_output_dir = str(getattr(prepared.cv_job, "derived_output_dir", "") or "").strip()

        for path in base_paths:
            parents.add(path.parent)
            if path.suffix:
                suffixes.add(path.suffix)
            stems.add(cls._normalize_stem(path.stem))

        if derived_output_dir:
            parents.add(Path(derived_output_dir))

        candidates: set[Path] = set()
        for parent in parents:
            for stem in stems:
                for suffix in suffixes:
                    for derived in cls._DERIVED_SUFFIXES:
                        candidates.add(parent / f"{stem}{derived}{suffix}")
        return candidates

    @classmethod
    def _normalize_stem(cls, stem: str) -> str:
        normalized = stem
        changed = True
        while changed:
            changed = False
            for suffix in cls._NORMALIZED_SUFFIXES:
                if normalized.endswith(suffix):
                    normalized = normalized[: -len(suffix)]
                    changed = True
                    break
        return normalized or stem