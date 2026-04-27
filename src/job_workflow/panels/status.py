from __future__ import annotations

from PySide6.QtWidgets import QLabel, QFrame, QProgressBar, QVBoxLayout, QWidget

from ..graph import _STEP_LABELS, _infer_current_step, _planned_job_steps
from ...workflow import WorkflowJob, graph_merge_precedes_convert, graph_reachable_types, graph_source_nodes, graph_source_reaches_merge


_STEP_STATUS_LABELS = {
    "pending": "Ausstehend",
    "running": "Läuft",
    "done": "Fertig",
    "reused-target": "Vorhanden",
    "skipped": "Übersprungen",
    "ok": "OK",
    "repairable": "Reparierbar",
    "irreparable": "Irreparabel",
    "error": "Fehler",
}


def build_source_summary(job: WorkflowJob) -> str:
    source_nodes = graph_source_nodes(job)
    if len(source_nodes) > 1:
        return f"{len(source_nodes)} Quellen"
    if job.source_mode == "files":
        return f"{len(job.files)} Datei(en)"
    if job.source_mode == "folder_scan":
        source_folder = (job.source_folder or "").strip()
        return source_folder.rsplit("/", 1)[-1] if source_folder else "Ordner"
    if job.source_mode == "pi_download":
        return job.device_name or "Pi-Kamera"
    return "Quelle"


def build_execution_notes(job: WorkflowJob) -> list[str]:
    notes: list[str] = []
    if any(file.merge_group_id for file in job.files) or any(
        graph_source_reaches_merge(job, node_id) for node_id, _node_type in graph_source_nodes(job)
    ):
        notes.append("Merge-Barriere aktiv: Das Zusammenführen startet erst, wenn alle Gruppenmitglieder bereit sind.")
        if graph_merge_precedes_convert(job):
            notes.append("Reihenfolge aus dem Canvas: Merge läuft vor der Konvertierung. Vorabprüfung prüft daher direkt die Eingangsdateien.")
    else:
        notes.append("Standalone-Pfad: Fertige Dateien können direkt nach dem Transfer in die Verarbeitung laufen.")
    has_graph = bool(getattr(job, "graph_nodes", None))
    reachable = graph_reachable_types(job) if has_graph else set()
    has_upload = bool(reachable & {"youtube_upload", "kaderblick"})
    has_ffmpeg_steps = bool(reachable & {"titlecard", "yt_version"})
    if has_upload:
        notes.append("Upload-Lane separat: Uploads können parallel zur nächsten Konvertierung laufen.")
    if has_ffmpeg_steps:
        notes.append("FFmpeg-Schritte liegen gemeinsam auf der Verarbeitungs-Lane und teilen sich dieselbe GPU-/Encoder-Ressource.")
    return notes


def build_step_summary_lines(job: WorkflowJob) -> list[str]:
    lines: list[str] = []
    for step_key in _planned_job_steps(job):
        status = ""
        if isinstance(job.step_statuses, dict):
            status = str(job.step_statuses.get(step_key, "") or "pending")
        detail = ""
        if isinstance(job.step_details, dict):
            detail = str(job.step_details.get(step_key, "") or "")
        label = _STEP_LABELS.get(step_key, step_key)
        status_label = _STEP_STATUS_LABELS.get(status, status)
        if detail:
            lines.append(f"{label}: {status_label} | {detail}")
        else:
            lines.append(f"{label}: {status_label}")
    return lines


class WorkflowStatusPanel(QFrame):
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setStyleSheet(
            "QFrame { background: #FFFFFF; border: 1px solid #D7E0EA; border-radius: 12px; }"
        )
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(8)

        self._overall_label = QLabel(self)
        self._overall_label.setStyleSheet("font-weight: 600; color: #0F172A;")
        layout.addWidget(self._overall_label)

        self._overall_bar = QProgressBar(self)
        self._overall_bar.setRange(0, 100)
        self._overall_bar.setFormat("%p%")
        layout.addWidget(self._overall_bar)

        self._current_label = QLabel(self)
        self._current_label.setStyleSheet("color: #475569;")
        layout.addWidget(self._current_label)

    def refresh_from_job(self, job: WorkflowJob) -> None:
        self._overall_label.setText(f"Gesamtfortschritt: {job.overall_progress_pct}%")
        self._overall_bar.setValue(max(0, min(job.overall_progress_pct, 100)))
        self._current_label.setText(f"Aktiver Step: {_STEP_LABELS.get(_infer_current_step(job), 'Transfer')}")


class WorkflowNotesPanel(QFrame):
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setStyleSheet(
            "QFrame { background: #FFFDF4; border: 1px solid #F3E7B3; border-radius: 12px; }"
        )
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(14, 12, 14, 12)
        self._layout.setSpacing(4)

        self._title_label = QLabel("Hinweise zur Ausführung")
        self._title_label.setStyleSheet("font-weight: 700; color: #7C4A03;")
        self._layout.addWidget(self._title_label)

    def refresh_from_job(self, job: WorkflowJob) -> None:
        while self._layout.count() > 1:
            item = self._layout.takeAt(1)
            widget = item.widget()
            if widget is not None:
                widget.hide()
                widget.setParent(None)
                widget.deleteLater()

        for note in build_execution_notes(job):
            label = QLabel("• " + note, self)
            label.setWordWrap(True)
            label.setStyleSheet("color: #92400E;")
            self._layout.addWidget(label)

        summary_title = QLabel("Step-Zusammenfassung", self)
        summary_title.setStyleSheet("font-weight: 700; color: #7C4A03; margin-top: 8px;")
        self._layout.addWidget(summary_title)

        for line in build_step_summary_lines(job):
            label = QLabel("• " + line, self)
            label.setWordWrap(True)
            label.setStyleSheet("color: #6B4F1D;")
            self._layout.addWidget(label)