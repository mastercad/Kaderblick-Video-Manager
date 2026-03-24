from __future__ import annotations

from typing import TypedDict

from PySide6.QtCore import QPointF, QRectF, Qt
from PySide6.QtGui import QColor, QPainter, QPen

from ...workflow import WorkflowJob, graph_merge_precedes_convert, graph_reachable_types

_STEP_LABELS = {
    "transfer": "Transfer",
    "convert": "Konvertierung",
    "merge": "Zusammenführen",
    "titlecard": "Titelkarte",
    "validate_surface": "Quick-Check",
    "validate_deep": "Deep-Scan",
    "cleanup": "Cleanup",
    "repair": "Reparatur",
    "yt_version": "YT-Version",
    "stop": "Stop / Log",
    "youtube_upload": "YouTube-Upload",
    "kaderblick": "Kaderblick",
}

_STEP_DETAILS = {
    "transfer": "Dateien laden, kopieren oder verschieben.",
    "convert": "Quelle in das Ziel-Videoformat umwandeln.",
    "merge": "Mehrere Quellen zu einem gemeinsamen Video kombinieren.",
    "titlecard": "Intro-Titelkarte vor das Ergebnis setzen.",
    "validate_surface": "Schnelle Lesbarkeits- und Kompatibilitätsprüfung des aktuellen Artefakts.",
    "validate_deep": "Vollständiger Tiefenscan inkl. Dekodierlauf, Zeitstempel- und Frame-Prüfung.",
    "cleanup": "Alte Zwischenergebnisse und abgeleitete Dateien vor einem neuen Lauf gezielt entfernen.",
    "repair": "Ergebnisdatei prüfen und bereinigt neu aufbauen.",
    "yt_version": "YouTube-optimierte Version erzeugen.",
    "stop": "Schreibt einen Abschluss-Logeintrag und beendet diesen Branch ohne weitere Verarbeitung.",
    "youtube_upload": "Video auf YouTube hochladen.",
    "kaderblick": "YouTube-Ergebnis bei Kaderblick eintragen.",
}

_VALIDATION_OUTPUT_BRANCHES = [
    ("ok", "OK"),
    ("repairable", "reparierbar"),
    ("irreparable", "irreparabel"),
]

_LANE_NODE_COLORS = {
    "transfer": "#DBEAFE",
    "processing": "#DCFCE7",
    "delivery": "#FEF3C7",
}

_LANE_LABELS = {
    "transfer": "Quellen",
    "processing": "Verarbeitung",
    "delivery": "Ziele",
}

_NODE_IDLE_FILL = "#FFFFFF"

_NODE_DEFINITIONS = {
    "source_files": {
        "label": "Dateien",
        "detail": "Direkte Dateiauswahl",
        "category": "Quellen",
        "lane": "transfer",
        "has_input": False,
        "has_output": True,
    },
    "source_folder_scan": {
        "label": "Ordner-Scan",
        "detail": "Ordner einlesen und Muster filtern",
        "category": "Quellen",
        "lane": "transfer",
        "has_input": False,
        "has_output": True,
    },
    "source_pi_download": {
        "label": "Pi-Download",
        "detail": "Aufnahmen von Pi-Kamera holen",
        "category": "Quellen",
        "lane": "transfer",
        "has_input": False,
        "has_output": True,
    },
    "convert": {
        "label": "Konvertierung",
        "detail": _STEP_DETAILS["convert"],
        "category": "Verarbeitung",
        "lane": "processing",
        "has_input": True,
        "has_output": True,
    },
    "merge": {
        "label": "Merge",
        "detail": _STEP_DETAILS["merge"],
        "category": "Verarbeitung",
        "lane": "processing",
        "has_input": True,
        "has_output": True,
    },
    "titlecard": {
        "label": "Titelkarte",
        "detail": _STEP_DETAILS["titlecard"],
        "category": "Verarbeitung",
        "lane": "processing",
        "has_input": True,
        "has_output": True,
    },
    "validate_surface": {
        "label": "Quick-Check",
        "detail": _STEP_DETAILS["validate_surface"],
        "category": "Prüfung",
        "lane": "processing",
        "has_input": True,
        "has_output": True,
        "output_branches": _VALIDATION_OUTPUT_BRANCHES,
    },
    "validate_deep": {
        "label": "Deep-Scan",
        "detail": _STEP_DETAILS["validate_deep"],
        "category": "Prüfung",
        "lane": "processing",
        "has_input": True,
        "has_output": True,
        "output_branches": _VALIDATION_OUTPUT_BRANCHES,
    },
    "cleanup": {
        "label": "Cleanup",
        "detail": _STEP_DETAILS["cleanup"],
        "category": "Kontrolle",
        "lane": "processing",
        "has_input": True,
        "has_output": True,
    },
    "repair": {
        "label": "Reparatur",
        "detail": _STEP_DETAILS["repair"],
        "category": "Verarbeitung",
        "lane": "processing",
        "has_input": True,
        "has_output": True,
    },
    "yt_version": {
        "label": "YT-Version",
        "detail": _STEP_DETAILS["yt_version"],
        "category": "Verarbeitung",
        "lane": "processing",
        "has_input": True,
        "has_output": True,
    },
    "stop": {
        "label": "Stop / Log",
        "detail": _STEP_DETAILS["stop"],
        "category": "Kontrolle",
        "lane": "delivery",
        "has_input": True,
        "has_output": False,
    },
    "youtube_upload": {
        "label": "YouTube Upload",
        "detail": _STEP_DETAILS["youtube_upload"],
        "category": "Ziele",
        "lane": "delivery",
        "has_input": True,
        "has_output": True,
    },
    "kaderblick": {
        "label": "Kaderblick",
        "detail": _STEP_DETAILS["kaderblick"],
        "category": "Ziele",
        "lane": "delivery",
        "has_input": True,
        "has_output": False,
    },
}

_SOURCE_NODE_TYPES = {"source_files", "source_folder_scan", "source_pi_download"}
_STEP_NODE_TYPES = {key for key in _NODE_DEFINITIONS if key not in _SOURCE_NODE_TYPES}
_STEP_LANES = {node_type: str(definition["lane"]) for node_type, definition in _NODE_DEFINITIONS.items()}
_UNIQUE_NODE_TYPES = _SOURCE_NODE_TYPES | {"convert", "merge", "validate_surface", "validate_deep", "cleanup", "repair", "yt_version", "stop", "youtube_upload", "kaderblick"}
_OUTGOING_RULES = {
    "source_files": {"convert", "merge", "titlecard", "validate_surface", "validate_deep", "cleanup", "repair", "yt_version", "stop", "youtube_upload"},
    "source_folder_scan": {"convert", "merge", "titlecard", "validate_surface", "validate_deep", "cleanup", "repair", "yt_version", "stop", "youtube_upload"},
    "source_pi_download": {"convert", "merge", "titlecard", "validate_surface", "validate_deep", "cleanup", "repair", "yt_version", "stop", "youtube_upload"},
    "convert": {"merge", "titlecard", "validate_surface", "validate_deep", "cleanup", "repair", "yt_version", "stop", "youtube_upload"},
    "merge": {"titlecard", "validate_surface", "validate_deep", "cleanup", "repair", "yt_version", "stop", "youtube_upload"},
    "titlecard": {"merge", "validate_surface", "validate_deep", "cleanup", "repair", "yt_version", "stop", "youtube_upload"},
    "validate_surface": {"validate_deep", "cleanup", "repair", "yt_version", "stop", "youtube_upload"},
    "validate_deep": {"cleanup", "repair", "yt_version", "stop", "youtube_upload"},
    "cleanup": {"repair", "yt_version", "stop", "youtube_upload"},
    "repair": {"validate_surface", "validate_deep", "cleanup", "yt_version", "stop", "youtube_upload"},
    "yt_version": {"validate_surface", "validate_deep", "cleanup", "stop", "youtube_upload"},
    "stop": set(),
    "youtube_upload": {"stop", "kaderblick"},
    "kaderblick": set(),
}

_STATE_META = {
    "pending": ("Ausstehend", "#E5E7EB", "#4B5563", 0),
    "running": ("Läuft", "#DBEAFE", "#1D4ED8", None),
    "done": ("Fertig", "#DCFCE7", "#166534", 100),
    "reused-target": ("Vorhanden", "#FEF3C7", "#92400E", 100),
    "skipped": ("Übersprungen", "#F3F4F6", "#6B7280", 100),
    "ok": ("OK", "#DCFCE7", "#166534", 100),
    "repairable": ("Reparierbar", "#FEF3C7", "#92400E", 100),
    "irreparable": ("Irreparabel", "#FEE2E2", "#B91C1C", 100),
    "error": ("Fehler", "#FEE2E2", "#B91C1C", 0),
}


class NodeVisualState(TypedDict):
    label: str
    detail: str
    state_text: str
    state_color: QColor
    fill_color: QColor
    progress_fill_color: QColor
    progress_fraction: float
    has_input: bool
    has_output: bool
    output_branches: list[tuple[str, str]]


def _node_output_branches(node_type: str) -> list[tuple[str, str]]:
    definition = _NODE_DEFINITIONS[node_type]
    branches = definition.get("output_branches")
    if isinstance(branches, list):
        return [(str(branch), str(label)) for branch, label in branches]
    if bool(definition["has_output"]):
        return [("", "")]
    return []


def _planned_job_steps(job: WorkflowJob) -> list[str]:
    has_merge = any(file.merge_group_id for file in job.files)
    reachable_types = graph_reachable_types(job) if getattr(job, "graph_nodes", None) else set()
    has_graph = bool(getattr(job, "graph_nodes", None))
    convert_enabled = "convert" in reachable_types if has_graph else job.convert_enabled
    titlecard_enabled = "titlecard" in reachable_types if has_graph else job.title_card_enabled
    surface_validation_enabled = "validate_surface" in reachable_types if has_graph else False
    deep_validation_enabled = "validate_deep" in reachable_types if has_graph else False
    cleanup_enabled = "cleanup" in reachable_types if has_graph else False
    has_repair = "repair" in reachable_types if has_graph else False
    youtube_version_enabled = "yt_version" in reachable_types if has_graph else job.create_youtube_version
    stop_enabled = "stop" in reachable_types if has_graph else False
    youtube_upload_enabled = "youtube_upload" in reachable_types if has_graph else job.upload_youtube
    kaderblick_enabled = "kaderblick" in reachable_types if has_graph else (job.upload_youtube and job.upload_kaderblick)
    if has_graph:
        has_output_stack = (
            convert_enabled
            or has_merge
            or youtube_upload_enabled
            or youtube_version_enabled
            or has_repair
            or surface_validation_enabled
            or deep_validation_enabled
            or cleanup_enabled
            or stop_enabled
        )
    else:
        has_output_stack = convert_enabled or has_merge or youtube_upload_enabled

    steps = ["transfer"]
    if has_merge and graph_merge_precedes_convert(job):
        steps.append("merge")
        if convert_enabled:
            steps.append("convert")
    else:
        if convert_enabled:
            steps.append("convert")
        if has_merge:
            steps.append("merge")
    if has_output_stack and titlecard_enabled:
        steps.append("titlecard")
    if has_output_stack and surface_validation_enabled:
        steps.append("validate_surface")
    if has_output_stack and deep_validation_enabled:
        steps.append("validate_deep")
    if has_output_stack and cleanup_enabled:
        steps.append("cleanup")
    if has_output_stack and has_repair:
        steps.append("repair")
    if has_output_stack and youtube_version_enabled:
        steps.append("yt_version")
    if has_output_stack and stop_enabled:
        steps.append("stop")
    if has_output_stack and youtube_upload_enabled:
        steps.append("youtube_upload")
    if has_output_stack and kaderblick_enabled:
        steps.append("kaderblick")
    return steps


def _infer_current_step(job: WorkflowJob) -> str:
    if job.current_step_key:
        return job.current_step_key

    status = job.resume_status or ""
    prefixes = (
        ("Transfer", "transfer"),
        ("Konvertiere", "convert"),
        ("Zusammenführen", "merge"),
        ("Titelkarte", "titlecard"),
        ("Kompatibilität prüfen", "validate_surface"),
        ("Deep-Scan", "validate_deep"),
        ("Bereinige Altdateien", "cleanup"),
        ("Repariere", "repair"),
        ("YT-Version", "yt_version"),
        ("Workflow-Zweig beendet", "stop"),
        ("YouTube-Upload", "youtube_upload"),
        ("Kaderblick", "kaderblick"),
    )
    for prefix, step_key in prefixes:
        if status.startswith(prefix):
            return step_key
    for step_key in reversed(_planned_job_steps(job)):
        if step_key in job.step_statuses:
            return step_key
    return "transfer"


def _normalized_step_status(job: WorkflowJob, step_key: str) -> str:
    raw = job.step_statuses.get(step_key, "") if isinstance(job.step_statuses, dict) else ""
    if isinstance(raw, str) and raw.startswith("error"):
        return "error"
    if raw in {"running", "done", "reused-target", "skipped", "ok", "repairable", "irreparable", "error"}:
        return raw
    if step_key == _infer_current_step(job) and (job.resume_status or raw):
        if raw not in {"done", "reused-target", "skipped"}:
            return "running"
    return "pending"


def _step_progress(job: WorkflowJob, step_key: str, state: str) -> int:
    default_progress = _STATE_META[state][3]
    if default_progress is not None:
        return int(default_progress)
    if step_key == _infer_current_step(job):
        return max(0, min(job.progress_pct, 100))
    return 0


def _workflow_editor_encoder_choices() -> list[tuple[str, str]]:
    return [
        ("auto", "Automatisch (NVENC falls verfügbar)"),
        ("h264_nvenc", "NVIDIA NVENC (GPU)"),
        ("libx264", "libx264 (CPU)"),
    ]


def _node_visual_state(node_type: str, job: WorkflowJob | None = None) -> NodeVisualState:
    definition = _NODE_DEFINITIONS[node_type]
    lane = str(definition["lane"])
    is_source_node = node_type in _SOURCE_NODE_TYPES
    step_key = "transfer" if is_source_node else (node_type if node_type in _STEP_LABELS else None)
    state = _normalized_step_status(job, step_key) if job is not None and step_key else "pending"
    label, _bg, fg, _default_pct = _STATE_META[state]
    progress = _step_progress(job, step_key, state) if job is not None and step_key else 0
    if is_source_node and job is not None:
        transfer_progress = max(0, min(getattr(job, "transfer_progress_pct", progress), 100))
        if state in {"done", "reused-target", "skipped", "ok"}:
            progress = 100
        elif state == "error":
            progress = transfer_progress if transfer_progress > 0 else progress
        else:
            progress = transfer_progress
    lane_color = QColor(_LANE_NODE_COLORS[lane])
    base = QColor(_NODE_IDLE_FILL)
    progress_fill = QColor(lane_color)
    progress_fill.setAlpha(224)
    if state == "error":
        base = QColor("#FEE2E2")
        progress_fill = QColor("#FCA5A5")
        progress_fill.setAlpha(240)
    elif state in {"done", "reused-target", "skipped"}:
        base = QColor(lane_color)
        base.setAlpha(96)
        progress_fill = QColor(lane_color)
        progress_fill.setAlpha(255)
    progress_fraction = 0.0
    if step_key:
        if state in {"done", "reused-target", "skipped"}:
            progress_fraction = 1.0
        elif state in {"error", "running"}:
            progress_fraction = max(0.0, min(progress / 100.0, 1.0))
    return {
        "label": str(definition["label"]),
        "detail": str(definition["detail"]),
        "state_text": f"{label} · {progress}%",
        "state_color": QColor(fg),
        "fill_color": base,
        "progress_fill_color": progress_fill,
        "progress_fraction": progress_fraction,
        "has_input": bool(definition["has_input"]),
        "has_output": bool(definition["has_output"]),
        "output_branches": _node_output_branches(node_type),
    }


def _paint_node_card(
    painter: QPainter,
    rect: QRectF,
    *,
    fill_color: QColor,
    border_color: QColor,
    title: str,
    detail: str,
    state_text: str,
    state_color: QColor,
    progress_fill_color: QColor,
    progress_fraction: float,
    has_input: bool,
    has_output: bool,
    output_branches: list[tuple[str, str]],
    port_radius: float,
) -> None:
    painter.save()
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    painter.setPen(Qt.PenStyle.NoPen)
    painter.setBrush(fill_color)
    painter.drawRoundedRect(rect, 12, 12)

    fraction = max(0.0, min(progress_fraction, 1.0))
    if fraction > 0.0:
        progress_rect = QRectF(rect.left(), rect.top(), rect.width() * fraction, rect.height())
        painter.setBrush(progress_fill_color)
        painter.drawRoundedRect(progress_rect, 12, 12)
    painter.setPen(QPen(border_color, 2))
    painter.setBrush(Qt.BrushStyle.NoBrush)
    painter.drawRoundedRect(rect, 12, 12)

    title_rect = QRectF(rect.left() + 14, rect.top() + 10, rect.width() - 34, 22)
    detail_rect = QRectF(rect.left() + 14, rect.top() + 34, rect.width() - 34, 24)
    state_rect = QRectF(rect.left() + 14, rect.top() + rect.height() - 26, rect.width() - 34, 18)

    if has_input:
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor("#0F172A"))
        painter.drawEllipse(QPointF(rect.left(), rect.center().y()), port_radius, port_radius)

    if has_output:
        branches = output_branches or [("", "")]
        for index, (_branch, branch_label) in enumerate(branches, start=1):
            y = rect.top() + rect.height() * index / (len(branches) + 1)
            painter.setBrush(QColor("#FFFFFF"))
            painter.setPen(QPen(border_color, 2))
            painter.drawEllipse(QPointF(rect.right(), y), port_radius, port_radius)
            if branch_label:
                label_rect = QRectF(rect.right() - 86, y - 10, 72, 20)
                painter.setPen(QColor("#475569"))
                painter.drawText(label_rect, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, branch_label)
    title_font = painter.font()
    title_font.setBold(True)
    painter.setFont(title_font)
    painter.setPen(QColor("#0F172A"))
    painter.drawText(title_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, title)

    detail_font = painter.font()
    detail_font.setBold(False)
    detail_font.setPointSize(max(detail_font.pointSize() - 1, 8))
    painter.setFont(detail_font)
    painter.setPen(QColor("#475569"))
    painter.drawText(detail_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, detail)

    state_font = painter.font()
    state_font.setPointSize(max(state_font.pointSize() - 1, 8))
    painter.setFont(state_font)
    painter.setPen(state_color)
    painter.drawText(state_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, state_text)
    painter.restore()