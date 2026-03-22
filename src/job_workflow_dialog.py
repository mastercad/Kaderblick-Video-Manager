from __future__ import annotations

import copy
from pathlib import Path

from PySide6.QtCore import QCoreApplication, QPointF, QThread, Qt, Signal
from PySide6.QtGui import QColor, QPainter, QPainterPath, QPen
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QGraphicsItem,
    QGraphicsPathItem,
    QGraphicsRectItem,
    QGraphicsScene,
    QGraphicsTextItem,
    QGraphicsView,
    QDoubleSpinBox,
    QFrame,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSplitter,
    QSpinBox,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from .file_list_widget import FileListWidget
from .kaderblick import fetch_cameras, fetch_video_types
from .settings import AppSettings
from .workflow import FileEntry, WorkflowJob
from .youtube_title_editor import MatchData, YouTubeTitleEditorDialog


class _CameraListWorker(QThread):
    finished = Signal(list)
    error = Signal(str)

    def __init__(self, device, config, parent=None):
        super().__init__(parent)
        self._device = device
        self._config = config

    def run(self):
        from .downloader import list_camera_files

        try:
            self.finished.emit(list_camera_files(self._device, self._config))
        except Exception as exc:
            self.error.emit(str(exc))


_STEP_LABELS = {
    "transfer": "Transfer",
    "convert": "Konvertierung",
    "merge": "Zusammenführen",
    "titlecard": "Titelkarte",
    "yt_version": "YT-Version",
    "youtube_upload": "YouTube-Upload",
    "kaderblick": "Kaderblick",
}

_STEP_DETAILS = {
    "transfer": "Dateien laden, kopieren oder verschieben.",
    "convert": "Quelle in das Ziel-Videoformat umwandeln.",
    "merge": "Mehrere Quellen zu einem gemeinsamen Video kombinieren.",
    "titlecard": "Intro-Titelkarte vor das Ergebnis setzen.",
    "yt_version": "YouTube-optimierte Version erzeugen.",
    "youtube_upload": "Video auf YouTube hochladen.",
    "kaderblick": "YouTube-Ergebnis bei Kaderblick eintragen.",
}

_STEP_LANES = {
    "transfer": "transfer",
    "convert": "processing",
    "merge": "processing",
    "titlecard": "processing",
    "yt_version": "processing",
    "youtube_upload": "delivery",
    "kaderblick": "delivery",
}

_LANE_LABELS = {
    "transfer": "Transfer",
    "processing": "Verarbeitung / GPU",
    "delivery": "Upload / Zielsysteme",
}

_LANE_NODE_COLORS = {
    "transfer": "#DBEAFE",
    "processing": "#DCFCE7",
    "delivery": "#FEF3C7",
}

_STATE_META = {
    "pending": ("Ausstehend", "#E5E7EB", "#4B5563", 0),
    "running": ("Läuft", "#DBEAFE", "#1D4ED8", None),
    "done": ("Fertig", "#DCFCE7", "#166534", 100),
    "reused-target": ("Vorhanden", "#FEF3C7", "#92400E", 100),
    "skipped": ("Übersprungen", "#F3F4F6", "#6B7280", 100),
    "error": ("Fehler", "#FEE2E2", "#B91C1C", 0),
}


def _planned_job_steps(job: WorkflowJob) -> list[str]:
    has_merge = any(file.merge_group_id for file in job.files)
    has_output_stack = job.convert_enabled or has_merge or job.upload_youtube

    steps = ["transfer"]
    if job.convert_enabled:
        steps.append("convert")
    if has_merge:
        steps.append("merge")
    if has_output_stack and job.title_card_enabled:
        steps.append("titlecard")
    if has_output_stack and job.create_youtube_version:
        steps.append("yt_version")
    if has_output_stack and job.upload_youtube:
        steps.append("youtube_upload")
    if has_output_stack and job.upload_youtube and job.upload_kaderblick:
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
        ("YT-Version", "yt_version"),
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
    if raw in {"running", "done", "reused-target", "skipped", "error"}:
        return raw
    if step_key == _infer_current_step(job) and (job.resume_status or raw):
        if raw not in {"done", "reused-target", "skipped"}:
            return "running"
    return "pending"


def _step_progress(job: WorkflowJob, step_key: str, state: str) -> int:
    default_progress = _STATE_META[state][3]
    if default_progress is not None:
        return default_progress
    if step_key == _infer_current_step(job):
        return max(0, min(job.progress_pct, 100))
    return 0


def _workflow_editor_encoder_choices() -> list[tuple[str, str]]:
    """Cheap encoder list for the workflow editor.

    The full hardware probe can block noticeably because it shells out to ffmpeg
    and runs test encodes. The workflow editor should open immediately; encoder
    fallback is handled later during actual processing.
    """
    return [
        ("auto", "Automatisch (NVENC falls verfügbar)"),
        ("h264_nvenc", "NVIDIA NVENC (GPU)"),
        ("libx264", "libx264 (CPU)"),
    ]


class _StepNodeItem(QGraphicsRectItem):
    def __init__(self, step_key: str, graph: "_WorkflowGraphView"):
        super().__init__(0, 0, 220, 88)
        self.step_key = step_key
        self._graph = graph
        self.setFlags(
            QGraphicsItem.GraphicsItemFlag.ItemIsMovable
            | QGraphicsItem.GraphicsItemFlag.ItemSendsGeometryChanges
            | QGraphicsItem.GraphicsItemFlag.ItemIsSelectable
        )
        self.setPen(QPen(QColor("#CBD5E1"), 2))
        self._title = QGraphicsTextItem(self)
        self._title.setDefaultTextColor(QColor("#0F172A"))
        self._title.setPos(12, 8)
        self._detail = QGraphicsTextItem(self)
        self._detail.setDefaultTextColor(QColor("#475569"))
        self._detail.setPos(12, 34)
        self._state = QGraphicsTextItem(self)
        self._state.setDefaultTextColor(QColor("#1D4ED8"))
        self._state.setPos(12, 60)

    def update_from_job(self, job: WorkflowJob) -> None:
        lane = _STEP_LANES[self.step_key]
        state = _normalized_step_status(job, self.step_key)
        label, _bg, fg, _default_pct = _STATE_META[state]
        progress = _step_progress(job, self.step_key, state)
        base = QColor(_LANE_NODE_COLORS[lane])
        if state == "error":
            base = QColor("#FEE2E2")
        elif state in {"done", "reused-target", "skipped"}:
            base = QColor("#F8FAFC")
        self.setBrush(base)
        self._title.setPlainText(_STEP_LABELS[self.step_key])
        self._detail.setPlainText(_STEP_DETAILS[self.step_key])
        self._state.setDefaultTextColor(QColor(fg))
        self._state.setPlainText(f"{label} · {progress}%")

    def itemChange(self, change, value):
        if change == QGraphicsItem.GraphicsItemChange.ItemPositionHasChanged:
            self._graph.update_edges()
        return super().itemChange(change, value)


class _WorkflowGraphView(QGraphicsView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._scene = QGraphicsScene(self)
        self.setScene(self._scene)
        self.setRenderHint(QPainter.RenderHint.Antialiasing)
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setMinimumHeight(420)
        self.setStyleSheet("background: #F8FAFC; border: 1px solid #D7E0EA; border-radius: 12px;")
        self._nodes: dict[str, _StepNodeItem] = {}
        self._edges: list[QGraphicsPathItem] = []
        self._saved_positions: dict[str, QPointF] = {}
        self._planned: list[str] = []

    def set_job(self, job: WorkflowJob) -> None:
        for step_key, node in self._nodes.items():
            self._saved_positions[step_key] = node.pos()
        self._scene.clear()
        self._nodes = {}
        self._edges = []
        self._planned = _planned_job_steps(job)

        lane_y_offsets = {"transfer": 90, "processing": 90, "delivery": 90}
        lane_x = {"transfer": 30, "processing": 310, "delivery": 590}
        for lane_key, lane_label in _LANE_LABELS.items():
            title = QGraphicsTextItem(lane_label)
            title.setDefaultTextColor(QColor("#334155"))
            title.setPos(lane_x[lane_key], 18)
            self._scene.addItem(title)

        for step_key in self._planned:
            node = _StepNodeItem(step_key, self)
            lane = _STEP_LANES[step_key]
            pos = self._saved_positions.get(step_key, QPointF(lane_x[lane], lane_y_offsets[lane]))
            if step_key not in self._saved_positions:
                lane_y_offsets[lane] += 112
            node.setPos(pos)
            node.update_from_job(job)
            self._scene.addItem(node)
            self._nodes[step_key] = node

        self.update_edges()
        rect = self._scene.itemsBoundingRect().adjusted(-30, -30, 30, 30)
        self.setSceneRect(rect)

    def update_edges(self) -> None:
        for edge in self._edges:
            self._scene.removeItem(edge)
        self._edges = []
        for from_step, to_step in zip(self._planned, self._planned[1:]):
            from_node = self._nodes.get(from_step)
            to_node = self._nodes.get(to_step)
            if from_node is None or to_node is None:
                continue
            start = from_node.sceneBoundingRect().center()
            end = to_node.sceneBoundingRect().center()
            path = QPainterPath(start)
            mid_x = (start.x() + end.x()) / 2
            path.cubicTo(mid_x, start.y(), mid_x, end.y(), end.x(), end.y())
            edge = QGraphicsPathItem(path)
            edge.setPen(QPen(QColor("#94A3B8"), 3))
            edge.setZValue(-1)
            self._scene.addItem(edge)
            self._edges.append(edge)


class JobWorkflowDialog(QDialog):
    def __init__(self, parent, job: WorkflowJob, *, allow_edit: bool = False, settings: AppSettings | None = None):
        super().__init__(parent)
        self._job = job
        self._draft = copy.deepcopy(job)
        self._allow_edit = allow_edit
        self._settings = settings
        self._ui_ready = False
        self._edit_requested = False
        self._changed = False
        self._kb_api_loaded = False
        self._pi_list_worker: _CameraListWorker | None = None
        self.setWindowTitle(f"Workflow-Ansicht – {job.name or 'Auftrag'}")
        self.resize(920, 620)
        self.setMinimumSize(760, 480)
        self._build_ui()

    @property
    def edit_requested(self) -> bool:
        return self._edit_requested

    @property
    def changed(self) -> bool:
        return self._changed

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)

        self._header_box = self._build_header()
        root.addWidget(self._header_box)
        self._summary_box = self._build_summary_box()
        self._notes_box = self._build_notes_box()
        self._graph_box = self._build_graph_box()

        main_splitter = QSplitter(Qt.Orientation.Horizontal, self)
        main_splitter.addWidget(self._graph_box)

        inspector_content = QWidget(self)
        inspector_layout = QVBoxLayout(inspector_content)
        inspector_layout.setContentsMargins(0, 0, 0, 0)
        inspector_layout.setSpacing(10)
        inspector_layout.addWidget(self._summary_box)
        if self._allow_edit:
            inspector_layout.addWidget(self._build_editor_box())
        inspector_layout.addWidget(self._notes_box)
        inspector_layout.addStretch()

        inspector_scroll = QScrollArea(self)
        inspector_scroll.setWidgetResizable(True)
        inspector_scroll.setFrameShape(QFrame.Shape.NoFrame)
        inspector_scroll.setWidget(inspector_content)
        main_splitter.addWidget(inspector_scroll)
        main_splitter.setStretchFactor(0, 3)
        main_splitter.setStretchFactor(1, 2)
        root.addWidget(main_splitter, 1)

        if self._allow_edit:
            self._load_editor_from_job()
        self._ui_ready = True
        self._refresh_dynamic_sections()

        buttons = QDialogButtonBox(self)
        if self._allow_edit:
            edit_btn = QPushButton("Im Assistenten bearbeiten")
            edit_btn.clicked.connect(self._request_edit)
            buttons.addButton(edit_btn, QDialogButtonBox.ButtonRole.ActionRole)
            apply_btn = QPushButton("Übernehmen")
            apply_btn.clicked.connect(self._apply_and_accept)
            buttons.addButton(apply_btn, QDialogButtonBox.ButtonRole.AcceptRole)
            close_btn = buttons.addButton("Abbrechen", QDialogButtonBox.ButtonRole.RejectRole)
            close_btn.clicked.connect(self.reject)
        else:
            close_btn = buttons.addButton("Schließen", QDialogButtonBox.ButtonRole.AcceptRole)
            close_btn.clicked.connect(self.accept)
        root.addWidget(buttons)

    def _build_header(self) -> QWidget:
        box = QFrame(self)
        box.setStyleSheet(
            "QFrame { background: #F8FAFC; border: 1px solid #D7E0EA; border-radius: 12px; }"
        )
        layout = QVBoxLayout(box)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(6)

        self._title_label = QLabel(self._draft.name or "Unbenannter Auftrag")
        title = self._title_label
        title.setStyleSheet("font-size: 18px; font-weight: 700; color: #0F172A;")
        layout.addWidget(title)

        self._meta_label = QLabel(
            f"Quelle: {self._source_summary()}    |    Status: {self._draft.resume_status or 'Wartend'}"
        )
        meta = self._meta_label
        meta.setStyleSheet("color: #475569;")
        layout.addWidget(meta)

        self._pipeline_label = QLabel(
            "Ablauf: " + "  →  ".join(_STEP_LABELS[step] for step in _planned_job_steps(self._draft))
        )
        pipeline = self._pipeline_label
        pipeline.setStyleSheet("color: #334155; font-weight: 600;")
        layout.addWidget(pipeline)
        return box

    def _build_editor_box(self) -> QWidget:
        box = QGroupBox("Workflow-Inspector", self)
        box.setStyleSheet(
            "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
        )
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(12)

        name_row = QHBoxLayout()
        name_row.addWidget(QLabel("Jobname:"))
        self._name_edit = QLineEdit(self._draft.name)
        self._name_edit.setPlaceholderText("Anzeigename für den Workflow")
        self._name_edit.textChanged.connect(self._on_editor_changed)
        name_row.addWidget(self._name_edit, 1)
        layout.addLayout(name_row)

        grid = QGridLayout()
        grid.setHorizontalSpacing(16)
        grid.setVerticalSpacing(10)

        self._step_checkboxes: dict[str, QCheckBox] = {}
        row = 0
        for step_key, label in (
            ("convert", "Konvertierung aktivieren"),
            ("titlecard", "Titelkarte einblenden"),
            ("yt_version", "YouTube-Version erzeugen"),
            ("youtube_upload", "YouTube-Upload aktivieren"),
            ("kaderblick", "Kaderblick-Eintrag aktivieren"),
        ):
            checkbox = QCheckBox(label)
            checkbox.toggled.connect(lambda _checked, key=step_key: self._on_step_toggled(key))
            self._step_checkboxes[step_key] = checkbox
            grid.addWidget(checkbox, row, 0)

            hint = QLabel(_STEP_DETAILS[step_key])
            hint.setWordWrap(True)
            hint.setStyleSheet("color: #64748B;")
            grid.addWidget(hint, row, 1)
            row += 1

        self._merge_label = QLabel()
        self._merge_label.setWordWrap(True)
        self._merge_label.setStyleSheet("color: #92400E; font-weight: 600;")

        self._editor_hint = QLabel()
        self._editor_hint.setWordWrap(True)
        self._editor_hint.setStyleSheet("color: #475569;")

        tabs = QTabWidget(self)

        workflow_tab = QWidget(self)
        workflow_layout = QVBoxLayout(workflow_tab)
        workflow_layout.setContentsMargins(0, 0, 0, 0)
        workflow_layout.setSpacing(10)
        workflow_layout.addLayout(grid)
        workflow_layout.addWidget(self._merge_label)
        workflow_layout.addWidget(self._editor_hint)
        workflow_layout.addStretch()

        tabs.addTab(workflow_tab, "Ablauf")
        tabs.addTab(self._build_source_box(), "Quelle")
        tabs.addTab(self._build_processing_box(), "Verarbeitung")
        tabs.addTab(self._build_step_options_box(), "Upload & Titel")
        layout.addWidget(tabs)

        return box

    def _build_graph_box(self) -> QWidget:
        box = QGroupBox("Workflow-Graph", self)
        box.setStyleSheet(
            "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
        )
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)
        hint = QLabel("Graphische Ansicht des Job-Ablaufs. Knoten lassen sich zur besseren Übersicht frei ziehen; die Verbindungen werden automatisch aktualisiert.")
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #475569;")
        layout.addWidget(hint)
        self._graph_view = _WorkflowGraphView(self)
        layout.addWidget(self._graph_view, 1)
        return box

    def _build_step_options_box(self) -> QWidget:
        box = QGroupBox("Step-Optionen", self)
        box.setStyleSheet(
            "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
        )
        form = QFormLayout(box)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)

        self._overwrite_cb = QCheckBox("Vorhandene Ergebnisse überschreiben")
        self._overwrite_cb.toggled.connect(lambda checked: self._update_bool_field("overwrite", checked))
        form.addRow("Allgemein:", self._overwrite_cb)

        self._yt_title_edit = QLineEdit()
        self._yt_title_edit.setPlaceholderText("leer = Dateiname")
        self._yt_title_edit.textChanged.connect(lambda text: self._update_text_field("default_youtube_title", text))
        form.addRow("Standard-YT-Titel:", self._yt_title_edit)

        self._yt_playlist_edit = QLineEdit()
        self._yt_playlist_edit.setPlaceholderText("leer = keine Playlist")
        self._yt_playlist_edit.textChanged.connect(lambda text: self._update_text_field("default_youtube_playlist", text))
        playlist_row = QHBoxLayout()
        playlist_row.addWidget(self._yt_playlist_edit, 1)
        self._playlist_helper_btn = QPushButton("🎬 Spieldaten …")
        self._playlist_helper_btn.clicked.connect(self._open_match_editor_for_playlist)
        playlist_row.addWidget(self._playlist_helper_btn)
        form.addRow("YouTube-Playlist:", playlist_row)

        self._kb_game_id_edit = QLineEdit()
        self._kb_game_id_edit.setPlaceholderText("z. B. 42")
        self._kb_game_id_edit.textChanged.connect(lambda text: self._update_text_field("default_kaderblick_game_id", text))
        form.addRow("Kaderblick Spiel-ID:", self._kb_game_id_edit)

        self._tc_home_edit = QLineEdit()
        self._tc_home_edit.textChanged.connect(lambda text: self._update_text_field("title_card_home_team", text))
        form.addRow("Titelkarte Heim:", self._tc_home_edit)

        self._tc_away_edit = QLineEdit()
        self._tc_away_edit.textChanged.connect(lambda text: self._update_text_field("title_card_away_team", text))
        form.addRow("Titelkarte Gast:", self._tc_away_edit)

        self._tc_date_edit = QLineEdit()
        self._tc_date_edit.textChanged.connect(lambda text: self._update_text_field("title_card_date", text))
        form.addRow("Titelkarte Datum:", self._tc_date_edit)

        self._tc_duration_spin = QDoubleSpinBox()
        self._tc_duration_spin.setRange(0.5, 10.0)
        self._tc_duration_spin.setSingleStep(0.5)
        self._tc_duration_spin.setSuffix(" s")
        self._tc_duration_spin.valueChanged.connect(lambda value: self._update_float_field("title_card_duration", value))
        form.addRow("Titelkarte Dauer:", self._tc_duration_spin)

        self._crf_spin = QSpinBox()
        self._crf_spin.setRange(0, 51)
        self._crf_spin.valueChanged.connect(lambda value: self._update_int_field("crf", value))
        form.addRow("CRF:", self._crf_spin)

        self._yt_competition_edit = QLineEdit()
        self._yt_competition_edit.setPlaceholderText("z. B. Sparkassenpokal")
        self._yt_competition_edit.textChanged.connect(lambda text: self._update_text_field("default_youtube_competition", text))
        form.addRow("Wettbewerb:", self._yt_competition_edit)

        self._kb_type_spin = QSpinBox()
        self._kb_type_spin.setRange(0, 9999)
        self._kb_type_spin.valueChanged.connect(lambda value: self._update_int_field("default_kaderblick_video_type_id", value))
        form.addRow("KB Video-Typ-ID:", self._kb_type_spin)

        self._kb_camera_spin = QSpinBox()
        self._kb_camera_spin.setRange(0, 9999)
        self._kb_camera_spin.valueChanged.connect(lambda value: self._update_int_field("default_kaderblick_camera_id", value))
        form.addRow("KB Kamera-ID:", self._kb_camera_spin)

        kb_row = QHBoxLayout()
        self._kb_reload_btn = QPushButton("↺ Typen & Kameras laden")
        self._kb_reload_btn.clicked.connect(lambda: self._kb_load_api_data(force=True))
        kb_row.addWidget(self._kb_reload_btn)
        kb_row.addStretch()
        form.addRow("Kaderblick:", kb_row)

        self._kb_status_label = QLabel("")
        self._kb_status_label.setWordWrap(True)
        self._kb_status_label.setStyleSheet("color: #64748B;")
        form.addRow("", self._kb_status_label)

        self._tc_logo_edit = QLineEdit()
        self._tc_logo_edit.setPlaceholderText("Pfad zum Logo-Bild")
        self._tc_logo_edit.textChanged.connect(lambda text: self._update_text_field("title_card_logo_path", text))
        form.addRow("Titelkarte Logo:", self._tc_logo_edit)

        self._tc_bg_edit = QLineEdit()
        self._tc_bg_edit.setPlaceholderText("#000000")
        self._tc_bg_edit.textChanged.connect(lambda text: self._update_text_field("title_card_bg_color", text))
        form.addRow("Titelkarte BG:", self._tc_bg_edit)

        self._tc_fg_edit = QLineEdit()
        self._tc_fg_edit.setPlaceholderText("#FFFFFF")
        self._tc_fg_edit.textChanged.connect(lambda text: self._update_text_field("title_card_fg_color", text))
        form.addRow("Titelkarte FG:", self._tc_fg_edit)
        return box

    def _build_processing_box(self) -> QWidget:
        box = QGroupBox("Verarbeitung und Audio", self)
        box.setStyleSheet(
            "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
        )
        form = QFormLayout(box)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)

        self._encoder_combo = QComboBox()
        for enc_id, enc_name in _workflow_editor_encoder_choices():
            self._encoder_combo.addItem(enc_name, enc_id)
        self._encoder_combo.currentIndexChanged.connect(self._on_encoder_changed)
        form.addRow("Encoder:", self._encoder_combo)

        self._preset_combo = QComboBox()
        self._preset_combo.addItems([
            "ultrafast", "superfast", "veryfast", "faster", "fast",
            "medium", "slow", "slower", "veryslow",
        ])
        self._preset_combo.currentTextChanged.connect(lambda text: self._update_text_field("preset", text))
        form.addRow("Preset:", self._preset_combo)

        self._fps_spin = QSpinBox()
        self._fps_spin.setRange(1, 120)
        self._fps_spin.valueChanged.connect(lambda value: self._update_int_field("fps", value))
        form.addRow("Framerate:", self._fps_spin)

        self._format_combo = QComboBox()
        self._format_combo.addItems(["mp4", "avi"])
        self._format_combo.currentTextChanged.connect(lambda text: self._update_text_field("output_format", text))
        form.addRow("Format:", self._format_combo)

        self._merge_audio_cb = QCheckBox("Separate Audio-Spur zusammenführen")
        self._merge_audio_cb.toggled.connect(lambda checked: self._update_bool_field("merge_audio", checked))
        form.addRow("Audio:", self._merge_audio_cb)

        amp_row = QHBoxLayout()
        self._amplify_audio_cb = QCheckBox("Lautstärke anpassen")
        self._amplify_audio_cb.toggled.connect(self._on_amplify_toggled)
        amp_row.addWidget(self._amplify_audio_cb)
        self._amplify_db_spin = QDoubleSpinBox()
        self._amplify_db_spin.setRange(-20.0, 40.0)
        self._amplify_db_spin.setSingleStep(1.0)
        self._amplify_db_spin.setDecimals(1)
        self._amplify_db_spin.setSuffix(" dB")
        self._amplify_db_spin.valueChanged.connect(lambda value: self._update_float_field("amplify_db", value))
        amp_row.addWidget(self._amplify_db_spin)
        amp_row.addStretch()
        form.addRow("Pegel:", amp_row)

        self._audio_sync_cb = QCheckBox("Audio-Sync / Frame-Drop-Korrektur")
        self._audio_sync_cb.toggled.connect(lambda checked: self._update_bool_field("audio_sync", checked))
        form.addRow("", self._audio_sync_cb)
        return box

    def _build_source_box(self) -> QWidget:
        box = QGroupBox("Quelle", self)
        box.setStyleSheet(
            "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
        )
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)

        self._source_mode_label = QLabel()
        self._source_mode_label.setStyleSheet("color: #475569; font-weight: 600;")
        layout.addWidget(self._source_mode_label)

        self._source_detail_label = QLabel()
        self._source_detail_label.setWordWrap(True)
        self._source_detail_label.setStyleSheet("color: #64748B;")
        layout.addWidget(self._source_detail_label)

        self._source_fields = QWidget(self)
        fields_layout = QVBoxLayout(self._source_fields)
        fields_layout.setContentsMargins(0, 0, 0, 0)
        fields_layout.setSpacing(8)

        self._file_list_widget = None
        self._source_mode_widgets: dict[str, QWidget] = {}

        self._source_mode_widgets["files"] = self._build_files_source_editor()
        self._source_mode_widgets["folder_scan"] = self._build_folder_source_editor()
        self._source_mode_widgets["pi_download"] = self._build_pi_source_editor()
        for widget in self._source_mode_widgets.values():
            fields_layout.addWidget(widget)

        layout.addWidget(self._source_fields)
        self._set_source_editor_visibility()
        return box

    def _build_files_source_editor(self) -> QWidget:
        wrapper = QWidget(self)
        layout = QVBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        if self._settings is not None:
            self._file_list_widget = FileListWidget(
                last_dir_getter=lambda: self._settings.last_directory,
                last_dir_setter=self._save_last_dir,
            )
            self._file_list_widget.match_data_changed.connect(self._on_match_data_changed)
            self._file_list_widget.files_changed.connect(self._on_files_changed)
            self._file_list_widget.load(self._draft.files)
            layout.addWidget(self._file_list_widget)
        else:
            hint = QLabel("Ohne geladene Einstellungen ist die Dateiliste hier nicht editierbar.")
            hint.setWordWrap(True)
            hint.setStyleSheet("color: #92400E;")
            layout.addWidget(hint)

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(8)

        self._files_dst_edit = QLineEdit()
        self._files_dst_edit.setPlaceholderText("leer = Dateien am Quellort verarbeiten")
        self._files_dst_edit.textChanged.connect(lambda text: self._update_text_field("copy_destination", text))
        form.addRow("Zielordner:", self._files_dst_edit)

        self._files_move_cb = QCheckBox("Quelldateien in Zielordner verschieben")
        self._files_move_cb.toggled.connect(lambda checked: self._update_bool_field("move_files", checked))
        form.addRow("", self._files_move_cb)
        layout.addLayout(form)
        return wrapper

    def _build_folder_source_editor(self) -> QWidget:
        wrapper = QWidget(self)
        form = QFormLayout(wrapper)
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(8)

        self._folder_src_edit = QLineEdit()
        self._folder_src_edit.setPlaceholderText("Quellordner")
        self._folder_src_edit.textChanged.connect(lambda text: self._update_text_field("source_folder", text))
        form.addRow("Quellordner:", self._folder_src_edit)

        self._file_pattern_edit = QLineEdit()
        self._file_pattern_edit.setPlaceholderText("*.mp4")
        self._file_pattern_edit.textChanged.connect(self._on_file_pattern_changed)
        form.addRow("Datei-Muster:", self._file_pattern_edit)

        self._folder_dst_edit = QLineEdit()
        self._folder_dst_edit.setPlaceholderText("leer = neben der Quelldatei")
        self._folder_dst_edit.textChanged.connect(lambda text: self._update_text_field("copy_destination", text))
        form.addRow("Zielordner:", self._folder_dst_edit)

        self._move_files_cb = QCheckBox("Quelldateien nach Verarbeitung verschieben")
        self._move_files_cb.toggled.connect(lambda checked: self._update_bool_field("move_files", checked))
        form.addRow("", self._move_files_cb)

        self._folder_prefix_edit = QLineEdit()
        self._folder_prefix_edit.setPlaceholderText("optional")
        self._folder_prefix_edit.textChanged.connect(lambda text: self._update_text_field("output_prefix", text))
        form.addRow("Ausgabe-Präfix:", self._folder_prefix_edit)
        return wrapper

    def _build_pi_source_editor(self) -> QWidget:
        wrapper = QWidget(self)
        form = QFormLayout(wrapper)
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(8)

        self._device_combo = QComboBox()
        self._device_combo.addItem("(Gerät wählen)", "")
        if self._settings is not None:
            for dev in self._settings.cameras.devices:
                self._device_combo.addItem(f"{dev.name}  ({dev.ip})", dev.name)
        self._device_combo.currentIndexChanged.connect(self._on_device_changed)
        form.addRow("Gerät:", self._device_combo)

        self._pi_dest_edit = QLineEdit()
        self._pi_dest_edit.setPlaceholderText("Lokales Zielverzeichnis")
        self._pi_dest_edit.textChanged.connect(lambda text: self._update_text_field("download_destination", text))
        form.addRow("Zielverzeichnis:", self._pi_dest_edit)

        self._delete_after_dl_cb = QCheckBox("Aufnahmen nach Download löschen")
        self._delete_after_dl_cb.toggled.connect(lambda checked: self._update_bool_field("delete_after_download", checked))
        form.addRow("", self._delete_after_dl_cb)

        self._pi_prefix_edit = QLineEdit()
        self._pi_prefix_edit.setPlaceholderText("optional")
        self._pi_prefix_edit.textChanged.connect(lambda text: self._update_text_field("output_prefix", text))
        form.addRow("Ausgabe-Präfix:", self._pi_prefix_edit)

        load_row = QHBoxLayout()
        self._pi_load_btn = QPushButton("📋 Dateien von Kamera laden")
        self._pi_load_btn.clicked.connect(self._load_pi_camera_files)
        load_row.addWidget(self._pi_load_btn)
        self._pi_load_status = QLabel("")
        self._pi_load_status.setStyleSheet("color: #64748B;")
        load_row.addWidget(self._pi_load_status, 1)
        form.addRow("", load_row)

        self._pi_file_list = FileListWidget(
            last_dir_getter=lambda: self._settings.last_directory if self._settings is not None else "",
            last_dir_setter=lambda _directory: None,
        )
        self._pi_file_list.match_data_changed.connect(self._on_match_data_changed)
        self._pi_file_list.files_changed.connect(self._on_pi_files_changed)
        self._pi_file_list.setVisible(False)
        form.addRow("Auswahl:", self._pi_file_list)
        return wrapper

    def _build_summary_box(self) -> QWidget:
        box = QFrame(self)
        box.setStyleSheet(
            "QFrame { background: #FFFFFF; border: 1px solid #D7E0EA; border-radius: 12px; }"
        )
        layout = QVBoxLayout(box)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(8)

        self._overall_label = QLabel(f"Gesamtfortschritt: {self._draft.overall_progress_pct}%")
        overall = self._overall_label
        overall.setStyleSheet("font-weight: 600; color: #0F172A;")
        layout.addWidget(overall)

        self._overall_bar = QProgressBar(self)
        bar = self._overall_bar
        bar.setRange(0, 100)
        bar.setValue(max(0, min(self._draft.overall_progress_pct, 100)))
        bar.setFormat("%p%")
        layout.addWidget(bar)

        self._current_label = QLabel(f"Aktiver Step: {_STEP_LABELS.get(_infer_current_step(self._draft), 'Transfer')}")
        current = self._current_label
        current.setStyleSheet("color: #475569;")
        layout.addWidget(current)
        return box

    def _build_lane_area(self) -> QWidget:
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        content = QWidget(scroll)
        layout = QVBoxLayout(content)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        self._lane_content = QWidget(scroll)
        planned = _planned_job_steps(self._draft)
        for lane in ("transfer", "processing", "delivery"):
            lane_steps = [step for step in planned if _STEP_LANES.get(step) == lane]
            if not lane_steps:
                continue
            layout.addWidget(self._build_lane_box(lane, lane_steps))
        layout.addStretch()
        scroll.setWidget(content)
        return scroll

    def _build_lane_box(self, lane_key: str, steps: list[str]) -> QWidget:
        box = QGroupBox(_LANE_LABELS[lane_key], self)
        box.setStyleSheet(
            "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
        )
        layout = QHBoxLayout(box)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)

        for idx, step_key in enumerate(steps):
            layout.addWidget(self._build_step_card(step_key), 1)
            if idx < len(steps) - 1:
                arrow = QLabel("→")
                arrow.setAlignment(Qt.AlignmentFlag.AlignCenter)
                arrow.setStyleSheet("font-size: 20px; color: #94A3B8; font-weight: 700;")
                layout.addWidget(arrow)
        return box

    def _build_step_card(self, step_key: str) -> QWidget:
        state = _normalized_step_status(self._draft, step_key)
        label, bg, fg, _default_pct = _STATE_META[state]
        progress = _step_progress(self._draft, step_key, state)

        card = QFrame(self)
        card.setStyleSheet(
            "QFrame { background: #FFFFFF; border: 1px solid #D7E0EA; border-radius: 10px; }"
        )
        layout = QVBoxLayout(card)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(6)

        top = QHBoxLayout()
        title = QLabel(_STEP_LABELS[step_key])
        title.setStyleSheet("font-weight: 700; color: #0F172A;")
        top.addWidget(title)
        top.addStretch()
        badge = QLabel(label)
        badge.setStyleSheet(
            f"background: {bg}; color: {fg}; border-radius: 9px; padding: 2px 8px; font-weight: 700;"
        )
        top.addWidget(badge)
        layout.addLayout(top)

        detail = QLabel(_STEP_DETAILS[step_key])
        detail.setWordWrap(True)
        detail.setStyleSheet("color: #475569;")
        layout.addWidget(detail)

        bar = QProgressBar(self)
        bar.setRange(0, 100)
        bar.setValue(progress)
        bar.setFormat(f"{progress}%")
        layout.addWidget(bar)

        status = self._draft.step_statuses.get(step_key, "") if isinstance(self._draft.step_statuses, dict) else ""
        status_text = status if status else (self._draft.resume_status if step_key == _infer_current_step(self._draft) else "Noch nicht ausgeführt")
        info = QLabel(status_text)
        info.setWordWrap(True)
        info.setStyleSheet("color: #64748B; font-size: 11px;")
        layout.addWidget(info)
        return card

    def _build_notes_box(self) -> QWidget:
        box = QFrame(self)
        box.setStyleSheet(
            "QFrame { background: #FFFDF4; border: 1px solid #F3E7B3; border-radius: 12px; }"
        )
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(4)

        title = QLabel("Hinweise zur Ausführung")
        title.setStyleSheet("font-weight: 700; color: #7C4A03;")
        layout.addWidget(title)

        notes = []
        if any(file.merge_group_id for file in self._draft.files):
            notes.append("Merge-Barriere aktiv: Das Zusammenführen startet erst, wenn alle Gruppenmitglieder bereit sind.")
        else:
            notes.append("Standalone-Pfad: Fertige Dateien können direkt nach dem Transfer in die Verarbeitung laufen.")
        if self._draft.upload_youtube or self._draft.upload_kaderblick:
            notes.append("Upload-Lane separat: Uploads können parallel zur nächsten Konvertierung laufen.")
        if self._draft.title_card_enabled or self._draft.create_youtube_version:
            notes.append("FFmpeg-Schritte liegen gemeinsam auf der Verarbeitungs-Lane und teilen sich dieselbe GPU-/Encoder-Ressource.")

        for note in notes:
            lbl = QLabel("• " + note)
            lbl.setWordWrap(True)
            lbl.setStyleSheet("color: #92400E;")
            layout.addWidget(lbl)
        return box

    def _source_summary(self) -> str:
        if self._draft.source_mode == "files":
            return f"{len(self._draft.files)} Datei(en)"
        if self._draft.source_mode == "folder_scan":
            return Path(self._draft.source_folder).name if self._draft.source_folder else "Ordner"
        if self._draft.source_mode == "pi_download":
            return self._draft.device_name or "Pi-Kamera"
        return "Quelle"

    def _request_edit(self) -> None:
        self._edit_requested = True
        self.accept()

    def _load_editor_from_job(self) -> None:
        self._name_edit.setText(self._draft.name)
        self._step_checkboxes["convert"].setChecked(self._draft.convert_enabled)
        self._step_checkboxes["titlecard"].setChecked(self._draft.title_card_enabled)
        self._step_checkboxes["yt_version"].setChecked(self._draft.create_youtube_version)
        self._step_checkboxes["youtube_upload"].setChecked(self._draft.upload_youtube)
        self._step_checkboxes["kaderblick"].setChecked(self._draft.upload_kaderblick)
        self._overwrite_cb.setChecked(self._draft.overwrite)
        self._yt_title_edit.setText(self._draft.default_youtube_title)
        self._yt_playlist_edit.setText(self._draft.default_youtube_playlist)
        self._kb_game_id_edit.setText(self._draft.default_kaderblick_game_id)
        self._tc_home_edit.setText(self._draft.title_card_home_team)
        self._tc_away_edit.setText(self._draft.title_card_away_team)
        self._tc_date_edit.setText(self._draft.title_card_date)
        self._tc_duration_spin.setValue(self._draft.title_card_duration)
        self._crf_spin.setValue(self._draft.crf)
        self._yt_competition_edit.setText(self._draft.default_youtube_competition)
        self._kb_type_spin.setValue(self._draft.default_kaderblick_video_type_id)
        self._kb_camera_spin.setValue(self._draft.default_kaderblick_camera_id)
        self._tc_logo_edit.setText(self._draft.title_card_logo_path)
        self._tc_bg_edit.setText(self._draft.title_card_bg_color or "#000000")
        self._tc_fg_edit.setText(self._draft.title_card_fg_color or "#FFFFFF")
        encoder_index = self._encoder_combo.findData(self._draft.encoder)
        self._encoder_combo.setCurrentIndex(encoder_index if encoder_index >= 0 else 0)
        self._preset_combo.setCurrentText(self._draft.preset)
        self._fps_spin.setValue(self._draft.fps)
        self._format_combo.setCurrentText(self._draft.output_format)
        self._merge_audio_cb.setChecked(self._draft.merge_audio)
        self._amplify_audio_cb.setChecked(self._draft.amplify_audio)
        self._amplify_db_spin.setValue(self._draft.amplify_db)
        self._audio_sync_cb.setChecked(self._draft.audio_sync)
        self._load_source_editor_from_job()
        self._sync_editor_state()

    def _load_source_editor_from_job(self) -> None:
        if hasattr(self, "_files_dst_edit"):
            self._files_dst_edit.setText(self._draft.copy_destination)
            self._files_move_cb.setChecked(self._draft.move_files)
        if hasattr(self, "_folder_src_edit"):
            self._folder_src_edit.setText(self._draft.source_folder)
            self._file_pattern_edit.setText(self._draft.file_pattern or "*.mp4")
            self._folder_dst_edit.setText(self._draft.copy_destination)
            self._move_files_cb.setChecked(self._draft.move_files)
            self._folder_prefix_edit.setText(self._draft.output_prefix)
        if hasattr(self, "_device_combo"):
            device_index = self._device_combo.findData(self._draft.device_name)
            self._device_combo.setCurrentIndex(device_index if device_index >= 0 else 0)
            default_dest = self._settings.cameras.destination if self._settings is not None else ""
            self._pi_dest_edit.setText(self._draft.download_destination or default_dest)
            self._delete_after_dl_cb.setChecked(self._draft.delete_after_download)
            self._pi_prefix_edit.setText(self._draft.output_prefix)
            if self._draft.source_mode == "pi_download" and self._draft.files:
                self._pi_file_list.load(self._draft.files)
                self._pi_file_list.setVisible(True)
                self._pi_load_status.setText(f"✓ {len(self._draft.files)} Aufnahme(n) vorgemerkt.")
                self._pi_load_status.setStyleSheet("color: green;")

    def _on_step_toggled(self, step_key: str) -> None:
        if step_key == "convert":
            self._draft.convert_enabled = self._step_checkboxes[step_key].isChecked()
        elif step_key == "titlecard":
            self._draft.title_card_enabled = self._step_checkboxes[step_key].isChecked()
        elif step_key == "yt_version":
            self._draft.create_youtube_version = self._step_checkboxes[step_key].isChecked()
        elif step_key == "youtube_upload":
            self._draft.upload_youtube = self._step_checkboxes[step_key].isChecked()
        elif step_key == "kaderblick":
            self._draft.upload_kaderblick = self._step_checkboxes[step_key].isChecked()
        self._sync_editor_state(triggered_step=step_key)

    def _on_editor_changed(self, text: str) -> None:
        self._draft.name = text.strip()
        self._refresh_dynamic_sections()

    def _sync_editor_state(self, *, triggered_step: str | None = None) -> None:
        has_merge = any(file.merge_group_id for file in self._draft.files)
        has_output_stack = self._draft.convert_enabled or has_merge or self._draft.upload_youtube

        if not self._draft.upload_youtube:
            self._draft.upload_kaderblick = False
        if not has_output_stack:
            self._draft.title_card_enabled = False
            self._draft.create_youtube_version = False

        self._step_checkboxes["convert"].setChecked(self._draft.convert_enabled)
        self._step_checkboxes["titlecard"].setChecked(self._draft.title_card_enabled)
        self._step_checkboxes["yt_version"].setChecked(self._draft.create_youtube_version)
        self._step_checkboxes["youtube_upload"].setChecked(self._draft.upload_youtube)
        self._step_checkboxes["kaderblick"].setChecked(self._draft.upload_kaderblick)

        self._step_checkboxes["titlecard"].setEnabled(has_output_stack)
        self._step_checkboxes["yt_version"].setEnabled(has_output_stack)
        self._step_checkboxes["kaderblick"].setEnabled(self._draft.upload_youtube)
        self._yt_title_edit.setEnabled(self._draft.upload_youtube)
        self._yt_playlist_edit.setEnabled(self._draft.upload_youtube)
        self._yt_competition_edit.setEnabled(self._draft.upload_youtube)
        self._playlist_helper_btn.setEnabled(self._draft.upload_youtube)
        self._kb_game_id_edit.setEnabled(self._draft.upload_youtube and self._draft.upload_kaderblick)
        self._kb_type_spin.setEnabled(self._draft.upload_youtube and self._draft.upload_kaderblick)
        self._kb_camera_spin.setEnabled(self._draft.upload_youtube and self._draft.upload_kaderblick)
        self._kb_reload_btn.setEnabled(self._draft.upload_youtube and self._draft.upload_kaderblick)
        self._kb_status_label.setEnabled(self._draft.upload_youtube and self._draft.upload_kaderblick)
        titlecard_enabled = self._draft.title_card_enabled and has_output_stack
        self._tc_home_edit.setEnabled(titlecard_enabled)
        self._tc_away_edit.setEnabled(titlecard_enabled)
        self._tc_date_edit.setEnabled(titlecard_enabled)
        self._tc_duration_spin.setEnabled(titlecard_enabled)
        self._tc_logo_edit.setEnabled(titlecard_enabled)
        self._tc_bg_edit.setEnabled(titlecard_enabled)
        self._tc_fg_edit.setEnabled(titlecard_enabled)
        self._amplify_db_spin.setEnabled(self._draft.amplify_audio)

        merge_count = len({file.merge_group_id for file in self._draft.files if file.merge_group_id})
        if merge_count:
            self._merge_label.setText(
                f"Merge ist aktiv: {merge_count} Gruppe(n) kommen aus der Dateiliste dieses Editors."
            )
        else:
            self._merge_label.setText(
                "Kein Merge aktiv. Für kombinierte Videos legst du Merge-Gruppen direkt in der Dateiliste dieses Editors an."
            )

        hints = []
        if not has_output_stack:
            hints.append("Titelkarte und YT-Version sind erst sinnvoll, wenn Konvertierung, Upload oder Merge aktiv ist.")
        if self._draft.upload_youtube:
            hints.append("YouTube-Upload erzeugt eine Delivery-Lane und erlaubt optional den Kaderblick-Schritt.")
        if triggered_step == "youtube_upload" and self._draft.upload_youtube:
            hints.append("Upload aktiviert: Du kannst jetzt optional Kaderblick zuschalten.")
        self._editor_hint.setText(" ".join(hints) if hints else "Der Workflow-Editor arbeitet auf denselben Jobdaten wie der Assistent.")

        self._set_source_editor_visibility()
        self._refresh_dynamic_sections()

    def _refresh_dynamic_sections(self) -> None:
        if not self._ui_ready:
            return
        self._title_label.setText(self._draft.name or "Unbenannter Auftrag")
        self._meta_label.setText(
            f"Quelle: {self._source_summary()}    |    Status: {self._draft.resume_status or 'Wartend'}"
        )
        self._pipeline_label.setText(
            "Ablauf: " + "  →  ".join(_STEP_LABELS[step] for step in _planned_job_steps(self._draft))
        )
        self._overall_label.setText(f"Gesamtfortschritt: {self._draft.overall_progress_pct}%")
        self._overall_bar.setValue(max(0, min(self._draft.overall_progress_pct, 100)))
        self._current_label.setText(f"Aktiver Step: {_STEP_LABELS.get(_infer_current_step(self._draft), 'Transfer')}")

        self._graph_view.set_job(self._draft)

        notes_layout = self._notes_box.layout()
        while notes_layout.count():
            item = notes_layout.takeAt(0)
            child = item.widget()
            if child is not None:
                child.deleteLater()
        title = QLabel("Hinweise zur Ausführung")
        title.setStyleSheet("font-weight: 700; color: #7C4A03;")
        notes_layout.addWidget(title)
        notes = []
        if any(file.merge_group_id for file in self._draft.files):
            notes.append("Merge-Barriere aktiv: Das Zusammenführen startet erst, wenn alle Gruppenmitglieder bereit sind.")
        else:
            notes.append("Standalone-Pfad: Fertige Dateien können direkt nach dem Transfer in die Verarbeitung laufen.")
        if self._draft.upload_youtube or self._draft.upload_kaderblick:
            notes.append("Upload-Lane separat: Uploads können parallel zur nächsten Konvertierung laufen.")
        if self._draft.title_card_enabled or self._draft.create_youtube_version:
            notes.append("FFmpeg-Schritte liegen gemeinsam auf der Verarbeitungs-Lane und teilen sich dieselbe GPU-/Encoder-Ressource.")
        for note in notes:
            lbl = QLabel("• " + note)
            lbl.setWordWrap(True)
            lbl.setStyleSheet("color: #92400E;")
            notes_layout.addWidget(lbl)

        if hasattr(self, "_source_mode_label"):
            source_labels = {
                "files": "Direkte Dateiauswahl",
                "folder_scan": "Ordner-Scan",
                "pi_download": "Pi-Kamera-Download",
            }
            self._source_mode_label.setText(f"Quellmodus: {source_labels.get(self._draft.source_mode, self._draft.source_mode)}")
            if self._draft.source_mode == "files":
                merge_count = len({file.merge_group_id for file in self._draft.files if file.merge_group_id})
                self._source_detail_label.setText(
                    f"{len(self._draft.files)} Datei(en) im Job, {merge_count} Merge-Gruppe(n). Änderungen an Dateiliste und Merge-Gruppen wirken direkt auf den Workflowplan."
                )
            elif self._draft.source_mode == "folder_scan":
                self._source_detail_label.setText(
                    f"Ordner: {self._draft.source_folder or '–'} | Muster: {self._draft.file_pattern or '*.mp4'}"
                )
            elif self._draft.source_mode == "pi_download":
                file_count = len(self._draft.files)
                self._source_detail_label.setText(
                    f"Gerät: {self._draft.device_name or '–'} | Ziel: {self._draft.download_destination or '–'} | Auswahl: {file_count} Datei(en)"
                )

    def _apply_and_accept(self) -> None:
        self._draft.name = self._name_edit.text().strip()
        if self._draft.source_mode == "files" and self._file_list_widget is not None:
            self._draft.files = self._file_list_widget.collect()
        elif self._draft.source_mode == "pi_download" and hasattr(self, "_pi_file_list"):
            self._draft.files = self._pi_file_list.collect() if not self._pi_file_list.is_empty() else []
        self._job.name = self._draft.name
        self._job.source_mode = self._draft.source_mode
        self._job.copy_destination = self._draft.copy_destination
        self._job.move_files = self._draft.move_files
        self._job.source_folder = self._draft.source_folder
        self._job.file_pattern = self._draft.file_pattern
        self._job.output_prefix = self._draft.output_prefix
        self._job.device_name = self._draft.device_name
        self._job.download_destination = self._draft.download_destination
        self._job.delete_after_download = self._draft.delete_after_download
        self._job.convert_enabled = self._draft.convert_enabled
        self._job.title_card_enabled = self._draft.title_card_enabled
        self._job.create_youtube_version = self._draft.create_youtube_version
        self._job.upload_youtube = self._draft.upload_youtube
        self._job.upload_kaderblick = self._draft.upload_kaderblick
        self._job.overwrite = self._draft.overwrite
        self._job.default_youtube_title = self._draft.default_youtube_title
        self._job.default_youtube_playlist = self._draft.default_youtube_playlist
        self._job.default_youtube_competition = self._draft.default_youtube_competition
        self._job.default_kaderblick_game_id = self._draft.default_kaderblick_game_id
        self._job.default_kaderblick_video_type_id = self._draft.default_kaderblick_video_type_id
        self._job.default_kaderblick_camera_id = self._draft.default_kaderblick_camera_id
        self._job.encoder = self._draft.encoder
        self._job.preset = self._draft.preset
        self._job.title_card_home_team = self._draft.title_card_home_team
        self._job.title_card_away_team = self._draft.title_card_away_team
        self._job.title_card_date = self._draft.title_card_date
        self._job.title_card_duration = self._draft.title_card_duration
        self._job.title_card_logo_path = self._draft.title_card_logo_path
        self._job.title_card_bg_color = self._draft.title_card_bg_color
        self._job.title_card_fg_color = self._draft.title_card_fg_color
        self._job.crf = self._draft.crf
        self._job.fps = self._draft.fps
        self._job.output_format = self._draft.output_format
        self._job.merge_audio = self._draft.merge_audio
        self._job.amplify_audio = self._draft.amplify_audio
        self._job.amplify_db = self._draft.amplify_db
        self._job.audio_sync = self._draft.audio_sync
        self._job.files = copy.deepcopy(self._draft.files)
        self._changed = True
        self.accept()

    def _update_bool_field(self, attr: str, value: bool) -> None:
        setattr(self._draft, attr, value)
        self._sync_editor_state()

    def _update_text_field(self, attr: str, value: str) -> None:
        setattr(self._draft, attr, value.strip())
        self._refresh_dynamic_sections()

    def _update_float_field(self, attr: str, value: float) -> None:
        setattr(self._draft, attr, float(value))
        self._refresh_dynamic_sections()

    def _update_int_field(self, attr: str, value: int) -> None:
        setattr(self._draft, attr, int(value))
        self._refresh_dynamic_sections()

    def _on_match_data_changed(self, home: str, away: str, date_iso: str) -> None:
        if home:
            self._draft.title_card_home_team = home
            self._tc_home_edit.setText(home)
        if away:
            self._draft.title_card_away_team = away
            self._tc_away_edit.setText(away)
        if date_iso:
            self._draft.title_card_date = date_iso
            self._tc_date_edit.setText(date_iso)
        self._refresh_dynamic_sections()

    def _on_files_changed(self) -> None:
        if self._file_list_widget is None:
            return
        self._draft.files = self._file_list_widget.collect()
        self._sync_editor_state()

    def _on_pi_files_changed(self) -> None:
        if not hasattr(self, "_pi_file_list"):
            return
        self._draft.files = self._pi_file_list.collect()
        self._sync_editor_state()

    def _on_file_pattern_changed(self, text: str) -> None:
        self._draft.file_pattern = text.strip() or "*.mp4"
        self._refresh_dynamic_sections()

    def _on_encoder_changed(self, index: int) -> None:
        self._draft.encoder = self._encoder_combo.itemData(index) or "auto"
        self._refresh_dynamic_sections()

    def _on_amplify_toggled(self, checked: bool) -> None:
        self._draft.amplify_audio = checked
        self._amplify_db_spin.setEnabled(checked)
        self._refresh_dynamic_sections()

    def _on_device_changed(self, index: int) -> None:
        self._draft.device_name = self._device_combo.itemData(index) or ""
        self._refresh_dynamic_sections()

    def _open_match_editor_for_playlist(self) -> None:
        tc_date = self._tc_date_edit.text().strip()
        tc_date_iso = ""
        if tc_date:
            parts = tc_date.split(".")
            if len(parts) == 3:
                tc_date_iso = f"{parts[2]}-{parts[1]}-{parts[0]}"
            else:
                tc_date_iso = tc_date
        initial = MatchData(
            competition=self._draft.default_youtube_competition.strip(),
            home_team=self._draft.title_card_home_team.strip(),
            away_team=self._draft.title_card_away_team.strip(),
            date_iso=tc_date_iso,
        )
        dlg = YouTubeTitleEditorDialog(self, mode="playlist", initial_match=initial)
        if not dlg.exec():
            return
        self._yt_playlist_edit.setText(dlg.playlist_title)
        self._yt_competition_edit.setText(dlg.match_data.competition)
        if dlg.match_data.home_team:
            self._tc_home_edit.setText(dlg.match_data.home_team)
        if dlg.match_data.away_team:
            self._tc_away_edit.setText(dlg.match_data.away_team)
        if dlg.match_data.date_iso:
            self._tc_date_edit.setText(dlg.match_data.date_iso)

    def _kb_load_api_data(self, force: bool = False) -> None:
        if self._settings is None:
            return
        if self._kb_api_loaded and not force:
            return
        kb = self._settings.kaderblick
        active_token = kb.jwt_token if kb.auth_mode == "jwt" else kb.bearer_token
        if not active_token:
            if self._file_list_widget is not None:
                self._file_list_widget.set_kaderblick_options([], [])
            if hasattr(self, "_pi_file_list"):
                self._pi_file_list.set_kaderblick_options([], [])
            mode_label = "JWT-Token" if kb.auth_mode == "jwt" else "Bearer-Token"
            self._kb_status_label.setText(f"⚠ Kein {mode_label} konfiguriert.")
            self._kb_status_label.setStyleSheet("color: orange;")
            self._kb_api_loaded = True
            return

        self._kb_reload_btn.setEnabled(False)
        self._kb_status_label.setText("⏳ Lade von API …")
        self._kb_status_label.setStyleSheet("color: #64748B;")
        QCoreApplication.processEvents()

        errors = []
        video_types = []
        cameras = []
        try:
            video_types = fetch_video_types(kb)
        except Exception as exc:
            errors.append(f"Video-Typen: {exc}")
        try:
            cameras = fetch_cameras(kb)
        except Exception as exc:
            errors.append(f"Kameras: {exc}")

        if self._file_list_widget is not None:
            self._file_list_widget.set_kaderblick_options(video_types, cameras)
        if hasattr(self, "_pi_file_list"):
            self._pi_file_list.set_kaderblick_options(video_types, cameras)

        if errors:
            self._kb_status_label.setText("❌ Fehler:\n" + "\n".join(errors))
            self._kb_status_label.setStyleSheet("color: red;")
        else:
            self._kb_status_label.setText(f"✅ {len(video_types)} Typen, {len(cameras)} Kameras geladen.")
            self._kb_status_label.setStyleSheet("color: green;")
        self._kb_reload_btn.setEnabled(True)
        self._kb_api_loaded = True

    def _load_pi_camera_files(self) -> None:
        if self._settings is None:
            return
        device_name = self._device_combo.currentData()
        if not device_name:
            QMessageBox.warning(self, "Kein Gerät", "Bitte zuerst ein Pi-Kamera-Gerät auswählen.")
            return
        device = next((item for item in self._settings.cameras.devices if item.name == device_name), None)
        if device is None:
            QMessageBox.warning(self, "Gerät nicht gefunden", f"Gerät '{device_name}' nicht in der Konfiguration.")
            return
        self._pi_load_btn.setEnabled(False)
        self._pi_load_status.setText("Verbinde …")
        self._pi_load_status.setStyleSheet("color: #64748B;")
        QCoreApplication.processEvents()
        self._pi_list_worker = _CameraListWorker(device, self._settings.cameras, self)
        self._pi_list_worker.finished.connect(self._on_camera_files_loaded)
        self._pi_list_worker.error.connect(self._on_camera_files_error)
        self._pi_list_worker.start()

    def _on_camera_files_loaded(self, files: list) -> None:
        self._pi_load_btn.setEnabled(True)
        if not files:
            self._pi_load_status.setText("Keine Aufnahmen auf der Kamera gefunden.")
            self._pi_load_status.setStyleSheet("color: orange;")
            return
        destination = self._pi_dest_edit.text().strip() or (self._settings.cameras.destination if self._settings is not None else "")
        device_name = self._device_combo.currentData() or ""
        entries: list[FileEntry] = []
        for item in files:
            entries.append(
                FileEntry(
                    source_path=str(Path(destination) / device_name / f"{item['base']}.mjpg"),
                    youtube_title=item["base"],
                )
            )
        self._pi_file_list.load(entries)
        self._pi_file_list.setVisible(True)
        self._draft.files = self._pi_file_list.collect()
        self._pi_load_status.setText(f"✓ {len(entries)} Aufnahme(n) gefunden.")
        self._pi_load_status.setStyleSheet("color: green;")
        self._sync_editor_state()

    def _on_camera_files_error(self, msg: str) -> None:
        self._pi_load_btn.setEnabled(True)
        self._pi_load_status.setText(f"❌ {msg}")
        self._pi_load_status.setStyleSheet("color: red; font-weight: 700;")

    def _set_source_editor_visibility(self) -> None:
        if not hasattr(self, "_source_mode_widgets"):
            return
        for mode, widget in self._source_mode_widgets.items():
            widget.setVisible(mode == self._draft.source_mode)

    def _save_last_dir(self, directory: str) -> None:
        if self._settings is None:
            return
        self._settings.last_directory = directory
        self._settings.save()