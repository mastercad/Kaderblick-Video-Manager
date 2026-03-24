"""Wizard-Dialog zum Anlegen und Bearbeiten eines Workflows."""

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from ...settings import AppSettings
from ...workflow import WorkflowJob
from .pages import JobEditorPagesMixin
from .source_page import JobEditorSourceMixin
from .state import JobEditorStateMixin


class JobEditorDialog(JobEditorSourceMixin, JobEditorPagesMixin, JobEditorStateMixin, QDialog):
    """Assistent zum Anlegen und Bearbeiten eines Workflows."""

    _STEPS = ["1  Quelle", "2  Verarbeitung", "3  Titelkarte", "4  Upload"]
    _STEP_ACTIVE = (
        "QLabel { background:#3a7bde; color:white; font-weight:bold; "
        "border-radius:12px; padding:3px 14px; font-size:12px; }"
    )
    _STEP_DONE = (
        "QLabel { background:#27ae60; color:white; font-weight:bold; "
        "border-radius:12px; padding:3px 14px; font-size:12px; }"
    )
    _STEP_TODO = (
        "QLabel { background:#e0e0e0; color:#999; "
        "border-radius:12px; padding:3px 14px; font-size:12px; }"
    )

    def __init__(self, parent, settings: AppSettings, job: WorkflowJob | None = None):
        super().__init__(parent)
        self._settings = settings
        self._job = job or self._create_default_job(settings)
        self._is_new = job is None
        self._kb_api_loaded = False
        self._yt_competition = self._job.default_youtube_competition
        self._current = 0
        self.setWindowTitle("Neuer Workflow" if self._is_new else f"Workflow bearbeiten – {self._job.name}")
        self.setMinimumWidth(720)
        self.setMinimumHeight(540)
        self._build_ui()
        self._populate_from_job()
        self._refresh_nav()

    @property
    def result_job(self) -> WorkflowJob:
        return self._job

    @staticmethod
    def _create_default_job(settings: AppSettings) -> WorkflowJob:
        job = WorkflowJob()
        job.encoder = settings.video.encoder
        job.crf = settings.video.crf
        job.preset = settings.video.preset
        job.fps = settings.video.fps
        job.output_format = settings.video.output_format
        job.output_resolution = settings.video.output_resolution
        job.no_bframes = settings.video.no_bframes
        job.amplify_audio = settings.audio.amplify_audio
        job.amplify_db = settings.audio.amplify_db
        return job

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setSpacing(0)
        root.setContentsMargins(0, 0, 0, 0)

        root.addWidget(self._build_step_bar())
        root.addWidget(self._hline())

        self._stack = QStackedWidget()
        self._stack.addWidget(self._build_page_source())
        self._stack.addWidget(self._build_page_processing())
        self._stack.addWidget(self._build_page_titlecard())
        self._stack.addWidget(self._build_page_upload())
        root.addWidget(self._stack, stretch=1)

        root.addWidget(self._hline())
        root.addLayout(self._build_nav_bar())

    @staticmethod
    def _hline() -> QFrame:
        frame = QFrame()
        frame.setFrameShape(QFrame.HLine)
        frame.setStyleSheet("color:#ddd;")
        return frame

    def _build_step_bar(self) -> QWidget:
        bar = QWidget()
        bar.setStyleSheet("background:#f5f6f8;")
        lay = QHBoxLayout(bar)
        lay.setContentsMargins(20, 10, 20, 10)
        lay.setSpacing(6)
        self._step_labels = []
        for index, name in enumerate(self._STEPS):
            lbl = QLabel(name)
            lbl.setAlignment(Qt.AlignCenter)
            self._step_labels.append(lbl)
            lay.addWidget(lbl)
            if index < len(self._STEPS) - 1:
                arr = QLabel("  ›  ")
                arr.setStyleSheet("color:#ccc; font-size:18px;")
                lay.addWidget(arr)
        lay.addStretch()
        return bar

    def _build_nav_bar(self) -> QHBoxLayout:
        lay = QHBoxLayout()
        lay.setContentsMargins(16, 8, 16, 10)

        cancel = QPushButton("Abbrechen")
        cancel.clicked.connect(self.reject)
        lay.addWidget(cancel)
        lay.addStretch()

        self._back_btn = QPushButton("← Zurück")
        self._back_btn.clicked.connect(self._go_back)
        lay.addWidget(self._back_btn)

        self._next_btn = QPushButton("Weiter →")
        self._next_btn.setDefault(True)
        self._next_btn.clicked.connect(self._go_next)
        lay.addWidget(self._next_btn)

        self._preview_btn = QPushButton("Workflow-Vorschau")
        self._preview_btn.clicked.connect(self._open_workflow_preview)
        lay.addWidget(self._preview_btn)

        self._finish_btn = QPushButton("✓  Fertig")
        self._finish_btn.setDefault(True)
        self._finish_btn.setStyleSheet(
            "QPushButton{background:#27ae60;color:white;font-weight:bold;"
            "padding:5px 20px;border-radius:4px;}"
            "QPushButton:hover{background:#219a52;}"
        )
        self._finish_btn.clicked.connect(self._finish)
        lay.addWidget(self._finish_btn)
        return lay