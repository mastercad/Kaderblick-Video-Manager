"""Wizard-Dialog zum Anlegen und Bearbeiten eines Workflow-Auftrags.

Vier fokussierte Schritte:
  1. Quelle      – woher kommen die Dateien
  2. Verarbeitung – Encoding + Audio
  3. Titelkarte   – optionales Intro-Bild vor jedem Video
  4. Upload       – YouTube + Kaderblick
"""

from pathlib import Path

from PySide6.QtCore import Qt, QCoreApplication, QThread, Signal
from PySide6.QtGui import QFont
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout, QStackedWidget,
    QGroupBox, QLabel, QPushButton,
    QCheckBox, QComboBox, QSpinBox, QDoubleSpinBox,
    QLineEdit, QFileDialog, QColorDialog, QMessageBox, QRadioButton,
    QButtonGroup, QWidget, QFrame, QSizePolicy,
)

from .settings import AppSettings, PROFILES
from .encoder import available_encoder_choices
from .file_list_widget import FileListWidget
from .workflow import WorkflowJob, FileEntry
from .kaderblick import fetch_video_types, fetch_cameras


# ─── Step-indicator styles ────────────────────────────────────────
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


# ─── Background worker for camera file listing ────────────────────
class _CameraListWorker(QThread):
    finished = Signal(list)
    error    = Signal(str)

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


# ─────────────────────────────────────────────────────────────────
#  Main wizard dialog
# ─────────────────────────────────────────────────────────────────
class JobEditorDialog(QDialog):
    """3-Schritt-Wizard zum Anlegen / Bearbeiten eines WorkflowJob."""

    _STEPS = ["1  Quelle", "2  Verarbeitung", "3  Titelkarte", "4  Upload"]

    def __init__(self, parent, settings: AppSettings,
                 job: WorkflowJob | None = None):
        super().__init__(parent)
        self._settings   = settings
        self._job        = job or self._create_default_job(settings)
        self._is_new     = job is None
        self._kb_api_loaded = False
        self._yt_competition = self._job.default_youtube_competition
        self._current    = 0
        self.setWindowTitle(
            "Neuer Auftrag" if self._is_new
            else f"Auftrag bearbeiten – {self._job.name}")
        self.setMinimumWidth(720)
        self.setMinimumHeight(540)
        self._build_ui()
        self._populate_from_job()
        self._refresh_nav()

    # ── Public ───────────────────────────────────────────────
    @property
    def result_job(self) -> WorkflowJob:
        return self._job

    @staticmethod
    def _create_default_job(settings: AppSettings) -> WorkflowJob:
        job = WorkflowJob()
        job.encoder       = settings.video.encoder
        job.crf           = settings.video.crf
        job.preset        = settings.video.preset
        job.fps           = settings.video.fps
        job.output_format = settings.video.output_format
        job.amplify_audio = settings.audio.amplify_audio
        job.amplify_db    = settings.audio.amplify_db
        return job

    # ── Shell ────────────────────────────────────────────────
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
        f = QFrame(); f.setFrameShape(QFrame.HLine)
        f.setStyleSheet("color:#ddd;"); return f

    def _build_step_bar(self) -> QWidget:
        bar = QWidget()
        bar.setStyleSheet("background:#f5f6f8;")
        lay = QHBoxLayout(bar)
        lay.setContentsMargins(20, 10, 20, 10)
        lay.setSpacing(6)
        self._step_labels: list[QLabel] = []
        for i, name in enumerate(self._STEPS):
            lbl = QLabel(name)
            lbl.setAlignment(Qt.AlignCenter)
            self._step_labels.append(lbl)
            lay.addWidget(lbl)
            if i < len(self._STEPS) - 1:
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

        self._finish_btn = QPushButton("✓  Fertig")
        self._finish_btn.setDefault(True)
        self._finish_btn.setStyleSheet(
            "QPushButton{background:#27ae60;color:white;font-weight:bold;"
            "padding:5px 20px;border-radius:4px;}"
            "QPushButton:hover{background:#219a52;}")
        self._finish_btn.clicked.connect(self._finish)
        lay.addWidget(self._finish_btn)
        return lay

    # ════════════════════════════════════════════════════════
    #  PAGE 1 – Quelle
    # ════════════════════════════════════════════════════════
    def _build_page_source(self) -> QWidget:
        page = QWidget()
        lay  = QVBoxLayout(page)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        # Job name
        name_row = QFormLayout()
        self._name_edit = QLineEdit()
        self._name_edit.setPlaceholderText(
            "Kurzbezeichnung (wird automatisch generiert, wenn leer)")
        name_row.addRow("Auftragsname:", self._name_edit)
        lay.addLayout(name_row)

        # Source cards
        cards_label = QLabel("Dateiquelle")
        cards_label.setStyleSheet("font-weight:bold; font-size:13px; color:#333;")
        lay.addWidget(cards_label)

        cards_row = QHBoxLayout()
        cards_row.setSpacing(10)
        self._mode_group = QButtonGroup(self)
        for mode_id, icon, title, desc in [
            (0, "📁", "Dateien auswählen",
             "Einzelne oder mehrere\nDateien direkt wählen"),
            (1, "📂", "Ordner scannen",
             "Alle passenden Dateien\nin einem Ordner verarbeiten"),
            (2, "📷", "Pi-Kamera",
             "Aufnahmen von einer\nRaspberry-Pi-Kamera laden"),
        ]:
            cards_row.addWidget(self._make_mode_card(mode_id, icon, title, desc))
        lay.addLayout(cards_row)

        # Sub-page per source mode
        self._source_stack = QStackedWidget()
        self._source_stack.addWidget(self._build_files_panel())
        self._source_stack.addWidget(self._build_folder_panel())
        self._source_stack.addWidget(self._build_pi_panel())
        self._mode_group.idToggled.connect(
            lambda _id, on: self._source_stack.setCurrentIndex(_id) if on else None)
        lay.addWidget(self._source_stack, stretch=1)
        return page

    # ── Source mode card ──────────────────────────────────────
    def _make_mode_card(self, mode_id: int, icon: str,
                        title: str, desc: str) -> QWidget:
        rb = QRadioButton()
        self._mode_group.addButton(rb, mode_id)

        frame = QFrame()
        frame.setCursor(Qt.PointingHandCursor)
        frame.setFrameShape(QFrame.StyledPanel)
        frame.setStyleSheet(
            "QFrame{border:2px solid #d0d7de;border-radius:10px;"
            "background:#fafbfc;padding:6px;}"
            "QFrame:hover{border-color:#3a7bde;background:#f0f5ff;}")
        frame.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        inner = QVBoxLayout(frame)
        inner.setContentsMargins(10, 12, 10, 12)
        inner.setSpacing(4)

        icon_lbl = QLabel(icon)
        icon_lbl.setAlignment(Qt.AlignCenter)
        icon_lbl.setStyleSheet(
            "font-size:32px; border:none; background:transparent;")
        inner.addWidget(icon_lbl)

        title_lbl = QLabel(title)
        title_lbl.setAlignment(Qt.AlignCenter)
        title_lbl.setStyleSheet(
            "font-weight:bold; font-size:12px; border:none; background:transparent;")
        inner.addWidget(title_lbl)

        desc_lbl = QLabel(desc)
        desc_lbl.setAlignment(Qt.AlignCenter)
        desc_lbl.setWordWrap(True)
        desc_lbl.setStyleSheet(
            "color:#666; font-size:11px; border:none; background:transparent;")
        inner.addWidget(desc_lbl)

        inner.addWidget(rb, alignment=Qt.AlignCenter)
        frame.mousePressEvent = lambda _e, b=rb: b.setChecked(True)
        return frame

    # ── Files panel ───────────────────────────────────────────
    def _build_files_panel(self) -> QWidget:
        w   = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(0, 4, 0, 0)
        self._file_list = FileListWidget(
            last_dir_getter=lambda: self._settings.last_directory,
            last_dir_setter=self._save_last_dir,
        )
        self._file_list.match_data_changed.connect(self._on_match_data_from_files)
        lay.addWidget(self._file_list)

        form = QFormLayout()
        form.setContentsMargins(0, 4, 0, 0)

        self._files_dst_edit = QLineEdit()
        self._files_dst_edit.setPlaceholderText("leer = Dateien am Quellort verarbeiten")
        dst_btn = self._browse_btn(lambda: self._browse_dir(
            self._files_dst_edit, "Zielordner wählen"))
        form.addRow("Zielordner:", self._hbox(self._files_dst_edit, dst_btn))

        self._files_move_cb = QCheckBox(
            "Quelldateien in Zielordner verschieben (statt kopieren)")
        form.addRow("", self._files_move_cb)

        lay.addLayout(form)
        return w

    # ── Folder panel ──────────────────────────────────────────
    def _build_folder_panel(self) -> QWidget:
        w    = QWidget()
        form = QFormLayout(w)
        form.setContentsMargins(0, 4, 0, 0)

        self._folder_src_edit = QLineEdit()
        self._folder_src_edit.setPlaceholderText("Quellordner …")
        src_btn = self._browse_btn(lambda: self._browse_dir(
            self._folder_src_edit, "Quellordner wählen"))
        form.addRow("Quellordner:", self._hbox(self._folder_src_edit, src_btn))

        self._file_pattern_edit = QLineEdit("*.mp4")
        self._file_pattern_edit.setPlaceholderText("*.mp4")
        form.addRow("Datei-Muster:", self._file_pattern_edit)

        self._folder_dst_edit = QLineEdit()
        self._folder_dst_edit.setPlaceholderText("leer = neben der Quelldatei")
        dst_btn = self._browse_btn(lambda: self._browse_dir(
            self._folder_dst_edit, "Zielordner wählen"))
        form.addRow("Zielordner:", self._hbox(self._folder_dst_edit, dst_btn))

        self._move_files_cb = QCheckBox(
            "Quelldateien nach Verarbeitung in Zielordner verschieben")
        form.addRow("", self._move_files_cb)

        self._folder_prefix_edit = QLineEdit()
        self._folder_prefix_edit.setPlaceholderText("leer = Originaldateiname behalten")
        form.addRow("Ausgabe-Präfix:", self._folder_prefix_edit)
        return w

    # ── Pi panel ──────────────────────────────────────────────
    def _build_pi_panel(self) -> QWidget:
        w   = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(8)

        form = QFormLayout()
        form.setContentsMargins(0, 4, 0, 0)

        self._device_combo = QComboBox()
        self._device_combo.addItem("(Gerät wählen)", "")
        for dev in self._settings.cameras.devices:
            self._device_combo.addItem(f"{dev.name}  ({dev.ip})", dev.name)
        form.addRow("Gerät:", self._device_combo)

        self._pi_dest_edit = QLineEdit()
        self._pi_dest_edit.setPlaceholderText("Lokales Zielverzeichnis …")
        pi_btn = self._browse_btn(lambda: self._browse_dir(
            self._pi_dest_edit, "Zielverzeichnis wählen"))
        form.addRow("Zielverzeichnis:", self._hbox(self._pi_dest_edit, pi_btn))

        self._delete_after_dl_cb = QCheckBox(
            "Aufnahmen nach Download von Kamera löschen")
        form.addRow("", self._delete_after_dl_cb)

        self._pi_prefix_edit = QLineEdit()
        self._pi_prefix_edit.setPlaceholderText("leer = Originaldateiname")
        form.addRow("Ausgabe-Präfix:", self._pi_prefix_edit)
        lay.addLayout(form)

        # Camera browser
        load_row = QHBoxLayout()
        self._pi_load_btn = QPushButton("📋 Dateien von Kamera laden")
        self._pi_load_btn.setToolTip(
            "Verbindet sich per SFTP mit der Kamera und listet vorhandene Aufnahmen.\n"
            "Anschließend kannst du YT-Titel und Playlist pro Datei setzen\n"
            "und nur ausgewählte Dateien herunterladen.")
        self._pi_load_btn.clicked.connect(self._load_pi_camera_files)
        load_row.addWidget(self._pi_load_btn)
        self._pi_load_status = QLabel("")
        self._pi_load_status.setStyleSheet("color:gray; font-style:italic;")
        load_row.addWidget(self._pi_load_status, stretch=1)
        lay.addLayout(load_row)

        self._pi_file_list = FileListWidget(
            last_dir_getter=lambda: self._settings.last_directory,
            last_dir_setter=lambda _d: None,
        )
        self._pi_file_list.setVisible(False)
        lay.addWidget(self._pi_file_list, stretch=1)
        return w

    # ════════════════════════════════════════════════════════
    #  PAGE 2 – Verarbeitung
    # ════════════════════════════════════════════════════════
    def _build_page_processing(self) -> QWidget:
        page = QWidget()
        lay  = QVBoxLayout(page)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        # Master toggle
        self._convert_enabled_cb = QCheckBox(
            "Dateien konvertieren  (Encoding aktiv)")
        font = QFont(); font.setPointSize(13); font.setBold(True)
        self._convert_enabled_cb.setFont(font)
        self._convert_enabled_cb.toggled.connect(
            lambda on: self._encoding_widget.setEnabled(on))
        lay.addWidget(self._convert_enabled_cb)

        # Encoding box
        self._encoding_widget = QGroupBox("Encoding-Einstellungen")
        enc_lay = QVBoxLayout(self._encoding_widget)

        # Profile shortcuts
        prof_row = QHBoxLayout()
        prof_row.addWidget(QLabel("Schnell-Profil:"))
        for pname in PROFILES:
            btn = QPushButton(pname)
            btn.setFlat(True)
            btn.setStyleSheet(
                "QPushButton{text-decoration:underline;color:#3a7bde;border:none;}")
            btn.clicked.connect(lambda _c, n=pname: self._apply_profile(n))
            prof_row.addWidget(btn)
        prof_row.addStretch()
        enc_lay.addLayout(prof_row)

        enc_form = QFormLayout()
        enc_form.setContentsMargins(0, 4, 0, 0)

        self._encoder_combo = QComboBox()
        for enc_id, enc_name in available_encoder_choices():
            self._encoder_combo.addItem(enc_name, enc_id)
        enc_form.addRow("Encoder:", self._encoder_combo)

        self._preset_combo = QComboBox()
        self._preset_combo.addItems([
            "ultrafast", "superfast", "veryfast", "faster", "fast",
            "medium", "slow", "slower", "veryslow"])
        enc_form.addRow("Preset:", self._preset_combo)

        crf_row = QHBoxLayout()
        self._crf_spin = QSpinBox(); self._crf_spin.setRange(0, 51)
        self._crf_spin.setFixedWidth(70)
        crf_row.addWidget(self._crf_spin)
        hint = QLabel("  0 = verlustfrei  ·  18 = sehr gut  ·  23 = Standard")
        hint.setStyleSheet("color:#888; font-size:11px;")
        crf_row.addWidget(hint); crf_row.addStretch()
        enc_form.addRow("CRF:", crf_row)

        fps_row = QHBoxLayout()
        self._fps_spin = QSpinBox(); self._fps_spin.setRange(1, 120)
        self._fps_spin.setFixedWidth(70)
        fps_row.addWidget(self._fps_spin)
        fps_row.addWidget(QLabel("fps")); fps_row.addStretch()
        enc_form.addRow("Framerate:", fps_row)

        self._format_combo = QComboBox()
        self._format_combo.addItems(["mp4", "avi"])
        enc_form.addRow("Format:", self._format_combo)

        self._overwrite_cb = QCheckBox(
            "Vorhandene Ausgabedateien überschreiben  (Skip-Schutz deaktivieren)")
        self._overwrite_cb.setToolTip(
            "Wenn aktiviert, werden bereits konvertierte Dateien erneut verarbeitet.\n"
            "Normalerweise deaktiviert lassen – dann werden fertige Dateien übersprungen.")
        enc_form.addRow("", self._overwrite_cb)

        enc_lay.addLayout(enc_form)
        lay.addWidget(self._encoding_widget)

        # Audio box
        audio_box = QGroupBox("Audio")
        audio_lay = QVBoxLayout(audio_box)

        self._merge_audio_cb = QCheckBox(
            "Separate Audio-Spur zusammenführen  (.wav + Video)")
        audio_lay.addWidget(self._merge_audio_cb)

        amp_row = QHBoxLayout()
        self._amplify_audio_cb = QCheckBox("Lautstärke anpassen um")
        self._amplify_audio_cb.toggled.connect(
            lambda on: self._amplify_db_spin.setEnabled(on))
        amp_row.addWidget(self._amplify_audio_cb)
        self._amplify_db_spin = QDoubleSpinBox()
        self._amplify_db_spin.setRange(-20.0, 40.0)
        self._amplify_db_spin.setSingleStep(1.0)
        self._amplify_db_spin.setDecimals(1)
        self._amplify_db_spin.setSuffix(" dB")
        self._amplify_db_spin.setFixedWidth(90)
        amp_row.addWidget(self._amplify_db_spin)
        amp_row.addWidget(QLabel("  (+6 dB ≈ doppelte Lautstärke)"))
        amp_row.addStretch()
        audio_lay.addLayout(amp_row)

        self._audio_sync_cb = QCheckBox(
            "Audio-Sync aktivieren  (Frame-Drop-Korrektur für Pi-Kameras)")
        audio_lay.addWidget(self._audio_sync_cb)

        lay.addWidget(audio_box)
        lay.addStretch()
        return page

    # ════════════════════════════════════════════════════════
    #  PAGE 3 – Titelkarte
    # ════════════════════════════════════════════════════════
    def _build_page_titlecard(self) -> QWidget:
        page = QWidget()
        lay  = QVBoxLayout(page)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        tc_box = QGroupBox("Titelkarte / Intro-Bild")
        tc_lay = QVBoxLayout(tc_box)

        self._tc_enabled_cb = QCheckBox(
            "Titelkarte vor jedem Video einblenden")
        self._tc_enabled_cb.setStyleSheet("font-weight:bold; font-size:13px;")
        self._tc_enabled_cb.toggled.connect(
            lambda on: self._tc_details.setEnabled(on))
        tc_lay.addWidget(self._tc_enabled_cb)

        self._tc_details = QWidget()
        self._tc_details.setEnabled(False)
        form = QFormLayout(self._tc_details)
        form.setContentsMargins(20, 4, 0, 4)
        form.setSpacing(8)

        # Logo
        logo_row = QHBoxLayout()
        self._tc_logo_edit = QLineEdit()
        self._tc_logo_edit.setPlaceholderText("Pfad zum Logo-Bild (leer = kein Logo)")
        logo_row.addWidget(self._tc_logo_edit)
        browse_logo_btn = QPushButton("…")
        browse_logo_btn.setFixedWidth(32)
        browse_logo_btn.clicked.connect(self._browse_tc_logo)
        logo_row.addWidget(browse_logo_btn)
        form.addRow("Logo:", logo_row)

        # Teams
        self._tc_home_edit = QLineEdit()
        self._tc_home_edit.setPlaceholderText("z. B. FC Musterstadt")
        form.addRow("Heim:", self._tc_home_edit)

        self._tc_away_edit = QLineEdit()
        self._tc_away_edit.setPlaceholderText("z. B. FC Auswärts")
        form.addRow("Gast:", self._tc_away_edit)

        self._tc_date_edit = QLineEdit()
        self._tc_date_edit.setPlaceholderText("z. B. 15.03.2025")
        form.addRow("Datum:", self._tc_date_edit)

        # Duration
        dur_row = QHBoxLayout()
        self._tc_duration_spin = QDoubleSpinBox()
        self._tc_duration_spin.setRange(0.5, 10.0)
        self._tc_duration_spin.setSingleStep(0.5)
        self._tc_duration_spin.setDecimals(1)
        self._tc_duration_spin.setSuffix(" s")
        self._tc_duration_spin.setValue(3.0)
        self._tc_duration_spin.setFixedWidth(90)
        dur_row.addWidget(self._tc_duration_spin)
        dur_row.addStretch()
        form.addRow("Dauer:", dur_row)

        # Colors
        color_row = QHBoxLayout()
        self._tc_bg_btn = QPushButton("  ")
        self._tc_bg_btn.setFixedWidth(50)
        self._tc_bg_btn.setToolTip("Hintergrundfarbe wählen")
        self._tc_bg_btn.clicked.connect(lambda: self._pick_color("bg"))
        color_row.addWidget(QLabel("Hintergrund:"))
        color_row.addWidget(self._tc_bg_btn)
        color_row.addSpacing(20)
        self._tc_fg_btn = QPushButton("  ")
        self._tc_fg_btn.setFixedWidth(50)
        self._tc_fg_btn.setToolTip("Textfarbe wählen")
        self._tc_fg_btn.clicked.connect(lambda: self._pick_color("fg"))
        color_row.addWidget(QLabel("Text:"))
        color_row.addWidget(self._tc_fg_btn)
        color_row.addStretch()
        form.addRow("Farben:", color_row)

        hint = QLabel(
            "💡 Im »Dateien auswählen«-Modus kann pro Datei ein individueller "
            "Untertitel (z.B. \"1. Halbzeit\", \"Kamera 1\") in der Dateiliste "
            "eingetragen werden.")
        hint.setWordWrap(True)
        hint.setStyleSheet("color:#666; font-style:italic; padding:4px 0;")
        form.addRow(hint)

        tc_lay.addWidget(self._tc_details)
        lay.addWidget(tc_box)
        lay.addStretch()

        # Pre-fill with sensible defaults
        self._tc_bg_color = "#000000"
        self._tc_fg_color = "#FFFFFF"
        self._update_color_btn("bg", "#000000")
        self._update_color_btn("fg", "#FFFFFF")

        # Auto-detect default logo path
        default_logo = Path(__file__).parent.parent.parent / \
            "videoschnitt" / "assets" / "kaderblick.png"
        if default_logo.exists():
            self._tc_logo_edit.setText(str(default_logo))

        return page

    def _browse_tc_logo(self) -> None:
        start = (self._tc_logo_edit.text().strip()
                 or self._settings.last_directory
                 or str(Path.home()))
        path, _ = QFileDialog.getOpenFileName(
            self, "Logo-Bild wählen", start,
            "Bilder (*.png *.jpg *.jpeg *.svg *.webp);;Alle Dateien (*)")
        if path:
            self._tc_logo_edit.setText(path)

    def _pick_color(self, which: str) -> None:
        current = self._tc_bg_color if which == "bg" else self._tc_fg_color
        color = QColorDialog.getColor(QColor(current), self, "Farbe wählen")
        if color.isValid():
            hex_color = color.name()
            if which == "bg":
                self._tc_bg_color = hex_color
            else:
                self._tc_fg_color = hex_color
            self._update_color_btn(which, hex_color)

    def _update_color_btn(self, which: str, hex_color: str) -> None:
        btn = self._tc_bg_btn if which == "bg" else self._tc_fg_btn
        bg  = hex_color
        # Luminance-based contrast for button label
        r, g, b = int(bg[1:3], 16), int(bg[3:5], 16), int(bg[5:7], 16)
        fg = "#000000" if (0.299*r + 0.587*g + 0.114*b) > 128 else "#FFFFFF"
        btn.setStyleSheet(
            f"background-color: {bg}; color: {fg}; border: 1px solid #aaa;")
        btn.setText(bg)

    # ════════════════════════════════════════════════════════
    #  PAGE 4 – Upload
    # ════════════════════════════════════════════════════════
    def _build_page_upload(self) -> QWidget:
        page = QWidget()
        lay  = QVBoxLayout(page)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        # ── YouTube ───────────────────────────────────────────
        yt_box = QGroupBox("YouTube")
        yt_lay = QVBoxLayout(yt_box)

        self._yt_upload_cb = QCheckBox("Auf YouTube hochladen")
        self._yt_upload_cb.setStyleSheet("font-weight:bold; font-size:13px;")
        self._yt_upload_cb.toggled.connect(self._on_yt_toggled)
        yt_lay.addWidget(self._yt_upload_cb)

        self._yt_details = QWidget()
        yt_d = QFormLayout(self._yt_details)
        yt_d.setContentsMargins(20, 4, 0, 4)

        self._yt_create_cb = QCheckBox(
            "YouTube-optimierte Version erstellen (separate Datei mit YT-Codec-Empfehlungen)")
        yt_d.addRow(self._yt_create_cb)

        self._yt_title_edit = QLineEdit()
        self._yt_title_edit.setPlaceholderText("leer = Dateiname als Titel")
        yt_d.addRow("Standard-Titel:", self._yt_title_edit)

        yt_pl_row = QHBoxLayout()
        self._yt_playlist_edit = QLineEdit()
        self._yt_playlist_edit.setPlaceholderText("leer = keine Playlist")
        yt_pl_row.addWidget(self._yt_playlist_edit)
        yt_match_btn = QPushButton("🎬 Spieldaten …")
        yt_match_btn.setToolTip(
            "Datum, Wettbewerb und Teams eingeben → Playlist wird automatisch generiert.")
        yt_match_btn.clicked.connect(self._open_match_editor_for_playlist)
        yt_pl_row.addWidget(yt_match_btn)
        yt_d.addRow("Playlist:", yt_pl_row)

        self._yt_files_hint = QLabel(
            "💡 Im »Dateien auswählen«-Modus: Titel und Playlist pro Datei "
            "über den 🎬-Button in der Dateiliste setzen.")
        self._yt_files_hint.setWordWrap(True)
        self._yt_files_hint.setStyleSheet(
            "color:#666; font-style:italic; padding:4px 0;")
        yt_d.addRow(self._yt_files_hint)

        yt_lay.addWidget(self._yt_details)
        lay.addWidget(yt_box)

        # ── Kaderblick ────────────────────────────────────────
        kb_box = QGroupBox("Kaderblick")
        kb_lay = QVBoxLayout(kb_box)

        self._kb_upload_cb = QCheckBox(
            "Video nach YouTube-Upload auf Kaderblick eintragen")
        self._kb_upload_cb.setStyleSheet("font-weight:bold; font-size:13px;")
        self._kb_upload_cb.toggled.connect(self._on_kb_toggled)
        kb_lay.addWidget(self._kb_upload_cb)

        self._kb_details_widget = QWidget()
        kb_d = QFormLayout(self._kb_details_widget)
        kb_d.setContentsMargins(20, 4, 0, 4)

        self._kb_game_id_edit = QLineEdit()
        self._kb_game_id_edit.setPlaceholderText("z. B. 19")
        self._kb_game_id_edit.setMaximumWidth(120)
        kb_d.addRow("Spiel-ID:", self._kb_game_id_edit)

        reload_row = QHBoxLayout()
        self._kb_reload_btn = QPushButton("↺  Typen & Kameras neu laden")
        self._kb_reload_btn.clicked.connect(
            lambda: self._kb_load_api_data(force=True))
        reload_row.addWidget(self._kb_reload_btn); reload_row.addStretch()
        kb_d.addRow("", reload_row)

        self._kb_status_label = QLabel("")
        self._kb_status_label.setWordWrap(True)
        kb_d.addRow("", self._kb_status_label)

        kb_lay.addWidget(self._kb_details_widget)
        lay.addWidget(kb_box)
        lay.addStretch()
        return page

    # ════════════════════════════════════════════════════════
    #  Navigation
    # ════════════════════════════════════════════════════════
    def _refresh_nav(self) -> None:
        for i, lbl in enumerate(self._step_labels):
            if i < self._current:
                lbl.setStyleSheet(_STEP_DONE)
            elif i == self._current:
                lbl.setStyleSheet(_STEP_ACTIVE)
            else:
                lbl.setStyleSheet(_STEP_TODO)

        self._back_btn.setVisible(self._current > 0)
        is_last = self._current == len(self._STEPS) - 1
        self._next_btn.setVisible(not is_last)
        self._finish_btn.setVisible(is_last)
        self._stack.setCurrentIndex(self._current)

    def _go_next(self) -> None:
        if not self._validate_page(self._current):
            return
        self._current += 1
        if self._current == 3:              # entering upload page
            self._sync_upload_visibility()
        self._refresh_nav()

    def _go_back(self) -> None:
        if self._current > 0:
            self._current -= 1
            self._refresh_nav()

    def _finish(self) -> None:
        if not self._validate_page(self._current):
            return
        self._write_job()
        self.accept()

    # ════════════════════════════════════════════════════════
    #  Validation
    # ════════════════════════════════════════════════════════
    def _validate_page(self, page: int) -> bool:
        if page == 0:
            mode_id = self._mode_group.checkedId()
            if mode_id == -1:
                QMessageBox.warning(self, "Keine Quelle",
                                    "Bitte eine Dateiquelle auswählen.")
                return False
            if mode_id == 0 and self._file_list.is_empty():
                QMessageBox.warning(self, "Keine Dateien",
                                    "Bitte mindestens eine Datei hinzufügen.")
                return False
            if mode_id == 1 and not self._folder_src_edit.text().strip():
                QMessageBox.warning(self, "Kein Quellordner",
                                    "Bitte einen Quellordner angeben.")
                return False
            if mode_id == 2:
                if not self._device_combo.currentData():
                    QMessageBox.warning(self, "Kein Gerät",
                                        "Bitte ein Pi-Kamera-Gerät auswählen.")
                    return False
                if not self._pi_dest_edit.text().strip():
                    QMessageBox.warning(self, "Kein Zielverzeichnis",
                                        "Bitte ein lokales Zielverzeichnis angeben.")
                    return False
        return True

    # ════════════════════════════════════════════════════════
    #  Upload-page visibility sync
    # ════════════════════════════════════════════════════════
    def _sync_upload_visibility(self) -> None:
        yt_on = self._yt_upload_cb.isChecked()
        self._yt_details.setVisible(yt_on)
        if yt_on:
            is_files = self._mode_group.checkedId() == 0
            self._yt_title_edit.setVisible(not is_files)
            self._yt_playlist_edit.setVisible(not is_files)
            self._yt_files_hint.setVisible(is_files)
        self._kb_upload_cb.setEnabled(yt_on)
        if not yt_on:
            self._kb_upload_cb.setChecked(False)
        self._kb_details_widget.setVisible(self._kb_upload_cb.isChecked())

    def _on_yt_toggled(self, on: bool) -> None:
        self._sync_upload_visibility()

    def _on_kb_toggled(self, on: bool) -> None:
        self._kb_details_widget.setVisible(on)
        if on and not self._kb_api_loaded:
            self._kb_load_api_data()

    # ════════════════════════════════════════════════════════
    #  Populate / Write
    # ════════════════════════════════════════════════════════
    def _populate_from_job(self) -> None:
        job = self._job
        self._name_edit.setText(job.name)

        mode_map = {"files": 0, "folder_scan": 1, "pi_download": 2}
        mode_id  = mode_map.get(job.source_mode, 0)
        btn = self._mode_group.button(mode_id)
        if btn:
            btn.setChecked(True)
        self._source_stack.setCurrentIndex(mode_id)

        # files
        self._file_list.load(job.files)
        self._files_dst_edit.setText(job.copy_destination)
        self._files_move_cb.setChecked(job.move_files)

        # folder
        self._folder_src_edit.setText(job.source_folder)
        self._file_pattern_edit.setText(job.file_pattern or "*.mp4")
        self._folder_dst_edit.setText(job.copy_destination)
        self._move_files_cb.setChecked(job.move_files)
        self._folder_prefix_edit.setText(job.output_prefix)

        # pi
        dev_idx = self._device_combo.findData(job.device_name)
        if dev_idx >= 0:
            self._device_combo.setCurrentIndex(dev_idx)
        self._pi_dest_edit.setText(
            job.download_destination or self._settings.cameras.destination)
        self._delete_after_dl_cb.setChecked(job.delete_after_download)
        self._pi_prefix_edit.setText(job.output_prefix)
        if job.source_mode == "pi_download" and job.files:
            self._pi_file_list.load(job.files)
            self._pi_file_list.setVisible(True)
            self._pi_load_status.setText(
                f"✓ {len(job.files)} Aufnahme(n) gespeichert.")
            self._pi_load_status.setStyleSheet("color:green;")

        # processing
        self._convert_enabled_cb.setChecked(job.convert_enabled)
        self._encoding_widget.setEnabled(job.convert_enabled)
        enc_idx = self._encoder_combo.findData(job.encoder)
        if enc_idx >= 0:
            self._encoder_combo.setCurrentIndex(enc_idx)
        self._preset_combo.setCurrentText(job.preset)
        self._crf_spin.setValue(job.crf)
        self._fps_spin.setValue(job.fps)
        self._format_combo.setCurrentText(job.output_format)
        self._overwrite_cb.setChecked(job.overwrite)

        # audio
        self._merge_audio_cb.setChecked(job.merge_audio)
        self._amplify_audio_cb.setChecked(job.amplify_audio)
        self._amplify_db_spin.setValue(job.amplify_db)
        self._amplify_db_spin.setEnabled(job.amplify_audio)
        self._audio_sync_cb.setChecked(job.audio_sync)

        # youtube
        self._yt_upload_cb.setChecked(job.upload_youtube)
        self._yt_create_cb.setChecked(job.create_youtube_version)
        self._yt_title_edit.setText(job.default_youtube_title)
        self._yt_playlist_edit.setText(job.default_youtube_playlist)
        self._yt_competition = job.default_youtube_competition
        self._yt_details.setVisible(job.upload_youtube)

        # kaderblick
        self._kb_upload_cb.setChecked(job.upload_kaderblick)
        self._kb_upload_cb.setEnabled(job.upload_youtube)
        self._kb_game_id_edit.setText(job.default_kaderblick_game_id)
        self._kb_details_widget.setVisible(job.upload_kaderblick)

        # title card
        self._tc_enabled_cb.setChecked(job.title_card_enabled)
        self._tc_details.setEnabled(job.title_card_enabled)
        self._tc_logo_edit.setText(job.title_card_logo_path)
        self._tc_home_edit.setText(job.title_card_home_team)
        self._tc_away_edit.setText(job.title_card_away_team)
        self._tc_date_edit.setText(job.title_card_date)
        # Fehlende Titelkartenwerte aus dem Match-Memory nachfüllen
        if not job.title_card_home_team or not job.title_card_away_team or not job.title_card_date:
            from .youtube_title_editor import load_memory
            mem = load_memory()
            last_m = mem.get("last_match", {})
            if not job.title_card_home_team and last_m.get("home_team"):
                self._tc_home_edit.setText(last_m["home_team"])
            if not job.title_card_away_team and last_m.get("away_team"):
                self._tc_away_edit.setText(last_m["away_team"])
            if not job.title_card_date and last_m.get("date_iso"):
                try:
                    y, mo, d = last_m["date_iso"].split("-")
                    self._tc_date_edit.setText(f"{d}.{mo}.{y}")
                except Exception:
                    self._tc_date_edit.setText(last_m["date_iso"])
        self._tc_duration_spin.setValue(job.title_card_duration)
        bg = job.title_card_bg_color or "#000000"
        fg = job.title_card_fg_color or "#FFFFFF"
        self._tc_bg_color = bg
        self._tc_fg_color = fg
        self._update_color_btn("bg", bg)
        self._update_color_btn("fg", fg)

    def _write_job(self) -> None:
        job     = self._job
        mode_id = self._mode_group.checkedId()
        mode_map = {0: "files", 1: "folder_scan", 2: "pi_download"}
        job.source_mode = mode_map[mode_id]
        job.name        = self._name_edit.text().strip()

        if mode_id == 0:
            job.files            = self._file_list.collect()
            job.copy_destination = self._files_dst_edit.text().strip()
            job.move_files       = self._files_move_cb.isChecked()
            job.source_folder    = ""
            job.device_name      = ""
            if not job.name:
                job.name = (Path(job.files[0].source_path).stem
                            if job.files else "Auftrag")

        elif mode_id == 1:
            job.source_folder  = self._folder_src_edit.text().strip()
            job.file_pattern   = (self._file_pattern_edit.text().strip() or "*.mp4")
            job.copy_destination = self._folder_dst_edit.text().strip()
            job.move_files     = self._move_files_cb.isChecked()
            job.output_prefix  = self._folder_prefix_edit.text().strip()
            job.files          = []
            job.device_name    = ""
            if not job.name:
                job.name = Path(job.source_folder).name or "Ordner"

        elif mode_id == 2:
            job.device_name          = self._device_combo.currentData()
            job.download_destination = self._pi_dest_edit.text().strip()
            job.delete_after_download = self._delete_after_dl_cb.isChecked()
            job.output_prefix        = self._pi_prefix_edit.text().strip()
            job.files  = (self._pi_file_list.collect()
                          if not self._pi_file_list.is_empty() else [])
            job.source_folder = ""
            if not job.name:
                job.name = job.device_name or "Pi-Kamera"

        job.convert_enabled   = self._convert_enabled_cb.isChecked()
        job.encoder            = self._encoder_combo.currentData()
        job.preset             = self._preset_combo.currentText()
        job.crf                = self._crf_spin.value()
        job.fps                = self._fps_spin.value()
        job.output_format      = self._format_combo.currentText()
        job.overwrite          = self._overwrite_cb.isChecked()

        job.merge_audio        = self._merge_audio_cb.isChecked()
        job.amplify_audio      = self._amplify_audio_cb.isChecked()
        job.amplify_db         = self._amplify_db_spin.value()
        job.audio_sync         = self._audio_sync_cb.isChecked()

        job.create_youtube_version  = self._yt_create_cb.isChecked()
        job.upload_youtube          = self._yt_upload_cb.isChecked()
        job.default_youtube_title   = self._yt_title_edit.text().strip()
        job.default_youtube_playlist = self._yt_playlist_edit.text().strip()
        job.default_youtube_competition = self._yt_competition.strip()

        job.upload_kaderblick          = self._kb_upload_cb.isChecked()
        job.default_kaderblick_game_id = self._kb_game_id_edit.text().strip()

        job.title_card_enabled    = self._tc_enabled_cb.isChecked()
        job.title_card_logo_path  = self._tc_logo_edit.text().strip()
        job.title_card_home_team  = self._tc_home_edit.text().strip()
        job.title_card_away_team  = self._tc_away_edit.text().strip()
        job.title_card_date       = self._tc_date_edit.text().strip()
        job.title_card_duration   = self._tc_duration_spin.value()
        job.title_card_bg_color   = self._tc_bg_color
        job.title_card_fg_color   = self._tc_fg_color

    # ════════════════════════════════════════════════════════
    #  Helpers
    # ════════════════════════════════════════════════════════
    @staticmethod
    def _hbox(*widgets) -> QHBoxLayout:
        lay = QHBoxLayout()
        for w in widgets:
            lay.addWidget(w)
        return lay

    @staticmethod
    def _browse_btn(callback) -> QPushButton:
        btn = QPushButton("…")
        btn.setFixedWidth(32)
        btn.clicked.connect(callback)
        return btn

    def _browse_dir(self, line_edit: QLineEdit, title: str) -> None:
        start = (line_edit.text().strip()
                 or self._settings.last_directory
                 or str(Path.home()))
        chosen = QFileDialog.getExistingDirectory(self, title, start)
        if chosen:
            line_edit.setText(chosen)
            self._save_last_dir(chosen)

    def _save_last_dir(self, directory: str) -> None:
        self._settings.last_directory = directory
        self._settings.save()

    def _load_pi_camera_files(self) -> None:
        dev_name = self._device_combo.currentData()
        if not dev_name:
            QMessageBox.warning(self, "Kein Gerät", "Bitte zuerst ein Gerät auswählen.")
            return
        dev = next((d for d in self._settings.cameras.devices
                    if d.name == dev_name), None)
        if dev is None:
            QMessageBox.warning(self, "Gerät nicht gefunden",
                                f"Gerät '{dev_name}' nicht in der Konfiguration.")
            return
        self._pi_load_btn.setEnabled(False)
        self._pi_load_status.setText("Verbinde …")
        self._pi_load_status.setStyleSheet("color:gray; font-style:italic;")
        QCoreApplication.processEvents()
        self._pi_list_worker = _CameraListWorker(dev, self._settings.cameras, self)
        self._pi_list_worker.finished.connect(self._on_camera_files_loaded)
        self._pi_list_worker.error.connect(self._on_camera_files_error)
        self._pi_list_worker.start()

    def _on_camera_files_loaded(self, files: list) -> None:
        self._pi_load_btn.setEnabled(True)
        if not files:
            self._pi_load_status.setText("Keine Aufnahmen auf der Kamera gefunden.")
            self._pi_load_status.setStyleSheet("color:orange;")
            return
        dev_name = self._device_combo.currentData()
        dest     = (self._pi_dest_edit.text().strip()
                    or self._settings.cameras.destination)
        entries  = [
            FileEntry(
                source_path=str(Path(dest) / dev_name / f"{f['base']}.mjpg"),
                youtube_title=f["base"],
            )
            for f in files
        ]
        self._pi_file_list.load(entries)
        self._pi_file_list.setVisible(True)
        self._pi_load_status.setText(f"✓ {len(files)} Aufnahme(n) gefunden.")
        self._pi_load_status.setStyleSheet("color:green;")

    def _on_camera_files_error(self, msg: str) -> None:
        self._pi_load_btn.setEnabled(True)
        self._pi_load_status.setText(f"❌ {msg}")
        self._pi_load_status.setStyleSheet("color:#c0392b; font-weight:bold;")

    def _on_match_data_from_files(self, home: str, away: str, date_iso: str) -> None:
        """Übernimmt Spieldaten aus der Dateiliste in die Titelkarten-Felder."""
        if home:
            self._tc_home_edit.setText(home)
        if away:
            self._tc_away_edit.setText(away)
        if date_iso:
            try:  # "YYYY-MM-DD" → "DD.MM.YYYY"
                y, mo, d = date_iso.split("-")
                self._tc_date_edit.setText(f"{d}.{mo}.{y}")
            except Exception:
                self._tc_date_edit.setText(date_iso)

    def _open_match_editor_for_playlist(self) -> None:
        from .youtube_title_editor import YouTubeTitleEditorDialog, MatchData
        # Vorhandene Titelkarten-Werte als Vorbelegung übergeben
        tc_date = self._tc_date_edit.text().strip()
        tc_date_iso = ""
        if tc_date:
            try:  # "DD.MM.YYYY" → "YYYY-MM-DD"
                parts = tc_date.split(".")
                if len(parts) == 3:
                    tc_date_iso = f"{parts[2]}-{parts[1]}-{parts[0]}"
                else:
                    tc_date_iso = tc_date
            except Exception:
                tc_date_iso = tc_date
        initial = MatchData(
            competition=self._yt_competition.strip(),
            home_team=self._tc_home_edit.text().strip(),
            away_team=self._tc_away_edit.text().strip(),
            date_iso=tc_date_iso,
        )
        dlg = YouTubeTitleEditorDialog(self, mode="playlist", initial_match=initial)
        if dlg.exec():
            self._yt_playlist_edit.setText(dlg.playlist_title)
            self._yt_competition = dlg.match_data.competition
            # Spieldaten in Titelkarten-Felder übernehmen
            m = dlg.match_data
            if m.home_team:
                self._tc_home_edit.setText(m.home_team)
            if m.away_team:
                self._tc_away_edit.setText(m.away_team)
            if m.date_iso:
                try:  # "YYYY-MM-DD" → "DD.MM.YYYY"
                    y, mo, d = m.date_iso.split("-")
                    self._tc_date_edit.setText(f"{d}.{mo}.{y}")
                except Exception:
                    self._tc_date_edit.setText(m.date_iso)

    def _kb_load_api_data(self, force: bool = False) -> None:
        if self._kb_api_loaded and not force:
            return
        kb = self._settings.kaderblick
        active_token = (kb.jwt_token if kb.auth_mode == "jwt" else kb.bearer_token)
        if not active_token:
            self._file_list.set_kaderblick_options([], [])
            mode_lbl = "JWT-Token" if kb.auth_mode == "jwt" else "Bearer-Token"
            self._kb_status_label.setText(
                f"⚠ Kein {mode_lbl} konfiguriert.\n"
                "Bitte unter Einstellungen → Kaderblick eintragen.")
            self._kb_status_label.setStyleSheet("color:orange;")
            self._kb_api_loaded = True
            return

        self._kb_reload_btn.setEnabled(False)
        self._kb_status_label.setText("⏳ Lade von API …")
        self._kb_status_label.setStyleSheet("color:gray;")
        QCoreApplication.processEvents()

        errors, types, cameras = [], [], []
        try:
            types   = fetch_video_types(kb)
        except Exception as exc:
            errors.append(f"Video-Typen: {exc}")
        try:
            cameras = fetch_cameras(kb)
        except Exception as exc:
            errors.append(f"Kameras: {exc}")

        self._file_list.set_kaderblick_options(types, cameras)
        if errors:
            self._kb_status_label.setText("❌ Fehler:\n" + "\n".join(errors))
            self._kb_status_label.setStyleSheet("color:red;")
        else:
            self._kb_status_label.setText(
                f"✅ {len(types)} Typen, {len(cameras)} Kameras geladen.")
            self._kb_status_label.setStyleSheet("color:green;")
        self._kb_reload_btn.setEnabled(True)
        self._kb_api_loaded = True

    def _apply_profile(self, name: str) -> None:
        values = PROFILES.get(name, {})
        if not values:
            return
        if "encoder" in values:
            idx = self._encoder_combo.findData(values["encoder"])
            if idx >= 0:
                self._encoder_combo.setCurrentIndex(idx)
        if "preset" in values:
            self._preset_combo.setCurrentText(values["preset"])
        if "crf" in values:
            self._crf_spin.setValue(values["crf"])
        if "output_format" in values:
            self._format_combo.setCurrentText(values["output_format"])
