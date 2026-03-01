"""Workflow-Wizard: Zwei-Etappen-Dialog zum Zusammenstellen von Aufträgen.

Seite 1 – Quellen:
  Quellen hinzufügen (Pi-Kameras, lokale Quellen / Datenträger).
  Jede Quelle hat einen Transfer-Typ und ein Zielverzeichnis.

Seite 2 – Verarbeitung:
  Kompakte Übersichtstabelle aller Quellen.  Doppelklick auf eine Zeile
  öffnet den Detail-Dialog für Encoding, Audio, YouTube-Einstellungen.

Am Ende: Globale Optionen (Rechner herunterfahren).
"""

from pathlib import Path

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QDialog, QDialogButtonBox, QVBoxLayout, QHBoxLayout, QFormLayout,
    QGroupBox, QLabel, QPushButton, QStackedWidget, QWidget,
    QTableWidget, QTableWidgetItem, QHeaderView, QAbstractItemView,
    QCheckBox, QComboBox, QSpinBox, QLineEdit, QFileDialog,
    QMessageBox, QScrollArea, QFrame, QSizePolicy, QRadioButton,
    QButtonGroup,
)

from .settings import AppSettings, PROFILES
from .encoder import available_encoder_choices, encoder_display_name, resolve_encoder
from .workflow import Workflow, WorkflowSource, WORKFLOW_DIR


# ═════════════════════════════════════════════════════════════════
#  Quellen-Editor (einzelne Quelle auf Seite 1)
# ═════════════════════════════════════════════════════════════════

class _SourceEditDialog(QDialog):
    """Dialog zum Anlegen / Bearbeiten einer einzelnen Workflow-Quelle."""

    def __init__(self, parent, settings: AppSettings,
                 source: WorkflowSource | None = None):
        super().__init__(parent)
        self.setWindowTitle(
            "Quelle bearbeiten" if source else "Quelle hinzufügen")
        self.setMinimumWidth(520)
        self._settings = settings
        self._source = source or WorkflowSource()

        layout = QVBoxLayout(self)

        # ── Typ-Auswahl ──────────────────────────────────────
        type_group = QGroupBox("Quelltyp")
        type_form = QFormLayout()

        self._type_combo = QComboBox()
        self._type_combo.addItem("Pi-Kamera (rsync/SSH)", "pi_camera")
        self._type_combo.addItem("Lokale Quelle / Datenträger", "local")
        idx = self._type_combo.findData(self._source.source_type)
        if idx >= 0:
            self._type_combo.setCurrentIndex(idx)
        self._type_combo.currentIndexChanged.connect(self._on_type_changed)
        type_form.addRow("Typ:", self._type_combo)

        type_group.setLayout(type_form)
        layout.addWidget(type_group)

        # ── Pi-Kamera ────────────────────────────────────────
        self._pi_group = QGroupBox("Pi-Kamera")
        pi_form = QFormLayout()

        self._device_combo = QComboBox()
        self._device_combo.addItem("(Gerät wählen)", "")
        for dev in settings.cameras.devices:
            self._device_combo.addItem(
                f"{dev.name} ({dev.ip})", dev.name)
        dev_idx = self._device_combo.findData(self._source.device_name)
        if dev_idx >= 0:
            self._device_combo.setCurrentIndex(dev_idx)
        pi_form.addRow("Gerät:", self._device_combo)

        dest_row = QHBoxLayout()
        self._pi_dest_edit = QLineEdit(
            self._source.destination_path
            or settings.cameras.destination)
        dest_row.addWidget(self._pi_dest_edit)
        pi_browse = QPushButton("…")
        pi_browse.setFixedWidth(32)
        pi_browse.clicked.connect(
            lambda: self._browse_dir(self._pi_dest_edit))
        dest_row.addWidget(pi_browse)
        pi_form.addRow("Zielverzeichnis:", dest_row)

        self._pi_group.setLayout(pi_form)
        layout.addWidget(self._pi_group)

        # ── Lokale Quelle ─────────────────────────────────────
        self._local_group = QGroupBox("Lokale Quelle / Datenträger")
        local_form = QFormLayout()

        # Modus-Auswahl: Ordner oder Datei(en)
        mode_row = QHBoxLayout()
        self._mode_folder_rb = QRadioButton("Ordner")
        self._mode_file_rb = QRadioButton("Datei(en)")
        self._mode_group = QButtonGroup(self)
        self._mode_group.addButton(self._mode_folder_rb, 0)
        self._mode_group.addButton(self._mode_file_rb, 1)
        mode_row.addWidget(self._mode_folder_rb)
        mode_row.addWidget(self._mode_file_rb)
        mode_row.addStretch()
        local_form.addRow("Modus:", mode_row)

        # ── Ordner-Modus ───────────────────────────────────
        self._folder_container = QWidget()
        folder_inner = QFormLayout(self._folder_container)
        folder_inner.setContentsMargins(0, 0, 0, 0)

        local_src_row = QHBoxLayout()
        self._local_src_edit = QLineEdit(self._source.source_path
            if not self._source.audio_path else "")
        self._local_src_edit.setPlaceholderText(
            "Ordner mit Aufnahmen (SSD, NAS, ext. Platte …)")
        local_src_row.addWidget(self._local_src_edit)
        local_browse = QPushButton("…")
        local_browse.setFixedWidth(32)
        local_browse.clicked.connect(
            lambda: self._browse_dir(self._local_src_edit))
        local_src_row.addWidget(local_browse)
        folder_inner.addRow("Quellordner:", local_src_row)

        self._ext_edit = QLineEdit(
            self._source.file_extensions or "*.mp4")
        self._ext_edit.setPlaceholderText("*.mp4")
        self._ext_edit.setToolTip(
            "Glob-Pattern für Dateien, z.B. *.mp4, *.mjpg, *.MP4")
        folder_inner.addRow("Datei-Pattern:", self._ext_edit)

        local_form.addRow(self._folder_container)

        # ── Datei-Modus ────────────────────────────────────
        self._file_container = QWidget()
        file_inner = QFormLayout(self._file_container)
        file_inner.setContentsMargins(0, 0, 0, 0)

        video_row = QHBoxLayout()
        self._video_file_edit = QLineEdit(
            self._source.source_path
            if self._source.audio_path else "")
        self._video_file_edit.setPlaceholderText("Videodatei auswählen")
        video_row.addWidget(self._video_file_edit)
        video_browse = QPushButton("…")
        video_browse.setFixedWidth(32)
        video_browse.clicked.connect(self._browse_video_file)
        video_row.addWidget(video_browse)
        file_inner.addRow("Videodatei:", video_row)

        audio_row = QHBoxLayout()
        self._audio_file_edit = QLineEdit(self._source.audio_path)
        self._audio_file_edit.setPlaceholderText(
            "Audiodatei (optional, für Zusammenführen)")
        audio_row.addWidget(self._audio_file_edit)
        audio_browse = QPushButton("…")
        audio_browse.setFixedWidth(32)
        audio_browse.clicked.connect(self._browse_audio_file)
        audio_row.addWidget(audio_browse)
        file_inner.addRow("Audiodatei:", audio_row)

        local_form.addRow(self._file_container)

        # ── Verschiebe-Option (beide Modi) ───────────────
        self._move_cb = QCheckBox("Dateien in Zielordner verschieben")
        self._move_cb.setChecked(self._source.move_to_destination)
        self._move_cb.setToolTip(
            "Wenn aktiv, werden die Dateien aus dem Quellordner\n"
            "in einen separaten Zielordner verschoben.\n"
            "Sonst werden sie direkt am Quellort verarbeitet.")
        self._move_cb.stateChanged.connect(self._on_move_toggled)
        local_form.addRow("", self._move_cb)

        # Zielordner (nur sichtbar wenn Verschieben aktiv)
        self._dest_container = QWidget()
        dest_inner = QFormLayout(self._dest_container)
        dest_inner.setContentsMargins(0, 0, 0, 0)
        local_dest_row = QHBoxLayout()
        self._local_dest_edit = QLineEdit(
            self._source.destination_path
            if self._source.move_to_destination else "")
        self._local_dest_edit.setPlaceholderText("Zielordner")
        local_dest_row.addWidget(self._local_dest_edit)
        local_dest_browse = QPushButton("…")
        local_dest_browse.setFixedWidth(32)
        local_dest_browse.clicked.connect(
            lambda: self._browse_dir(self._local_dest_edit))
        local_dest_row.addWidget(local_dest_browse)
        dest_inner.addRow("Zielordner:", local_dest_row)
        local_form.addRow(self._dest_container)

        self._local_group.setLayout(local_form)
        layout.addWidget(self._local_group)

        # Modus-Umschaltung verbinden
        self._mode_group.idToggled.connect(self._on_local_mode_changed)
        # Initialen Modus setzen
        if self._source.audio_path:
            self._mode_file_rb.setChecked(True)
        else:
            self._mode_folder_rb.setChecked(True)
        self._on_local_mode_changed()

        # ── Transfer-Optionen ─────────────────────────────────
        opt_group = QGroupBox("Transfer-Optionen")
        opt_form = QFormLayout()

        self._delete_cb = QCheckBox(
            "Quelldateien nach erfolgreichem Transfer löschen")
        self._delete_cb.setChecked(self._source.delete_source)
        opt_form.addRow("", self._delete_cb)

        opt_group.setLayout(opt_form)
        layout.addWidget(opt_group)

        # ── Buttons ──────────────────────────────────────────
        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self._on_type_changed()
        self._on_move_toggled()

    def _browse_dir(self, line_edit: QLineEdit):
        p = line_edit.text() or str(Path.home())
        folder = QFileDialog.getExistingDirectory(
            self, "Ordner wählen", p)
        if folder:
            line_edit.setText(folder)

    def _browse_video_file(self):
        p = self._video_file_edit.text() or str(Path.home())
        path, _ = QFileDialog.getOpenFileName(
            self, "Videodatei wählen", p,
            "Videodateien (*.mp4 *.mjpg *.mjpeg *.avi *.mkv *.mov);;"
            "Alle Dateien (*)")
        if path:
            self._video_file_edit.setText(path)

    def _browse_audio_file(self):
        start = self._video_file_edit.text() or str(Path.home())
        if Path(start).is_file():
            start = str(Path(start).parent)
        path, _ = QFileDialog.getOpenFileName(
            self, "Audiodatei wählen", start,
            "Audiodateien (*.wav *.mp3 *.aac *.flac *.ogg);;"
            "Alle Dateien (*)")
        if path:
            self._audio_file_edit.setText(path)

    def _on_type_changed(self):
        t = self._type_combo.currentData()
        self._pi_group.setVisible(t == "pi_camera")
        self._local_group.setVisible(t == "local")
        self.adjustSize()

    def _on_local_mode_changed(self):
        is_folder = self._mode_folder_rb.isChecked()
        self._folder_container.setVisible(is_folder)
        self._file_container.setVisible(not is_folder)
        self.adjustSize()

    def _on_move_toggled(self):
        self._dest_container.setVisible(self._move_cb.isChecked())
        self.adjustSize()

    def _accept(self):
        t = self._type_combo.currentData()

        if t == "pi_camera":
            device = self._device_combo.currentData()
            if not device:
                QMessageBox.warning(
                    self, "Pflichtfeld", "Bitte ein Gerät auswählen.")
                return
            dest = self._pi_dest_edit.text().strip()
            if not dest:
                QMessageBox.warning(
                    self, "Pflichtfeld", "Bitte ein Zielverzeichnis angeben.")
                return
            self._source.device_name = device
            self._source.destination_path = dest
            self._source.source_path = ""
            self._source.audio_path = ""
            self._source.move_to_destination = False
            self._source.name = device

        elif t == "local":
            is_folder = self._mode_folder_rb.isChecked()

            if is_folder:
                src = self._local_src_edit.text().strip()
                if not src:
                    QMessageBox.warning(
                        self, "Pflichtfeld",
                        "Bitte einen Quellordner angeben.")
                    return
                self._source.source_path = src
                self._source.audio_path = ""
                self._source.file_extensions = (
                    self._ext_edit.text().strip() or "*.mp4")
                self._source.name = Path(src).name or src
            else:
                video = self._video_file_edit.text().strip()
                if not video:
                    QMessageBox.warning(
                        self, "Pflichtfeld",
                        "Bitte eine Videodatei auswählen.")
                    return
                self._source.source_path = video
                self._source.audio_path = (
                    self._audio_file_edit.text().strip())
                self._source.file_extensions = ""
                self._source.name = Path(video).stem

            self._source.move_to_destination = self._move_cb.isChecked()
            if self._source.move_to_destination:
                dest = self._local_dest_edit.text().strip()
                if not dest:
                    QMessageBox.warning(
                        self, "Pflichtfeld",
                        "Bitte einen Zielordner angeben.")
                    return
                self._source.destination_path = dest
            else:
                if is_folder:
                    self._source.destination_path = (
                        self._local_src_edit.text().strip())
                else:
                    self._source.destination_path = str(
                        Path(self._video_file_edit.text().strip()).parent)
            self._source.device_name = ""

        self._source.source_type = t
        self._source.delete_source = self._delete_cb.isChecked()
        self.accept()

    def result_source(self) -> WorkflowSource:
        return self._source


# ═════════════════════════════════════════════════════════════════
#  Verarbeitungs-Editor (Modal-Dialog, per Doppelklick auf Seite 2)
# ═════════════════════════════════════════════════════════════════

class _ProcessingEditDialog(QDialog):
    """Modal-Dialog zum Bearbeiten der Verarbeitungseinstellungen einer Quelle.
    Wird per Doppelklick auf eine Zeile der Übersichtstabelle geöffnet."""

    def __init__(self, parent, source: WorkflowSource, settings: AppSettings):
        super().__init__(parent)
        self._source = source
        self._settings = settings

        icon = {"pi_camera": "📷", "local": "📁"}.get(source.source_type, "📄")
        self.setWindowTitle(f"Verarbeitung – {icon} {source.name}")
        self.setMinimumWidth(500)

        layout = QVBoxLayout(self)

        # ── Verarbeitung ─────────────────────────────────────
        proc_form = QFormLayout()
        proc_form.setLabelAlignment(Qt.AlignRight)

        self._merge_cb = QCheckBox("Audio + Video zusammenführen")
        self._merge_cb.setChecked(source.merge_audio_video)
        self._merge_cb.setToolTip(
            "Führt getrennte Video- und Audio-Dateien zusammen.\n"
            "Erkennt z. B. .mjpg + .wav, .mp4 + .wav usw.\n"
            "Bei Einzeldateien wird der explizit angegebene Audio-Pfad genutzt.")
        proc_form.addRow("", self._merge_cb)

        self._amplify_cb = QCheckBox("Audio verstärken (compand + loudnorm)")
        self._amplify_cb.setChecked(source.amplify_audio)
        proc_form.addRow("", self._amplify_cb)

        self._sync_cb = QCheckBox("Audio-Video-Sync (Frame-Drop-Korrektur)")
        self._sync_cb.setChecked(source.audio_sync)
        self._sync_cb.setToolTip(
            "Zählt Frames und passt FPS an Audio-Dauer an.\n"
            "Nötig bei Kameras mit Frame-Drops (z. B. Pi-Kameras).")
        proc_form.addRow("", self._sync_cb)

        # Einzeldatei ohne Audio → Merge/Sync sinnlos
        _is_file_without_audio = (
            source.source_type == "local"
            and not source.file_extensions
            and not source.audio_path)
        if _is_file_without_audio:
            _no_audio_tip = (
                "Nicht verfügbar: Keine separate Audio-Datei zugeordnet.\n"
                "Audiodatei im Quellen-Dialog (Seite 1) angeben.")
            for cb in (self._merge_cb, self._sync_cb):
                cb.setChecked(False)
                cb.setEnabled(False)
                cb.setToolTip(_no_audio_tip)
            self._amplify_cb.setToolTip(
                "Verstärkt die eingebettete Tonspur der Videodatei\n"
                "mit compand + loudnorm (ffmpeg Audio-Filter).")

        layout.addLayout(proc_form)

        # ── Profil-Schnellauswahl ────────────────────────────
        profile_bar = QHBoxLayout()
        profile_lbl = QLabel("Profil:")
        profile_lbl.setStyleSheet("font-weight: bold;")
        profile_bar.addWidget(profile_lbl)
        for pname in PROFILES:
            pbtn = QPushButton(pname)
            pbtn.setToolTip(f"Übernimmt Encoding-Werte von '{pname}'")
            pbtn.clicked.connect(
                lambda checked, n=pname: self._apply_profile(n))
            profile_bar.addWidget(pbtn)
        profile_bar.addStretch()
        layout.addLayout(profile_bar)

        # ── Encoding ─────────────────────────────────────────
        enc_group = QGroupBox("Encoding")
        enc_form = QFormLayout()

        self._encoder_combo = QComboBox()
        for enc_id, enc_name in available_encoder_choices():
            self._encoder_combo.addItem(enc_name, enc_id)
        idx = self._encoder_combo.findData(source.encoder)
        if idx >= 0:
            self._encoder_combo.setCurrentIndex(idx)
        enc_form.addRow("Encoder:", self._encoder_combo)

        self._preset_combo = QComboBox()
        self._preset_combo.addItems([
            "ultrafast", "superfast", "veryfast", "faster", "fast",
            "medium", "slow", "slower", "veryslow"])
        self._preset_combo.setCurrentText(source.preset)
        enc_form.addRow("Preset:", self._preset_combo)

        self._crf_spin = QSpinBox()
        self._crf_spin.setRange(0, 51)
        self._crf_spin.setValue(source.crf)
        crf_row = QHBoxLayout()
        crf_row.addWidget(self._crf_spin)
        crf_hint = QLabel("0=verlustfrei  18=gut  23=Standard")
        crf_hint.setStyleSheet("color: gray; font-size: 11px;")
        crf_row.addWidget(crf_hint)
        crf_row.addStretch()
        enc_form.addRow("CRF:", crf_row)

        self._fps_spin = QSpinBox()
        self._fps_spin.setRange(1, 120)
        self._fps_spin.setValue(source.fps)
        enc_form.addRow("FPS:", self._fps_spin)

        self._format_combo = QComboBox()
        self._format_combo.addItems(["mp4", "avi"])
        self._format_combo.setCurrentText(source.output_format)
        enc_form.addRow("Format:", self._format_combo)

        enc_group.setLayout(enc_form)
        layout.addWidget(enc_group)

        # ── Ausgabe ──────────────────────────────────────────
        out_form = QFormLayout()
        self._filename_edit = QLineEdit(source.output_filename)
        self._filename_edit.setPlaceholderText(
            "Leer = automatisch aus Quelldatei")
        self._filename_edit.setToolTip(
            "Dateiname für die erzeugte Datei (ohne Endung).\n"
            "Leer = wird automatisch aus dem Quelldateinamen abgeleitet.")
        out_form.addRow("Ausgabedatei:", self._filename_edit)
        layout.addLayout(out_form)

        # ── YouTube ──────────────────────────────────────────
        yt_group = QGroupBox("YouTube")
        yt_form = QFormLayout()

        self._yt_create_cb = QCheckBox(
            "YouTube-optimierte Version erstellen")
        self._yt_create_cb.setChecked(source.create_youtube)
        self._yt_create_cb.stateChanged.connect(self._on_yt_create_toggle)
        yt_form.addRow("", self._yt_create_cb)

        self._yt_upload_cb = QCheckBox("Auf YouTube hochladen")
        self._yt_upload_cb.setChecked(source.upload_youtube)
        self._yt_upload_cb.stateChanged.connect(self._on_yt_upload_toggle)
        yt_form.addRow("", self._yt_upload_cb)

        self._yt_title_edit = QLineEdit(source.youtube_title)
        self._yt_title_edit.setMaxLength(100)
        self._yt_title_edit.setPlaceholderText(
            "YouTube-Titel (leer = Dateiname)")
        yt_form.addRow("Titel:", self._yt_title_edit)

        self._yt_playlist_edit = QLineEdit(source.youtube_playlist)
        self._yt_playlist_edit.setPlaceholderText(
            "Playlist-Name (wird automatisch angelegt)")
        yt_form.addRow("Playlist:", self._yt_playlist_edit)

        yt_group.setLayout(yt_form)
        layout.addWidget(yt_group)

        self._on_yt_create_toggle()

        # ── Buttons ──────────────────────────────────────────
        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._apply_and_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _apply_profile(self, profile_name: str):
        """Wendet ein Encoding-Profil auf die Felder dieses Dialogs an."""
        values = PROFILES.get(profile_name, {})
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

    def _on_yt_create_toggle(self):
        """Upload-Checkbox nur aktiv wenn YouTube-Version erstellt wird."""
        can_upload = self._yt_create_cb.isChecked()
        self._yt_upload_cb.setEnabled(can_upload)
        if not can_upload:
            self._yt_upload_cb.setChecked(False)
        self._on_yt_upload_toggle()

    def _on_yt_upload_toggle(self):
        """Titel/Playlist nur aktiv wenn Upload aktiviert ist."""
        enabled = self._yt_upload_cb.isChecked()
        self._yt_title_edit.setEnabled(enabled)
        self._yt_playlist_edit.setEnabled(enabled)

    def _apply_and_accept(self):
        """Schreibt die GUI-Werte zurück in die WorkflowSource und schließt."""
        s = self._source
        s.merge_audio_video = self._merge_cb.isChecked()
        s.amplify_audio = self._amplify_cb.isChecked()
        s.audio_sync = self._sync_cb.isChecked()
        s.encoder = self._encoder_combo.currentData()
        s.preset = self._preset_combo.currentText()
        s.crf = self._crf_spin.value()
        s.fps = self._fps_spin.value()
        s.output_format = self._format_combo.currentText()
        s.output_filename = self._filename_edit.text().strip()
        s.create_youtube = self._yt_create_cb.isChecked()
        s.upload_youtube = self._yt_upload_cb.isChecked()
        s.youtube_title = self._yt_title_edit.text().strip()
        s.youtube_playlist = self._yt_playlist_edit.text().strip()
        self.accept()


# ═════════════════════════════════════════════════════════════════
#  Profil-Schnellauswahl (auf Seite 2 oben)
# ═════════════════════════════════════════════════════════════════

class _ProfileQuickBar(QWidget):
    """Leiste oben auf Seite 2: Profil auf alle Quellen anwenden."""

    profile_applied = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        lbl = QLabel("Schnell-Profil für alle:")
        lbl.setStyleSheet("font-weight: bold;")
        layout.addWidget(lbl)

        for name in PROFILES:
            btn = QPushButton(name)
            btn.setToolTip(
                f"Wendet das Profil '{name}' auf alle Quellen an")
            btn.clicked.connect(lambda checked, n=name: self._apply(n))
            layout.addWidget(btn)

        layout.addStretch()

    def _apply(self, profile_name: str):
        self.profile_applied.emit(profile_name)


# ═════════════════════════════════════════════════════════════════
#  Hauptdialog: Workflow-Wizard
# ═════════════════════════════════════════════════════════════════

class WorkflowWizard(QDialog):
    """Zwei-Etappen-Wizard zum Zusammenstellen eines Workflows.

    Seite 1: Quellen (Kameras, lokale Quellen)
    Seite 2: Verarbeitung pro Quelle (Encoding, YouTube, etc.)
    """

    def __init__(self, parent, settings: AppSettings,
                 workflow: Workflow | None = None):
        super().__init__(parent)
        self.setWindowTitle("Workflow-Assistent")
        self.resize(820, 700)
        self.setMinimumSize(700, 500)

        self._settings = settings
        self._workflow = workflow or Workflow()
        self._active_sources: list[WorkflowSource] = []

        self._build_ui()
        self._update_buttons()

    @property
    def workflow(self) -> Workflow:
        return self._workflow

    # ── UI aufbauen ──────────────────────────────────────────

    def _build_ui(self):
        main_layout = QVBoxLayout(self)

        # ── Seitentitel ──────────────────────────────────────
        self._title_label = QLabel()
        self._title_label.setFont(QFont("", 14, QFont.Bold))
        self._title_label.setStyleSheet("margin-bottom: 4px;")
        main_layout.addWidget(self._title_label)

        self._subtitle_label = QLabel()
        self._subtitle_label.setStyleSheet("color: gray; margin-bottom: 8px;")
        self._subtitle_label.setWordWrap(True)
        main_layout.addWidget(self._subtitle_label)

        # ── Stacked Widget (Seiten) ──────────────────────────
        self._stack = QStackedWidget()
        self._stack.addWidget(self._build_page_sources())
        self._stack.addWidget(self._build_page_processing())
        main_layout.addWidget(self._stack, stretch=1)

        # ── Globale Optionen ─────────────────────────────────
        self._global_group = QGroupBox("Globale Optionen")
        g_layout = QHBoxLayout()

        self._shutdown_cb = QCheckBox("Rechner nach Abschluss herunterfahren")
        self._shutdown_cb.setChecked(self._workflow.shutdown_after)
        g_layout.addWidget(self._shutdown_cb)

        g_layout.addStretch()

        self._global_group.setLayout(g_layout)
        main_layout.addWidget(self._global_group)
        self._global_group.setVisible(False)  # Erst auf Seite 2

        # ── Navigations-Buttons ──────────────────────────────
        btn_layout = QHBoxLayout()

        # Workflow laden/speichern
        self._load_btn = QPushButton("Workflow laden …")
        self._load_btn.clicked.connect(self._load_workflow)
        btn_layout.addWidget(self._load_btn)

        self._save_wf_btn = QPushButton("Workflow speichern …")
        self._save_wf_btn.clicked.connect(self._save_workflow)
        btn_layout.addWidget(self._save_wf_btn)

        btn_layout.addStretch()

        self._back_btn = QPushButton("← Zurück")
        self._back_btn.clicked.connect(self._go_back)
        btn_layout.addWidget(self._back_btn)

        self._next_btn = QPushButton("Weiter →")
        self._next_btn.clicked.connect(self._go_next)
        btn_layout.addWidget(self._next_btn)

        self._start_btn = QPushButton("▶  Workflow starten")
        self._start_btn.setStyleSheet(
            "QPushButton { background-color: #2d8d46; color: white; "
            "font-weight: bold; padding: 6px 16px; }")
        self._start_btn.clicked.connect(self._finish)
        btn_layout.addWidget(self._start_btn)

        cancel_btn = QPushButton("Abbrechen")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(cancel_btn)

        main_layout.addLayout(btn_layout)

        self._set_page(0)

    # ── Seite 1: Quellen ─────────────────────────────────────

    def _build_page_sources(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)

        # Quellen-Tabelle
        self._src_table = QTableWidget(0, 5)
        self._src_table.setHorizontalHeaderLabels(
            ["✓", "Name", "Typ", "Quelle / Gerät", "Ziel"])
        hdr = self._src_table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.Fixed)
        hdr.resizeSection(0, 30)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(3, QHeaderView.Stretch)
        hdr.setSectionResizeMode(4, QHeaderView.Stretch)
        self._src_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._src_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._src_table.verticalHeader().setVisible(False)
        self._src_table.setAlternatingRowColors(True)
        self._src_table.doubleClicked.connect(self._edit_source)
        layout.addWidget(self._src_table)

        # Buttons unter der Tabelle
        btn_row = QHBoxLayout()
        add_btn = QPushButton("＋ Quelle hinzufügen")
        add_btn.setToolTip("Neue Quelle hinzufügen (Pi-Kamera oder lokale Quelle)")
        add_btn.clicked.connect(self._add_source)
        btn_row.addWidget(add_btn)

        btn_row.addStretch()

        add_all_btn = QPushButton("Alle konfigurierten Kameras")
        add_all_btn.setToolTip(
            "Fügt alle in den Einstellungen konfigurierten\n"
            "Pi-Kameras als Quellen hinzu")
        add_all_btn.clicked.connect(self._add_all_cameras)
        btn_row.addWidget(add_all_btn)

        btn_row.addStretch()

        edit_btn = QPushButton("Bearbeiten")
        edit_btn.clicked.connect(self._edit_source)
        btn_row.addWidget(edit_btn)

        remove_btn = QPushButton("Entfernen")
        remove_btn.clicked.connect(self._remove_source)
        btn_row.addWidget(remove_btn)

        layout.addLayout(btn_row)

        # Bestehende Quellen anzeigen
        self._refresh_source_table()

        return page

    # ── Seite 2: Verarbeitung ────────────────────────────────

    def _build_page_processing(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)

        # Profil-Schnellauswahl
        self._profile_bar = _ProfileQuickBar()
        self._profile_bar.profile_applied.connect(
            self._apply_profile_to_all)
        layout.addWidget(self._profile_bar)

        # Kompakte Übersichtstabelle
        self._proc_table = QTableWidget(0, 5)
        self._proc_table.setHorizontalHeaderLabels(
            ["Name", "Encoding", "Audio", "YouTube", "Ausgabe"])
        hdr = self._proc_table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(1, QHeaderView.Stretch)
        hdr.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(4, QHeaderView.Stretch)
        self._proc_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._proc_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._proc_table.verticalHeader().setVisible(False)
        self._proc_table.setAlternatingRowColors(True)
        self._proc_table.doubleClicked.connect(self._edit_processing)
        layout.addWidget(self._proc_table)

        # Hinweis + Bearbeiten-Button
        hint_row = QHBoxLayout()
        hint = QLabel("💡 Doppelklick auf eine Zeile zum Bearbeiten")
        hint.setStyleSheet("color: gray; font-size: 11px;")
        hint_row.addWidget(hint)
        hint_row.addStretch()
        edit_proc_btn = QPushButton("Bearbeiten")
        edit_proc_btn.clicked.connect(self._edit_processing)
        hint_row.addWidget(edit_proc_btn)
        layout.addLayout(hint_row)

        return page

    def _refresh_processing_table(self):
        """Füllt die Verarbeitungs-Tabelle mit kompakten Zusammenfassungen."""
        active = [s for s in self._workflow.sources if s.enabled]
        self._active_sources = active
        self._proc_table.setRowCount(len(active))

        type_icons = {"pi_camera": "📷", "local": "📁"}

        for row, src in enumerate(active):
            icon = type_icons.get(src.source_type, "📄")

            # Name
            self._proc_table.setItem(
                row, 0, QTableWidgetItem(f"{icon} {src.name}"))

            # Encoding-Zusammenfassung
            enc_name = encoder_display_name(src.encoder)
            enc_text = f"{enc_name} · CRF {src.crf} · {src.preset}"
            self._proc_table.setItem(
                row, 1, QTableWidgetItem(enc_text))

            # Audio-Zusammenfassung
            audio_parts = []
            if src.merge_audio_video:
                audio_parts.append("Merge")
            if src.amplify_audio:
                audio_parts.append("Verstärken")
            if src.audio_sync:
                audio_parts.append("Sync")
            self._proc_table.setItem(
                row, 2, QTableWidgetItem(
                    ", ".join(audio_parts) if audio_parts else "—"))

            # YouTube-Zusammenfassung
            yt_parts = []
            if src.create_youtube:
                yt_parts.append("Erstellen")
            if src.upload_youtube:
                yt_parts.append("Upload")
            self._proc_table.setItem(
                row, 3, QTableWidgetItem(
                    " + ".join(yt_parts) if yt_parts else "—"))

            # Ausgabe
            self._proc_table.setItem(
                row, 4, QTableWidgetItem(
                    src.output_filename or "(auto)"))

    def _edit_processing(self):
        """Verarbeitungs-Dialog für die ausgewählte Zeile öffnen."""
        row = self._proc_table.currentRow()
        if row < 0 or row >= len(self._active_sources):
            return
        src = self._active_sources[row]
        dlg = _ProcessingEditDialog(self, src, self._settings)
        if dlg.exec():
            self._refresh_processing_table()

    # ── Navigation ───────────────────────────────────────────

    def _set_page(self, index: int):
        self._stack.setCurrentIndex(index)

        if index == 0:
            self._title_label.setText("Etappe 1: Quellen")
            self._subtitle_label.setText(
                "Wo liegen die Aufnahmen? Füge Pi-Kameras "
                "oder lokale Quellen hinzu.")
            self._global_group.setVisible(False)
        elif index == 1:
            self._title_label.setText("Etappe 2: Verarbeitung")
            self._subtitle_label.setText(
                "Lege für jede Quelle fest, wie die Dateien verarbeitet "
                "werden sollen. Doppelklick zum Bearbeiten.")
            self._global_group.setVisible(True)
            self._refresh_processing_table()

        self._update_buttons()

    def _update_buttons(self):
        page = self._stack.currentIndex()
        self._back_btn.setVisible(page > 0)
        self._next_btn.setVisible(page == 0)
        self._start_btn.setVisible(page == 1)
        self._load_btn.setVisible(True)
        self._save_wf_btn.setVisible(True)

    def _go_next(self):
        if self._stack.currentIndex() == 0:
            # Validierung: Mindestens eine aktive Quelle?
            active = [s for s in self._workflow.sources if s.enabled]
            if not active:
                QMessageBox.warning(
                    self, "Keine Quellen",
                    "Bitte mindestens eine Quelle hinzufügen.")
                return
            self._set_page(1)

    def _go_back(self):
        if self._stack.currentIndex() == 1:
            # Panels-Daten sichern
            self._collect_processing_data()
            self._set_page(0)

    def _finish(self):
        """Workflow abschließen und starten."""
        self._collect_processing_data()
        self._workflow.shutdown_after = self._shutdown_cb.isChecked()

        active = [s for s in self._workflow.sources if s.enabled]
        if not active:
            QMessageBox.warning(
                self, "Keine Quellen",
                "Keine aktiven Quellen im Workflow.")
            return

        # Zusammenfassung anzeigen
        summary_lines = [f"<b>{len(active)} Quelle(n):</b><ul>"]
        for s in active:
            parts = [s.name]
            if s.create_youtube:
                parts.append("YouTube")
            if s.upload_youtube:
                parts.append("Upload")
            summary_lines.append(
                f"<li>{' – '.join(parts)}</li>")
        summary_lines.append("</ul>")
        if self._workflow.shutdown_after:
            summary_lines.append(
                "<b>⚠ Der Rechner wird nach Abschluss "
                "heruntergefahren!</b>")

        result = QMessageBox.question(
            self, "Workflow starten?",
            "".join(summary_lines),
            QMessageBox.Yes | QMessageBox.No)
        if result == QMessageBox.Yes:
            # Workflow als letzten speichern
            self._workflow.save_as_last()
            self.accept()

    def _collect_processing_data(self):
        """Daten sind bereits in den WorkflowSource-Objekten (vom Dialog)."""
        pass

    # ── Quellen-Verwaltung ───────────────────────────────────

    def _refresh_source_table(self):
        sources = self._workflow.sources
        self._src_table.setRowCount(len(sources))

        type_labels = {
            "pi_camera": "📷 Pi-Kamera",
            "local": "📁 Lokal",
        }

        for row, src in enumerate(sources):
            # Checkbox
            chk_item = QTableWidgetItem()
            chk_item.setFlags(
                Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
            chk_item.setCheckState(
                Qt.Checked if src.enabled else Qt.Unchecked)
            self._src_table.setItem(row, 0, chk_item)

            self._src_table.setItem(
                row, 1, QTableWidgetItem(src.name))
            self._src_table.setItem(
                row, 2, QTableWidgetItem(
                    type_labels.get(src.source_type, src.source_type)))

            # Quelle
            if src.source_type == "pi_camera":
                source_text = src.device_name
            else:
                source_text = src.source_path
            self._src_table.setItem(
                row, 3, QTableWidgetItem(source_text))

            self._src_table.setItem(
                row, 4, QTableWidgetItem(src.destination_path))

        # Checkbox-Änderungen tracken
        self._src_table.itemChanged.connect(self._on_source_check_changed)

    def _on_source_check_changed(self, item: QTableWidgetItem):
        if item.column() == 0:
            row = item.row()
            if 0 <= row < len(self._workflow.sources):
                self._workflow.sources[row].enabled = (
                    item.checkState() == Qt.Checked)

    def _add_source(self):
        src = WorkflowSource()
        # Defaults aus globalen Settings übernehmen
        src.encoder = self._settings.video.encoder
        src.crf = self._settings.video.crf
        src.preset = self._settings.video.preset
        src.fps = self._settings.video.fps
        src.amplify_audio = self._settings.audio.amplify_audio

        dlg = _SourceEditDialog(self, self._settings, src)
        if dlg.exec():
            result = dlg.result_source()
            # Typ-spezifische Defaults nach Dialog-Wahl setzen
            if result.source_type == "pi_camera":
                if not result.destination_path:
                    result.destination_path = self._settings.cameras.destination
                result.merge_audio_video = True
                result.audio_sync = True
            self._workflow.sources.append(result)
            self._refresh_source_table()

    def _add_all_cameras(self):
        """Fügt alle konfigurierten Pi-Kameras als Quellen hinzu."""
        devices = self._settings.cameras.devices
        if not devices:
            QMessageBox.information(
                self, "Keine Kameras",
                "Es sind keine Pi-Kameras konfiguriert.\n"
                "Bitte zuerst unter Einstellungen → Kameras Geräte anlegen.")
            return

        existing = {s.device_name for s in self._workflow.sources
                    if s.source_type == "pi_camera"}
        added = 0
        for dev in devices:
            if dev.name not in existing:
                src = WorkflowSource(
                    source_type="pi_camera",
                    name=dev.name,
                    device_name=dev.name,
                    destination_path=self._settings.cameras.destination,
                    delete_source=self._settings.cameras.delete_after_download,
                    merge_audio_video=True,
                    audio_sync=self._settings.video.audio_sync,
                    encoder=self._settings.video.encoder,
                    crf=self._settings.video.crf,
                    preset=self._settings.video.preset,
                    fps=self._settings.video.fps,
                    amplify_audio=self._settings.audio.amplify_audio,
                )
                self._workflow.sources.append(src)
                added += 1

        self._refresh_source_table()
        if added:
            QMessageBox.information(
                self, "Kameras hinzugefügt",
                f"{added} Pi-Kamera(s) hinzugefügt.")
        else:
            QMessageBox.information(
                self, "Hinweis",
                "Alle konfigurierten Kameras sind bereits als Quellen vorhanden.")

    def _edit_source(self):
        row = self._src_table.currentRow()
        if row < 0 or row >= len(self._workflow.sources):
            return
        src = self._workflow.sources[row]
        dlg = _SourceEditDialog(self, self._settings, src)
        if dlg.exec():
            self._workflow.sources[row] = dlg.result_source()
            self._refresh_source_table()

    def _remove_source(self):
        rows = sorted(
            {idx.row() for idx in self._src_table.selectedIndexes()},
            reverse=True)
        for r in rows:
            if 0 <= r < len(self._workflow.sources):
                del self._workflow.sources[r]
        self._refresh_source_table()

    # ── Profil auf alle anwenden ─────────────────────────────

    def _apply_profile_to_all(self, profile_name: str):
        """Wendet ein Profil auf alle aktiven Quellen an."""
        values = PROFILES.get(profile_name, {})
        if not values:
            return
        for src in self._workflow.sources:
            if not src.enabled:
                continue
            if "encoder" in values:
                src.encoder = values["encoder"]
            if "preset" in values:
                src.preset = values["preset"]
            if "crf" in values:
                src.crf = values["crf"]
            if "output_format" in values:
                src.output_format = values["output_format"]
        self._refresh_processing_table()

    # ── Workflow laden / speichern ────────────────────────────

    def _save_workflow(self):
        self._collect_processing_data()
        self._workflow.shutdown_after = self._shutdown_cb.isChecked()

        WORKFLOW_DIR.mkdir(parents=True, exist_ok=True)
        default_name = self._workflow.name or "workflow"
        path, _ = QFileDialog.getSaveFileName(
            self, "Workflow speichern",
            str(WORKFLOW_DIR / f"{default_name}.json"),
            "JSON-Dateien (*.json)")
        if path:
            p = Path(path)
            self._workflow.name = p.stem
            self._workflow.save(p)
            QMessageBox.information(
                self, "Gespeichert",
                f"Workflow gespeichert: {p.name}")

    def _load_workflow(self):
        WORKFLOW_DIR.mkdir(parents=True, exist_ok=True)
        path, _ = QFileDialog.getOpenFileName(
            self, "Workflow laden",
            str(WORKFLOW_DIR),
            "JSON-Dateien (*.json)")
        if path:
            try:
                wf = Workflow.load(Path(path))
                self._workflow = wf
                self._shutdown_cb.setChecked(wf.shutdown_after)
                self._refresh_source_table()
                if self._stack.currentIndex() == 1:
                    self._refresh_processing_table()
            except Exception as e:
                QMessageBox.critical(
                    self, "Ladefehler",
                    f"Workflow konnte nicht geladen werden:\n{e}")
