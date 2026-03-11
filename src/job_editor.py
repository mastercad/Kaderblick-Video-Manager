"""Dialog zum Anlegen und Bearbeiten eines einzelnen Workflow-Auftrags.

Alles in einem Dialog – kein mehrstufiger Wizard:
  • Quelle  (direkte Dateien | Pi-Kamera | Ordner)
  • Verarbeitung  (Encoding, optional)
  • Audio  (Merge, Verstärken, Sync – optional)
  • YouTube  (Erstellen, Hochladen, Titel, Playlist)

Die Sektion „Verarbeitung" kann komplett deaktiviert werden;
dann werden die Dateien unverändert verwendet (z. B. nur umbenennen
oder direkt auf YouTube hochladen).
"""

from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout,
    QGroupBox, QLabel, QPushButton, QDialogButtonBox,
    QCheckBox, QComboBox, QSpinBox, QDoubleSpinBox,
    QLineEdit, QFileDialog, QMessageBox, QRadioButton,
    QButtonGroup, QScrollArea, QWidget, QFrame,
)

from .settings import AppSettings, PROFILES
from .encoder import available_encoder_choices, encoder_display_name
from .file_list_widget import FileListWidget
from .workflow import WorkflowJob, FileEntry


# ─────────────────────────────────────────────────────────────────
#  Pipeline-Vorlagen
# ─────────────────────────────────────────────────────────────────

PIPELINE_TEMPLATES: dict[str, dict] = {
    "Benutzerdefiniert": {},
    "Pi-Kamera  →  Konvertieren": {
        "source_mode":      "pi_download",
        "convert_enabled":  True,
        "merge_audio":      True,
        "audio_sync":       True,
        "amplify_audio":    False,
        "upload_youtube":   False,
        "create_youtube_version": False,
    },
    "Pi-Kamera  →  Konvertieren  →  YouTube": {
        "source_mode":      "pi_download",
        "convert_enabled":  True,
        "merge_audio":      True,
        "audio_sync":       True,
        "amplify_audio":    False,
        "create_youtube_version": True,
        "upload_youtube":   True,
    },
    "Ordner  →  Konvertieren": {
        "source_mode":      "folder_scan",
        "convert_enabled":  True,
        "merge_audio":      False,
        "audio_sync":       False,
        "upload_youtube":   False,
        "create_youtube_version": False,
    },
    "Ordner  →  Konvertieren  →  YouTube": {
        "source_mode":      "folder_scan",
        "convert_enabled":  True,
        "merge_audio":      False,
        "audio_sync":       False,
        "create_youtube_version": True,
        "upload_youtube":   True,
    },
    "Dateien  →  YouTube hochladen": {
        "source_mode":      "files",
        "convert_enabled":  False,
        "upload_youtube":   True,
        "create_youtube_version": False,
    },
    "Dateien  →  Konvertieren  →  YouTube": {
        "source_mode":      "files",
        "convert_enabled":  True,
        "merge_audio":      False,
        "audio_sync":       False,
        "create_youtube_version": True,
        "upload_youtube":   True,
    },
}


class JobEditorDialog(QDialog):
    """Dialog zum Anlegen / Bearbeiten eines einzelnen WorkflowJob."""

    def __init__(self, parent, settings: AppSettings,
                 job: WorkflowJob | None = None):
        super().__init__(parent)
        self._settings = settings
        self._job = job or self._create_default_job(settings)
        self._is_new = job is None
        self.setWindowTitle(
            "Neuer Auftrag" if self._is_new else f"Auftrag bearbeiten – {self._job.name}")
        self.setMinimumWidth(600)
        self.setMinimumHeight(500)
        self._build_ui()
        self._populate_from_job()
        self._update_visibility()

    # ── Öffentliche Schnittstelle ─────────────────────────────

    @property
    def result_job(self) -> WorkflowJob:
        """Gibt den bearbeiteten/neuen Auftrag zurück."""
        return self._job

    # ── Standardwerte ─────────────────────────────────────────

    @staticmethod
    def _create_default_job(settings: AppSettings) -> WorkflowJob:
        job = WorkflowJob()
        job.encoder = settings.video.encoder
        job.crf = settings.video.crf
        job.preset = settings.video.preset
        job.fps = settings.video.fps
        job.output_format = settings.video.output_format
        job.amplify_audio = settings.audio.amplify_audio
        job.amplify_db = settings.audio.amplify_db
        return job

    # ── UI aufbauen ───────────────────────────────────────────

    def _build_ui(self) -> None:
        outer = QVBoxLayout(self)
        outer.setSpacing(8)

        # ── Pipeline-Vorlage ──────────────────────────────────
        if self._is_new:
            tpl_row = QHBoxLayout()
            tpl_lbl = QLabel("Pipeline-Vorlage:")
            tpl_lbl.setStyleSheet("font-weight: bold;")
            tpl_row.addWidget(tpl_lbl)
            self._template_combo = QComboBox()
            for name in PIPELINE_TEMPLATES:
                self._template_combo.addItem(name)
            self._template_combo.setToolTip(
                "Wähle eine Vorlage – sie befüllt alle Felder automatisch.\n"
                "Du kannst danach alles anpassen.")
            self._template_combo.currentTextChanged.connect(self._apply_template)
            tpl_row.addWidget(self._template_combo, stretch=1)
            outer.addLayout(tpl_row)

            sep = QFrame()
            sep.setFrameShape(QFrame.HLine)
            sep.setStyleSheet("color: #ccc;")
            outer.addWidget(sep)

        # Scrollbarer Bereich für den Formularinhalt
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        inner = QWidget()
        self._form_layout = QVBoxLayout(inner)
        self._form_layout.setSpacing(10)
        scroll.setWidget(inner)
        outer.addWidget(scroll, stretch=1)

        self._build_section_source()
        self._build_section_processing()
        self._build_section_audio()
        self._build_section_youtube()

        # Button-Leiste außerhalb des Scroll-Bereichs
        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Ok).setText("Übernehmen")
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._validate_and_accept)
        buttons.rejected.connect(self.reject)
        outer.addWidget(buttons)

    # ── Sektion: Quelle ───────────────────────────────────────

    def _build_section_source(self) -> None:
        group = QGroupBox("Quelle")
        layout = QVBoxLayout(group)

        # Auftrag-Name
        name_row = QFormLayout()
        self._name_edit = QLineEdit()
        self._name_edit.setPlaceholderText("Kurzbezeichnung dieses Auftrags")
        self._name_edit.setToolTip(
            "Optionaler Name für diesen Auftrag – dient nur zur Anzeige in der Jobliste.\n"
            "Wird leer gelassen, wird er automatisch generiert:\n"
            "  • Dateien → Dateiname der ersten Datei\n"
            "  • Ordner scannen → Name des Quellordners\n"
            "  • Pi-Kamera → Name des Geräts")
        name_row.addRow("Name:", self._name_edit)
        layout.addLayout(name_row)

        # Modus-Auswahl
        mode_row = QHBoxLayout()
        self._mode_files_rb  = QRadioButton("Dateien auswählen")
        self._mode_files_rb.setToolTip(
            "Einzelne Dateien direkt auswählen.\n"
            "Jede Datei kann einen eigenen Ausgabename, YT-Titel und Playlist erhalten.")
        self._mode_folder_rb = QRadioButton("Ordner scannen")
        self._mode_folder_rb.setToolTip(
            "Alle Dateien in einem Ordner verarbeiten, die dem Datei-Muster entsprechen.\n"
            "Neue Dateien werden beim nächsten Start automatisch erkannt.")
        self._mode_pi_rb     = QRadioButton("Pi-Kamera herunterladen")
        self._mode_pi_rb.setToolTip(
            "Lädt Aufnahmen von einer konfigurierten Raspberry-Pi-Kamera herunter\n"
            "und verarbeitet sie anschließend.")
        self._mode_group = QButtonGroup(self)
        self._mode_group.addButton(self._mode_files_rb,  0)
        self._mode_group.addButton(self._mode_folder_rb, 1)
        self._mode_group.addButton(self._mode_pi_rb,     2)
        mode_row.addWidget(self._mode_files_rb)
        mode_row.addWidget(self._mode_folder_rb)
        mode_row.addWidget(self._mode_pi_rb)
        mode_row.addStretch()
        layout.addLayout(mode_row)

        # ── Bereich: Direkte Dateiliste ─────────────────────
        self._files_panel = QWidget()
        files_layout = QVBoxLayout(self._files_panel)
        files_layout.setContentsMargins(0, 0, 0, 0)
        self._file_list = FileListWidget(
            last_dir_getter=lambda: self._settings.last_directory,
            last_dir_setter=self._save_last_dir,
        )
        files_layout.addWidget(self._file_list)
        layout.addWidget(self._files_panel)

        # ── Bereich: Ordner scannen ─────────────────────────
        self._folder_panel = QWidget()
        folder_form = QFormLayout(self._folder_panel)
        folder_form.setContentsMargins(0, 4, 0, 0)

        # Quellordner
        self._folder_src_edit = QLineEdit()
        self._folder_src_edit.setPlaceholderText("Quellordner …")
        self._folder_src_edit.setToolTip(
            "Ordner, in dem nach passenden Dateien gesucht wird.\n"
            "Es werden nur Dateien im Hauptordner geprüft (kein Rekursiv-Scan).")
        folder_browse_btn = QPushButton("…")
        folder_browse_btn.setFixedWidth(32)
        folder_browse_btn.clicked.connect(
            lambda: self._browse_dir(self._folder_src_edit, "Quellordner wählen"))
        src_row = QHBoxLayout()
        src_row.addWidget(self._folder_src_edit)
        src_row.addWidget(folder_browse_btn)
        folder_form.addRow("Quellordner:", src_row)

        self._file_pattern_edit = QLineEdit()
        self._file_pattern_edit.setPlaceholderText("*.mp4")
        self._file_pattern_edit.setToolTip(
            "Glob-Muster für Dateien, z. B. *.mp4  *.mjpg  *.MP4")
        folder_form.addRow("Datei-Muster:", self._file_pattern_edit)

        # Zielordner (optional)
        self._folder_dst_edit = QLineEdit()
        self._folder_dst_edit.setPlaceholderText(
            "leer = Dateien direkt am Quellort verarbeiten")
        self._folder_dst_edit.setToolTip(
            "Optionaler Ausgabeordner für konvertierte Dateien.\n"
            "Bleibt das Feld leer, wird die Ausgabedatei neben der Quelldatei gespeichert.")
        dst_row = QHBoxLayout()
        dst_row.addWidget(self._folder_dst_edit)
        folder_dst_btn = QPushButton("…")
        folder_dst_btn.setFixedWidth(32)
        folder_dst_btn.clicked.connect(
            lambda: self._browse_dir(self._folder_dst_edit, "Zielordner wählen"))
        dst_row.addWidget(folder_dst_btn)
        folder_form.addRow("Zielordner:", dst_row)

        self._move_files_cb = QCheckBox("Dateien verschieben (statt am Quellort lassen)")
        self._move_files_cb.setToolTip(
            "Verschiebt die Quelldateien nach erfolgreicher Verarbeitung in den Zielordner.\n"
            "Ohne diese Option bleiben die Originaldateien an ihrem Ort.")
        folder_form.addRow("", self._move_files_cb)

        # Ausgabe-Präfix (optional)
        folder_form.addRow(QLabel(""))  # Abstand
        self._folder_prefix_edit = QLineEdit()
        self._folder_prefix_edit.setPlaceholderText("leer = Originaldateiname behalten")
        self._folder_prefix_edit.setToolTip(
            "Optionaler Präfix für umbenannte Ausgabedateien.\n"
            "Beispiel: 'Spiel_2024-03-10_' → Ausgabe heißt 'Spiel_2024-03-10_001.mp4', '…_002.mp4' usw.\n"
            "Bleibt das Feld leer, wird der Originaldateiname beibehalten.")
        folder_form.addRow("Ausgabe-Präfix:", self._folder_prefix_edit)

        layout.addWidget(self._folder_panel)

        # ── Bereich: Pi-Kamera ──────────────────────────────
        self._pi_panel = QWidget()
        pi_form = QFormLayout(self._pi_panel)
        pi_form.setContentsMargins(0, 4, 0, 0)

        self._device_combo = QComboBox()
        self._device_combo.addItem("(Gerät wählen)", "")
        for dev in self._settings.cameras.devices:
            self._device_combo.addItem(f"{dev.name}  ({dev.ip})", dev.name)
        self._device_combo.setToolTip(
            "Wähle die Raspberry-Pi-Kamera aus, von der Aufnahmen heruntergeladen werden sollen.\n"
            "Kameras können unter Einstellungen → Kameras angelegt werden.")
        pi_form.addRow("Gerät:", self._device_combo)

        self._pi_dest_edit = QLineEdit()
        self._pi_dest_edit.setPlaceholderText("Lokales Zielverzeichnis …")
        self._pi_dest_edit.setToolTip(
            "Lokaler Ordner, in den die heruntergeladenen Dateien gespeichert werden.\n"
            "Falls leer, wird das in den Kamera-Einstellungen hinterlegte Verzeichnis verwendet.")
        pi_dest_row = QHBoxLayout()
        pi_dest_row.addWidget(self._pi_dest_edit)
        pi_dest_btn = QPushButton("…")
        pi_dest_btn.setFixedWidth(32)
        pi_dest_btn.clicked.connect(
            lambda: self._browse_dir(self._pi_dest_edit, "Zielverzeichnis wählen"))
        pi_dest_row.addWidget(pi_dest_btn)
        pi_form.addRow("Zielverzeichnis:", pi_dest_row)

        self._delete_after_dl_cb = QCheckBox(
            "Aufnahmen nach erfolgreichem Download löschen")
        self._delete_after_dl_cb.setToolTip(
            "Löscht die Aufnahmen auf der Kamera nach erfolgreichem Download.\n"
            "Achtung: Diese Aktion kann nicht rückgängig gemacht werden!")
        pi_form.addRow("", self._delete_after_dl_cb)

        # Ausgabe-Präfix (optional)
        pi_form.addRow(QLabel(""))
        self._pi_prefix_edit = QLineEdit()
        self._pi_prefix_edit.setPlaceholderText("leer = Originaldateiname behalten")
        self._pi_prefix_edit.setToolTip(
            "Optionaler Präfix für umbenannte Ausgabedateien.\n"
            "Nützlich wenn die Kamera-Dateien automatisch 'aufnahme_<Timestamp>' heißen.\n"
            "Beispiel: 'Heimspiel_10.03_' → Ausgabe heißt 'Heimspiel_10.03_001.mp4', '…_002.mp4' usw.\n"
            "Bleibt das Feld leer, wird der Originaldateiname beibehalten.")
        pi_form.addRow("Ausgabe-Präfix:", self._pi_prefix_edit)

        layout.addWidget(self._pi_panel)

        # Signale verbinden
        self._mode_group.idToggled.connect(
            lambda _id, checked: self._update_visibility() if checked else None)

        self._form_layout.addWidget(group)

    # ── Sektion: Verarbeitung ─────────────────────────────────

    def _build_section_processing(self) -> None:
        group = QGroupBox("Verarbeitung")
        layout = QVBoxLayout(group)

        # Master-Schalter
        self._convert_enabled_cb = QCheckBox("Konvertierung aktiv")
        self._convert_enabled_cb.setToolTip(
            "Wenn deaktiviert, werden die Quelldateien unverändert verwendet.\n"
            "YouTube-Upload und Umbenennung sind weiterhin möglich.")
        self._convert_enabled_cb.toggled.connect(self._update_visibility)
        layout.addWidget(self._convert_enabled_cb)

        # Profil-Schnellauswahl
        profile_row = QHBoxLayout()
        profile_row.addWidget(QLabel("Schnell-Profil:"))
        for pname in PROFILES:
            btn = QPushButton(pname)
            btn.setToolTip(f"Übernimmt Encoding-Werte von Profil '{pname}'")
            btn.setFlat(True)
            btn.setStyleSheet("QPushButton { text-decoration: underline; color: #3a7bde; border: none; }")
            btn.clicked.connect(lambda _checked, n=pname: self._apply_profile(n))
            profile_row.addWidget(btn)
        profile_row.addStretch()

        # Das eigentliche Encoding-Formular
        self._encoding_widget = QWidget()
        enc_form = QFormLayout(self._encoding_widget)
        enc_form.setContentsMargins(0, 0, 0, 0)

        self._encoder_combo = QComboBox()
        for enc_id, enc_name in available_encoder_choices():
            self._encoder_combo.addItem(enc_name, enc_id)
        self._encoder_combo.setToolTip(
            "Video-Codec für die Konvertierung.\n"
            "H.264 (libx264): beste Kompatibilität, schnell.\n"
            "H.265 (libx265): kleinere Datei bei gleicher Qualität, langsamer.\n"
            "Hardware-Encoder (nvenc, vaapi …): sehr schnell, erfordert passende GPU.")
        enc_form.addRow("Encoder:", self._encoder_combo)

        self._preset_combo = QComboBox()
        self._preset_combo.addItems([
            "ultrafast", "superfast", "veryfast", "faster", "fast",
            "medium", "slow", "slower", "veryslow"])
        self._preset_combo.setToolTip(
            "Kodiergeschwindigkeit vs. Dateigröße.\n"
            "Schnellere Presets = größere Datei bei gleicher Qualität.\n"
            "Langsamere Presets = kleinere Datei, brauchen mehr CPU-Zeit.\n"
            "Empfehlung: 'fast' oder 'medium' für den täglichen Einsatz.")
        enc_form.addRow("Preset:", self._preset_combo)

        crf_row = QHBoxLayout()
        self._crf_spin = QSpinBox()
        self._crf_spin.setRange(0, 51)
        self._crf_spin.setToolTip(
            "Constant Rate Factor – steuert die Qualität der Ausgabedatei.\n"
            "Niedrigerer Wert = bessere Qualität, größere Datei.\n"
            "0 = verlustfrei  |  18 = sehr gut  |  23 = Standard  |  28 = niedrig\n"
            "Empfehlung für Sport-Videos: 18–22.")
        crf_row.addWidget(self._crf_spin)
        crf_hint = QLabel("0 = verlustfrei  ·  18 = sehr gut  ·  23 = Standard")
        crf_hint.setStyleSheet("color: #888; font-size: 11px;")
        crf_row.addWidget(crf_hint)
        crf_row.addStretch()
        enc_form.addRow("CRF:", crf_row)

        fps_row = QHBoxLayout()
        self._fps_spin = QSpinBox()
        self._fps_spin.setRange(1, 120)
        self._fps_spin.setToolTip(
            "Ziel-Framerate der Ausgabedatei (Bilder pro Sekunde).\n"
            "Typische Werte: 25 (PAL), 30 (NTSC), 50/60 (Sport-Videos).\n"
            "Pi-Kameras liefern oft 25 oder 30 FPS.")
        fps_row.addWidget(self._fps_spin)
        fps_row.addStretch()
        enc_form.addRow("FPS:", fps_row)

        self._format_combo = QComboBox()
        self._format_combo.addItems(["mp4", "avi"])
        self._format_combo.setToolTip(
            "Container-Format der Ausgabedatei.\n"
            "MP4: universell kompatibel, ideal für YouTube und Web.\n"
            "AVI: älteres Format, breite Software-Unterstützung.\n"
            "Empfehlung: MP4.")
        enc_form.addRow("Format:", self._format_combo)

        layout.addLayout(profile_row)
        layout.addWidget(self._encoding_widget)

        self._form_layout.addWidget(group)
        self._processing_group = group

    # ── Sektion: Audio ────────────────────────────────────────

    def _build_section_audio(self) -> None:
        group = QGroupBox("Audio")
        layout = QVBoxLayout(group)

        self._merge_audio_cb = QCheckBox(
            "Separate Audio-Spur zusammenführen (z. B. .wav + .mjpg)")
        self._merge_audio_cb.setToolTip(
            "Führt eine separate Audio-Datei (.wav, .mp3 …) mit der Videodatei zusammen.\n"
            "ffmpeg erkennt passende Audio-Dateien automatisch nach Dateiname.")
        layout.addWidget(self._merge_audio_cb)

        amplify_row = QHBoxLayout()
        self._amplify_audio_cb = QCheckBox("Lautstärke anpassen:")
        self._amplify_audio_cb.setToolTip(
            "Erhöht oder verringert die Lautstärke um den angegebenen dB-Wert.\n"
            "Anschließend wird Loudnorm (EBU R128) angewendet.")
        self._amplify_audio_cb.toggled.connect(
            lambda on: self._amplify_db_spin.setEnabled(on))
        amplify_row.addWidget(self._amplify_audio_cb)

        self._amplify_db_spin = QDoubleSpinBox()
        self._amplify_db_spin.setRange(-20.0, 40.0)
        self._amplify_db_spin.setSingleStep(1.0)
        self._amplify_db_spin.setDecimals(1)
        self._amplify_db_spin.setSuffix(" dB")
        self._amplify_db_spin.setFixedWidth(90)
        amplify_row.addWidget(self._amplify_db_spin)

        db_hint = QLabel("+6 dB ≈ doppelte Lautstärke")
        db_hint.setStyleSheet("color: #888; font-size: 11px;")
        amplify_row.addWidget(db_hint)
        amplify_row.addStretch()
        layout.addLayout(amplify_row)

        self._audio_sync_cb = QCheckBox(
            "Audio-Sync (Frame-Drop-Korrektur für Pi-Kameras)")
        self._audio_sync_cb.setToolTip(
            "Zählt Frames und passt die FPS dynamisch an die Audio-Dauer an.\n"
            "Notwendig wenn die Kamera Frames verliert (typisch für Pi-Kameras).")
        layout.addWidget(self._audio_sync_cb)

        self._form_layout.addWidget(group)

    # ── Sektion: YouTube ──────────────────────────────────────

    def _build_section_youtube(self) -> None:
        group = QGroupBox("YouTube")
        layout = QVBoxLayout(group)

        self._yt_create_cb = QCheckBox(
            "YouTube-optimierte Version erstellen (separate Ausgabedatei)")
        self._yt_create_cb.setToolTip(
            "Erzeugt eine zweite, YouTube-optimierte Ausgabedatei\n"
            "(angepasste Bitrate, Codec-Einstellungen gemäß YT-Empfehlung).")
        layout.addWidget(self._yt_create_cb)

        self._yt_upload_cb = QCheckBox("Auf YouTube hochladen")
        self._yt_upload_cb.setToolTip(
            "Lädt die (optimierte oder konvertierte) Datei auf YouTube hoch.\n"
            "Wenn keine Konvertierung aktiv ist, wird die Originaldatei hochgeladen.")
        self._yt_upload_cb.toggled.connect(self._update_visibility)
        layout.addWidget(self._yt_upload_cb)

        # Upload-Details – nur sichtbar wenn Upload aktiv
        self._yt_details_widget = QWidget()
        yt_form = QFormLayout(self._yt_details_widget)
        yt_form.setContentsMargins(20, 4, 0, 0)

        self._yt_title_edit = QLineEdit()
        self._yt_title_edit.setPlaceholderText("leer = Dateiname wird als Titel verwendet")
        self._yt_title_edit.setToolTip(
            "YouTube-Titel für alle Videos dieses Auftrags.\n"
            "Bleibt das Feld leer, wird der jeweilige Dateiname als Titel verwendet.")
        yt_form.addRow("Titel:", self._yt_title_edit)

        self._yt_playlist_edit = QLineEdit()
        self._yt_playlist_edit.setPlaceholderText("z. B. Saison 2024/25  (leer = keine Playlist)")
        self._yt_playlist_edit.setToolTip(
            "YouTube-Playlist, in die alle Videos eingeordnet werden.\n"
            "Existiert die Playlist noch nicht, wird sie automatisch angelegt.")
        yt_form.addRow("Playlist:", self._yt_playlist_edit)

        layout.addWidget(self._yt_details_widget)
        self._form_layout.addWidget(group)

    # ── Felder aus Job befüllen ───────────────────────────────

    def _populate_from_job(self) -> None:
        job = self._job

        self._name_edit.setText(job.name)

        # Quellmodus
        mode_map = {"files": 0, "folder_scan": 1, "pi_download": 2}
        btn = self._mode_group.button(mode_map.get(job.source_mode, 0))
        if btn:
            btn.setChecked(True)

        # files
        self._file_list.load(job.files)

        # folder_scan
        self._folder_src_edit.setText(job.source_folder)
        self._file_pattern_edit.setText(job.file_pattern or "*.mp4")
        self._folder_dst_edit.setText(job.copy_destination)
        self._move_files_cb.setChecked(job.move_files)
        self._folder_prefix_edit.setText(job.output_prefix)

        # pi_download
        dev_idx = self._device_combo.findData(job.device_name)
        if dev_idx >= 0:
            self._device_combo.setCurrentIndex(dev_idx)
        self._pi_dest_edit.setText(job.download_destination
                                   or self._settings.cameras.destination)
        self._delete_after_dl_cb.setChecked(job.delete_after_download)
        self._pi_prefix_edit.setText(job.output_prefix)

        # YouTube
        self._yt_title_edit.setText(job.default_youtube_title)
        self._yt_playlist_edit.setText(job.default_youtube_playlist)

        # Verarbeitung
        self._convert_enabled_cb.setChecked(job.convert_enabled)

        enc_idx = self._encoder_combo.findData(job.encoder)
        if enc_idx >= 0:
            self._encoder_combo.setCurrentIndex(enc_idx)
        self._preset_combo.setCurrentText(job.preset)
        self._crf_spin.setValue(job.crf)
        self._fps_spin.setValue(job.fps)
        self._format_combo.setCurrentText(job.output_format)

        # Audio
        self._merge_audio_cb.setChecked(job.merge_audio)
        self._amplify_audio_cb.setChecked(job.amplify_audio)
        self._amplify_db_spin.setValue(job.amplify_db)
        self._amplify_db_spin.setEnabled(job.amplify_audio)
        self._audio_sync_cb.setChecked(job.audio_sync)

        # YouTube
        self._yt_create_cb.setChecked(job.create_youtube_version)
        self._yt_upload_cb.setChecked(job.upload_youtube)

    # ── Vorlage anwenden ─────────────────────────────────────

    def _apply_template(self, name: str) -> None:
        """Befüllt das Formular mit den Werten einer Pipeline-Vorlage."""
        tpl = PIPELINE_TEMPLATES.get(name, {})
        if not tpl:
            return   # "Benutzerdefiniert" – nichts ändern

        mode_map = {"files": 0, "folder_scan": 1, "pi_download": 2}
        if "source_mode" in tpl:
            btn = self._mode_group.button(mode_map.get(tpl["source_mode"], 0))
            if btn:
                btn.setChecked(True)
        if "convert_enabled" in tpl:
            self._convert_enabled_cb.setChecked(tpl["convert_enabled"])
        if "merge_audio" in tpl:
            self._merge_audio_cb.setChecked(tpl["merge_audio"])
        if "amplify_audio" in tpl:
            self._amplify_audio_cb.setChecked(tpl["amplify_audio"])
        if "audio_sync" in tpl:
            self._audio_sync_cb.setChecked(tpl["audio_sync"])
        if "create_youtube_version" in tpl:
            self._yt_create_cb.setChecked(tpl["create_youtube_version"])
        if "upload_youtube" in tpl:
            self._yt_upload_cb.setChecked(tpl["upload_youtube"])

        self._update_visibility()

    # ── Sichtbarkeit aktualisieren ────────────────────────────

    def _update_visibility(self) -> None:
        mode_id = self._mode_group.checkedId()  # 0=files 1=folder 2=pi
        self._files_panel.setVisible(mode_id == 0)
        self._folder_panel.setVisible(mode_id == 1)
        self._pi_panel.setVisible(mode_id == 2)

        convert_on = self._convert_enabled_cb.isChecked()
        self._encoding_widget.setEnabled(convert_on)
        self._processing_group.setTitle(
            "Verarbeitung"
            if convert_on else "Verarbeitung  (deaktiviert – Originaldatei wird verwendet)")

        upload_on = self._yt_upload_cb.isChecked()
        self._yt_details_widget.setVisible(upload_on)

        self.adjustSize()

    # ── Profilwerte übernehmen ────────────────────────────────

    def _apply_profile(self, profile_name: str) -> None:
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

    # ── Validierung und Speichern ─────────────────────────────

    def _validate_and_accept(self) -> None:
        mode_id = self._mode_group.checkedId()

        if mode_id == 0:  # files
            if self._file_list.is_empty():
                QMessageBox.warning(
                    self, "Keine Dateien",
                    "Bitte mindestens eine Datei hinzufügen.")
                return

        elif mode_id == 1:  # folder_scan
            if not self._folder_src_edit.text().strip():
                QMessageBox.warning(
                    self, "Kein Quellordner",
                    "Bitte einen Quellordner angeben.")
                return

        elif mode_id == 2:  # pi_download
            if not self._device_combo.currentData():
                QMessageBox.warning(
                    self, "Kein Gerät",
                    "Bitte ein Pi-Kamera-Gerät auswählen.")
                return
            if not self._pi_dest_edit.text().strip():
                QMessageBox.warning(
                    self, "Kein Zielverzeichnis",
                    "Bitte ein lokales Zielverzeichnis angeben.")
                return

        self._write_job()
        self.accept()

    def _write_job(self) -> None:
        """Schreibt alle GUI-Werte in das WorkflowJob-Objekt."""
        job = self._job
        mode_id = self._mode_group.checkedId()
        mode_map = {0: "files", 1: "folder_scan", 2: "pi_download"}
        job.source_mode = mode_map[mode_id]
        job.name = self._name_edit.text().strip()

        if mode_id == 0:
            job.files = self._file_list.collect()
            job.source_folder = ""
            job.device_name = ""
            if not job.name:
                job.name = (Path(job.files[0].source_path).stem
                            if job.files else "Auftrag")

        elif mode_id == 1:
            job.source_folder = self._folder_src_edit.text().strip()
            job.file_pattern = self._file_pattern_edit.text().strip() or "*.mp4"
            job.copy_destination = self._folder_dst_edit.text().strip()
            job.move_files = self._move_files_cb.isChecked()
            job.output_prefix = self._folder_prefix_edit.text().strip()
            job.files = []
            job.device_name = ""
            if not job.name:
                job.name = Path(job.source_folder).name or "Ordner"

        elif mode_id == 2:
            job.device_name = self._device_combo.currentData()
            job.download_destination = self._pi_dest_edit.text().strip()
            job.delete_after_download = self._delete_after_dl_cb.isChecked()
            job.output_prefix = self._pi_prefix_edit.text().strip()
            job.files = []
            job.source_folder = ""
            if not job.name:
                job.name = job.device_name or "Pi-Kamera"

        # Verarbeitung
        job.convert_enabled = self._convert_enabled_cb.isChecked()
        job.encoder = self._encoder_combo.currentData()
        job.preset = self._preset_combo.currentText()
        job.crf = self._crf_spin.value()
        job.fps = self._fps_spin.value()
        job.output_format = self._format_combo.currentText()

        # Audio
        job.merge_audio = self._merge_audio_cb.isChecked()
        job.amplify_audio = self._amplify_audio_cb.isChecked()
        job.amplify_db = self._amplify_db_spin.value()
        job.audio_sync = self._audio_sync_cb.isChecked()

        # YouTube
        job.create_youtube_version = self._yt_create_cb.isChecked()
        job.upload_youtube = self._yt_upload_cb.isChecked()
        job.default_youtube_title = self._yt_title_edit.text().strip()
        job.default_youtube_playlist = self._yt_playlist_edit.text().strip()

    # ── Hilfsmethoden ─────────────────────────────────────────

    def _browse_dir(self, line_edit: QLineEdit, title: str) -> None:
        start = line_edit.text().strip() or self._settings.last_directory or str(Path.home())
        chosen = QFileDialog.getExistingDirectory(self, title, start)
        if chosen:
            line_edit.setText(chosen)
            self._save_last_dir(chosen)

    def _save_last_dir(self, directory: str) -> None:
        self._settings.last_directory = directory
        self._settings.save()
