"""Video and audio settings dialogs."""

from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QSpinBox,
    QVBoxLayout,
)

from ...media.diagnostics import gpu_diagnostics
from ...media.encoder import available_encoder_choices, encoder_display_name, resolve_encoder
from ...settings import AppSettings, PROFILES
from ...settings.profiles import (
    VIDEO_FORMAT_OPTIONS,
    VIDEO_LABEL_CONTAINER,
    VIDEO_LABEL_FPS,
    VIDEO_LABEL_PRESET,
    VIDEO_LABEL_PROFILE,
    VIDEO_LABEL_RESOLUTION,
    VIDEO_PRESET_OPTIONS,
    VIDEO_RESOLUTION_OPTIONS,
    VIDEO_TEXT_NO_BFRAMES,
    VIDEO_TOOLTIP_AUDIO_SYNC,
    VIDEO_TOOLTIP_CONTAINER,
    VIDEO_TOOLTIP_NO_BFRAMES,
    VIDEO_TOOLTIP_OVERWRITE,
    VIDEO_TOOLTIP_PRESET,
    VIDEO_TOOLTIP_RESOLUTION,
)


class VideoSettingsDialog(QDialog):
    def __init__(self, parent, settings: AppSettings):
        super().__init__(parent)
        self.setWindowTitle("Video-Einstellungen")
        self.settings = settings
        vs = settings.video
        self._updating_profile = False

        layout = QVBoxLayout(self)

        profile_group = QGroupBox("Profil")
        profile_form = QFormLayout()
        self.profile_combo = QComboBox()
        for name in PROFILES:
            self.profile_combo.addItem(name)
        self.profile_combo.setCurrentText(vs.profile)
        self.profile_combo.currentTextChanged.connect(self._on_profile_changed)
        profile_form.addRow(VIDEO_LABEL_PROFILE, self.profile_combo)
        profile_group.setLayout(profile_form)
        layout.addWidget(profile_group)

        group = QGroupBox("Video-Kodierung")
        form = QFormLayout()

        self.encoder_combo = QComboBox()
        for enc_id, enc_name in available_encoder_choices():
            self.encoder_combo.addItem(enc_name, enc_id)
        idx = self.encoder_combo.findData(vs.encoder)
        if idx >= 0:
            self.encoder_combo.setCurrentIndex(idx)
        self.encoder_combo.currentIndexChanged.connect(self._on_setting_changed)

        resolved = resolve_encoder(vs.encoder)
        self.encoder_info = QLabel(f"→ {encoder_display_name(resolved)}")
        self.encoder_info.setEnabled(False)
        enc_row = QHBoxLayout()
        enc_row.addWidget(self.encoder_combo)
        enc_row.addWidget(self.encoder_info)
        enc_row.addStretch()
        form.addRow("Encoder:", enc_row)

        diag = gpu_diagnostics()
        gpu_icon = "🟢" if diag.nvenc_available else "🔴"
        self.gpu_status = QLabel(f"{gpu_icon} {diag.summary}")
        self.gpu_status.setWordWrap(True)
        self.gpu_status.setToolTip("\n".join(diag.details))
        if not diag.nvenc_available:
            self.gpu_status.setStyleSheet("color: #b35900;")
        form.addRow("GPU-Status:", self.gpu_status)

        self.fps_spin = QSpinBox()
        self.fps_spin.setRange(1, 120)
        self.fps_spin.setValue(vs.fps)
        form.addRow(VIDEO_LABEL_FPS, self.fps_spin)

        self.resolution_combo = QComboBox()
        for value, label in VIDEO_RESOLUTION_OPTIONS:
            self.resolution_combo.addItem(label, value)
        self.resolution_combo.setCurrentIndex(max(self.resolution_combo.findData(vs.output_resolution), 0))
        self.resolution_combo.currentIndexChanged.connect(self._on_setting_changed)
        self.resolution_combo.setToolTip(VIDEO_TOOLTIP_RESOLUTION)
        form.addRow(VIDEO_LABEL_RESOLUTION, self.resolution_combo)

        self.fmt_combo = QComboBox()
        for value, label in VIDEO_FORMAT_OPTIONS:
            self.fmt_combo.addItem(label, value)
        self.fmt_combo.setCurrentIndex(max(self.fmt_combo.findData(vs.output_format), 0))
        self.fmt_combo.currentIndexChanged.connect(self._on_setting_changed)
        self.fmt_combo.setToolTip(VIDEO_TOOLTIP_CONTAINER)
        form.addRow(VIDEO_LABEL_CONTAINER, self.fmt_combo)

        self.crf_spin = QSpinBox()
        self.crf_spin.setRange(0, 51)
        self.crf_spin.setValue(vs.crf)
        self.crf_spin.valueChanged.connect(self._on_setting_changed)
        crf_row = QHBoxLayout()
        crf_row.addWidget(self.crf_spin)
        self.crf_hint = QLabel("0=verlustfrei  18=sehr gut  23=Standard")
        self.crf_hint.setEnabled(False)
        crf_row.addWidget(self.crf_hint)
        crf_row.addStretch()
        form.addRow("CRF / CQ (Qualität):", crf_row)

        self.preset_combo = QComboBox()
        self.preset_combo.addItems(VIDEO_PRESET_OPTIONS)
        self.preset_combo.setCurrentText(vs.preset)
        self.preset_combo.currentTextChanged.connect(self._on_setting_changed)
        self.preset_combo.setToolTip(VIDEO_TOOLTIP_PRESET)
        form.addRow(VIDEO_LABEL_PRESET, self.preset_combo)

        self.lossless_cb = QCheckBox("Verlustfrei")
        self.lossless_cb.setChecked(vs.lossless)
        self.lossless_cb.stateChanged.connect(self._on_setting_changed)
        form.addRow("", self.lossless_cb)

        self.audio_sync_cb = QCheckBox("Audio-Video-Sync (Frame-Drop-Korrektur)")
        self.audio_sync_cb.setChecked(vs.audio_sync)
        self.audio_sync_cb.setToolTip(VIDEO_TOOLTIP_AUDIO_SYNC)
        form.addRow("", self.audio_sync_cb)

        self.overwrite_cb = QCheckBox("Vorhandene Dateien überschreiben")
        self.overwrite_cb.setChecked(vs.overwrite)
        self.overwrite_cb.setToolTip(VIDEO_TOOLTIP_OVERWRITE)
        form.addRow("", self.overwrite_cb)

        self.no_bframes_cb = QCheckBox(VIDEO_TEXT_NO_BFRAMES)
        self.no_bframes_cb.setChecked(vs.no_bframes)
        self.no_bframes_cb.setToolTip(VIDEO_TOOLTIP_NO_BFRAMES)
        form.addRow("", self.no_bframes_cb)

        kf_row = QHBoxLayout()
        self.keyframe_spin = QSpinBox()
        self.keyframe_spin.setRange(0, 60)
        self.keyframe_spin.setValue(vs.keyframe_interval)
        self.keyframe_spin.setSuffix(" Sek (0 = Encoder-Standard)")
        self.keyframe_spin.setToolTip(
            "Maximaler Abstand zwischen Keyframes.\n"
            "Kleiner Wert = schnellerer Random-Zugriff, für KI-Analyse empfohlen: 1-2 Sek.\n"
            "0 = Encoder entscheidet (meist 2-10 Sek.)"
        )
        kf_row.addWidget(self.keyframe_spin)
        kf_row.addStretch()
        form.addRow("Keyframe-Abstand:", kf_row)

        group.setLayout(form)
        layout.addWidget(group)

        merge_group = QGroupBox("Halbzeiten zusammenführen")
        merge_form = QFormLayout()
        self.merge_cb = QCheckBox("Dateien pro Kamera-Ordner zusammenführen")
        self.merge_cb.setChecked(vs.merge_halves)
        self.merge_cb.setToolTip(
            "Sortiert die konvertierten Dateien pro Ordner nach Name\n"
            "und fügt sie mit Titelkarten (\"1. Halbzeit\", \"2. Halbzeit\") zusammen."
        )
        merge_form.addRow("", self.merge_cb)
        self.title_dur_spin = QSpinBox()
        self.title_dur_spin.setRange(1, 15)
        self.title_dur_spin.setValue(vs.merge_title_duration)
        self.title_dur_spin.setSuffix(" Sekunden")
        merge_form.addRow("Titelbild-Dauer:", self.title_dur_spin)
        merge_group.setLayout(merge_form)
        layout.addWidget(merge_group)

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Save).setText("Speichern")
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _on_profile_changed(self, profile_name: str):
        if self._updating_profile:
            return
        self._updating_profile = True
        values = PROFILES.get(profile_name, {})
        if "encoder" in values:
            idx = self.encoder_combo.findData(values["encoder"])
            if idx >= 0:
                self.encoder_combo.setCurrentIndex(idx)
        if "lossless" in values:
            self.lossless_cb.setChecked(values["lossless"])
        if "preset" in values:
            self.preset_combo.setCurrentText(values["preset"])
        if "crf" in values:
            self.crf_spin.setValue(values["crf"])
        if "output_format" in values:
            self.fmt_combo.setCurrentIndex(max(self.fmt_combo.findData(values["output_format"]), 0))
        if "output_resolution" in values:
            self.resolution_combo.setCurrentIndex(max(self.resolution_combo.findData(values["output_resolution"]), 0))
        if "no_bframes" in values:
            self.no_bframes_cb.setChecked(values["no_bframes"])
        if "keyframe_interval" in values:
            self.keyframe_spin.setValue(values["keyframe_interval"])
        self._update_encoder_info()
        self._updating_profile = False

    def _on_setting_changed(self):
        if not self._updating_profile:
            self._updating_profile = True
            self.profile_combo.setCurrentText("Benutzerdefiniert")
            self._updating_profile = False
        self._update_encoder_info()

    def _update_encoder_info(self):
        enc_id = self.encoder_combo.currentData()
        resolved = resolve_encoder(enc_id)
        self.encoder_info.setText(f"→ {encoder_display_name(resolved)}")

    def _save(self):
        vs = self.settings.video
        vs.profile = self.profile_combo.currentText()
        vs.encoder = self.encoder_combo.currentData()
        vs.fps = self.fps_spin.value()
        vs.output_resolution = str(self.resolution_combo.currentData() or "source")
        vs.output_format = str(self.fmt_combo.currentData() or "mp4")
        vs.crf = self.crf_spin.value()
        vs.preset = self.preset_combo.currentText()
        vs.lossless = self.lossless_cb.isChecked()
        vs.audio_sync = self.audio_sync_cb.isChecked()
        vs.overwrite = self.overwrite_cb.isChecked()
        vs.merge_halves = self.merge_cb.isChecked()
        vs.merge_title_duration = self.title_dur_spin.value()
        vs.no_bframes = self.no_bframes_cb.isChecked()
        vs.keyframe_interval = self.keyframe_spin.value()
        self.settings.save()
        self.accept()


class AudioSettingsDialog(QDialog):
    def __init__(self, parent, settings: AppSettings):
        super().__init__(parent)
        self.setWindowTitle("Audio-Einstellungen")
        self.settings = settings
        audio = settings.audio

        layout = QVBoxLayout(self)
        group = QGroupBox("Audio")
        form = QFormLayout()
        self.include_cb = QCheckBox("Audio einbinden")
        self.include_cb.setChecked(audio.include_audio)
        form.addRow("", self.include_cb)
        self.amplify_cb = QCheckBox("Audio verstärken")
        self.amplify_cb.setChecked(audio.amplify_audio)
        form.addRow("", self.amplify_cb)
        self.amplify_db_spin = QDoubleSpinBox()
        self.amplify_db_spin.setRange(-10.0, 30.0)
        self.amplify_db_spin.setSingleStep(1.0)
        self.amplify_db_spin.setDecimals(1)
        self.amplify_db_spin.setSuffix(" dB")
        self.amplify_db_spin.setValue(audio.amplify_db)
        self.amplify_db_spin.setToolTip(
            "Verstärkung in Dezibel.\n"
            "+6 dB ≈ doppelte Lautstärke, 0 = unverändert.\n"
            "Anschließend wird loudnorm (EBU R128) angewendet."
        )
        self.amplify_db_spin.setEnabled(audio.amplify_audio)
        self.amplify_cb.toggled.connect(self.amplify_db_spin.setEnabled)
        form.addRow("Verstärkung:", self.amplify_db_spin)
        self.suffix_edit = QLineEdit(audio.audio_suffix)
        self.suffix_edit.setPlaceholderText('z.B. "_normalized"')
        form.addRow("Audio-Suffix:", self.suffix_edit)
        self.bitrate_combo = QComboBox()
        self.bitrate_combo.addItems(["96k", "128k", "192k", "256k", "320k"])
        self.bitrate_combo.setCurrentText(audio.audio_bitrate)
        form.addRow("Audio-Bitrate:", self.bitrate_combo)
        group.setLayout(form)
        layout.addWidget(group)
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Save).setText("Speichern")
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _save(self):
        audio = self.settings.audio
        audio.include_audio = self.include_cb.isChecked()
        audio.amplify_audio = self.amplify_cb.isChecked()
        audio.amplify_db = self.amplify_db_spin.value()
        audio.audio_suffix = self.suffix_edit.text()
        audio.audio_bitrate = self.bitrate_combo.currentText()
        self.settings.save()
        self.accept()