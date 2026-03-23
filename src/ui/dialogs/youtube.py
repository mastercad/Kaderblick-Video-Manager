"""YouTube-related dialogs."""

from PySide6.QtWidgets import QCheckBox, QComboBox, QDialog, QDialogButtonBox, QFormLayout, QGroupBox, QLabel, QSpinBox, QVBoxLayout

from ...settings import AppSettings


class YouTubeSettingsDialog(QDialog):
    def __init__(self, parent, settings: AppSettings):
        super().__init__(parent)
        self.setWindowTitle("YouTube-Einstellungen")
        self.settings = settings
        yt = settings.youtube

        layout = QVBoxLayout(self)

        convert_group = QGroupBox("YouTube-Konvertierung")
        convert_form = QFormLayout()
        self.create_cb = QCheckBox("YouTube-optimierte Version erstellen")
        self.create_cb.setChecked(yt.create_youtube)
        convert_form.addRow("", self.create_cb)
        self.yt_crf_spin = QSpinBox()
        self.yt_crf_spin.setRange(0, 51)
        self.yt_crf_spin.setValue(yt.youtube_crf)
        convert_form.addRow("CRF:", self.yt_crf_spin)
        self.maxrate_combo = QComboBox()
        self.maxrate_combo.addItems(["4M", "6M", "8M", "12M", "16M", "20M"])
        self.maxrate_combo.setCurrentText(yt.youtube_maxrate)
        convert_form.addRow("Max. Bitrate:", self.maxrate_combo)
        self.bufsize_combo = QComboBox()
        self.bufsize_combo.addItems(["8M", "12M", "16M", "24M", "32M"])
        self.bufsize_combo.setCurrentText(yt.youtube_bufsize)
        convert_form.addRow("Buffer-Größe:", self.bufsize_combo)
        self.yt_abr_combo = QComboBox()
        self.yt_abr_combo.addItems(["96k", "128k", "192k", "256k"])
        self.yt_abr_combo.setCurrentText(yt.youtube_audio_bitrate)
        convert_form.addRow("Audio-Bitrate:", self.yt_abr_combo)
        convert_group.setLayout(convert_form)
        layout.addWidget(convert_group)

        upload_group = QGroupBox("YouTube-Upload")
        upload_form = QFormLayout()
        self.upload_cb = QCheckBox("Videos auf YouTube hochladen")
        self.upload_cb.setChecked(yt.upload_to_youtube)
        upload_form.addRow("", self.upload_cb)
        hint = QLabel("(Titel und Playlist werden direkt im Workflow gepflegt)")
        hint.setEnabled(False)
        upload_form.addRow("", hint)
        upload_group.setLayout(upload_form)
        layout.addWidget(upload_group)

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Save).setText("Speichern")
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _save(self):
        yt = self.settings.youtube
        yt.create_youtube = self.create_cb.isChecked()
        yt.youtube_crf = self.yt_crf_spin.value()
        yt.youtube_maxrate = self.maxrate_combo.currentText()
        yt.youtube_bufsize = self.bufsize_combo.currentText()
        yt.youtube_audio_bitrate = self.yt_abr_combo.currentText()
        yt.upload_to_youtube = self.upload_cb.isChecked()
        self.settings.save()
        self.accept()