"""Kompakter Dialog zum Bearbeiten direkter ConvertJob-Metadaten."""

from PySide6.QtWidgets import QDialog, QDialogButtonBox, QFormLayout, QGroupBox, QLabel, QLineEdit, QVBoxLayout

from ...media.converter import ConvertJob


class JobEditDialog(QDialog):
    def __init__(self, parent, job: ConvertJob):
        super().__init__(parent)
        self.setWindowTitle("Job bearbeiten")
        self.job = job

        layout = QVBoxLayout(self)
        info_group = QGroupBox("Download-Auftrag" if job.job_type == "download" else "Konvertier-Auftrag")
        info_form = QFormLayout()
        if job.job_type == "download":
            dev_label = QLabel(job.device_name)
            dev_label.setStyleSheet("color: palette(link); font-weight: bold;")
            info_form.addRow("Gerät:", dev_label)
            info_form.addRow("Zielordner:", QLabel(str(job.source_path)))
        else:
            file_label = QLabel(job.source_path.name)
            file_label.setStyleSheet("color: palette(link);")
            info_form.addRow("Datei:", file_label)
        info_group.setLayout(info_form)
        layout.addWidget(info_group)

        yt_group = QGroupBox("YouTube-Metadaten")
        yt_form = QFormLayout()
        self.title_edit = QLineEdit(job.youtube_title)
        self.title_edit.setMaxLength(100)
        if job.job_type == "download":
            self.title_edit.setPlaceholderText("Wird auf alle heruntergeladenen Dateien übertragen")
        yt_form.addRow("YouTube-Titel:", self.title_edit)
        self.playlist_edit = QLineEdit(job.youtube_playlist)
        self.playlist_edit.setPlaceholderText("Playlist-Name (wird automatisch angelegt)")
        yt_form.addRow("Playlist:", self.playlist_edit)
        yt_group.setLayout(yt_form)
        layout.addWidget(yt_group)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _save(self):
        self.job.youtube_title = self.title_edit.text()
        self.job.youtube_playlist = self.playlist_edit.text()
        self.accept()