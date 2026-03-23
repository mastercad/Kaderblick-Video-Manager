"""General application settings dialog."""

from PySide6.QtWidgets import QCheckBox, QDialog, QDialogButtonBox, QFormLayout, QGroupBox, QLabel, QVBoxLayout

from ...settings import AppSettings


class GeneralSettingsDialog(QDialog):
    def __init__(self, parent, settings: AppSettings):
        super().__init__(parent)
        self.setWindowTitle("Allgemeine Einstellungen")
        self.settings = settings

        layout = QVBoxLayout(self)
        group = QGroupBox("Verhalten beim Start")
        form = QFormLayout()
        self.restore_cb = QCheckBox("Letzten Workflow-Stand beim Start wiederherstellen")
        self.restore_cb.setChecked(settings.restore_last_workflow)
        form.addRow("", self.restore_cb)
        hint = QLabel("Der aktuelle Workflow-Stand wird beim Beenden automatisch\nals last_workflow.json gespeichert.")
        hint.setEnabled(False)
        form.addRow("", hint)
        group.setLayout(form)
        layout.addWidget(group)
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Save).setText("Speichern")
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _save(self):
        self.settings.restore_last_workflow = self.restore_cb.isChecked()
        self.settings.save()
        self.accept()