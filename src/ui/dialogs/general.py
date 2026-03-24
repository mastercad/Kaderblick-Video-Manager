"""General application settings dialog."""

from datetime import date
from pathlib import Path

from PySide6.QtCore import QDate
from PySide6.QtWidgets import QCheckBox, QComboBox, QDateEdit, QDialog, QDialogButtonBox, QFileDialog, QFormLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit, QPushButton, QVBoxLayout

from ...settings import AppSettings
from ...integrations.youtube_title_editor import _add_to_history, load_memory, save_memory


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

        output_group = QGroupBox("Globale Ausgabe")
        output_form = QFormLayout()
        output_hint = QLabel(
            "Wenn gesetzt, schreiben Workflows ohne eigenes Ziel automatisch in\n"
            "<Basisordner>/<Workflow-Name>/."
        )
        output_hint.setEnabled(False)
        self.output_root_edit = QLineEdit(settings.workflow_output_root)
        browse_row = QHBoxLayout()
        browse_row.addWidget(self.output_root_edit)
        browse_btn = QPushButton("…")
        browse_btn.setFixedWidth(32)
        browse_btn.clicked.connect(self._browse_output_root)
        browse_row.addWidget(browse_btn)
        output_form.addRow("Basisordner:", browse_row)
        output_form.addRow("", output_hint)
        output_group.setLayout(output_form)
        layout.addWidget(output_group)

        match_group = QGroupBox("Globale Spieldaten")
        match_form = QFormLayout()
        match_hint = QLabel(
            "Wettbewerb, Heim- und Auswärtsmannschaft verwenden denselben Verlauf wie der Metadaten-Editor.\n"
            "Das Datum startet immer mit dem heutigen Tag. Pro Workflow können die Werte weiter überschrieben werden."
        )
        match_hint.setEnabled(False)
        memory = load_memory()
        last_match = memory.get("last_match", {})
        today = date.today()

        self.match_date_edit = QDateEdit(QDate(today.year, today.month, today.day))
        self.match_date_edit.setCalendarPopup(True)
        self.match_date_edit.setDisplayFormat("dd.MM.yyyy")
        self.match_competition_edit = self._make_history_combo(memory.get("history_competition", []), "z. B. Kreispokal")
        self.match_home_edit = self._make_history_combo(memory.get("history_home_team", []), "Heimmannschaft")
        self.match_away_edit = self._make_history_combo(memory.get("history_away_team", []), "Auswärtsmannschaft")
        self.match_competition_edit.setCurrentText(str(last_match.get("competition", "")))
        self.match_home_edit.setCurrentText(str(last_match.get("home_team", "")))
        self.match_away_edit.setCurrentText(str(last_match.get("away_team", "")))
        match_form.addRow("Spieldatum:", self.match_date_edit)
        match_form.addRow("Wettbewerb:", self.match_competition_edit)
        match_form.addRow("Heimmannschaft:", self.match_home_edit)
        match_form.addRow("Auswärtsmannschaft:", self.match_away_edit)
        match_form.addRow("", match_hint)
        match_group.setLayout(match_form)
        layout.addWidget(match_group)

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Save).setText("Speichern")
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _browse_output_root(self):
        start = self.output_root_edit.text().strip() or self.settings.last_directory or str(Path.home())
        chosen = QFileDialog.getExistingDirectory(self, "Basisordner für Workflow-Ausgaben wählen", start)
        if chosen:
            self.output_root_edit.setText(chosen)

    @staticmethod
    def _make_history_combo(items, placeholder: str) -> QComboBox:
        combo = QComboBox()
        combo.setEditable(True)
        combo.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        combo.lineEdit().setPlaceholderText(placeholder)
        for item in items:
            combo.addItem(str(item))
        return combo

    def _save(self):
        self.settings.restore_last_workflow = self.restore_cb.isChecked()
        self.settings.workflow_output_root = self.output_root_edit.text().strip()
        self.settings.save()

        qd = self.match_date_edit.date()
        date_iso = f"{qd.year():04d}-{qd.month():02d}-{qd.day():02d}"
        competition = self.match_competition_edit.currentText().strip()
        home_team = self.match_home_edit.currentText().strip()
        away_team = self.match_away_edit.currentText().strip()
        memory = load_memory()
        memory["last_match"] = {
            "date_iso": date_iso,
            "competition": competition,
            "home_team": home_team,
            "away_team": away_team,
        }
        memory["history_competition"] = _add_to_history(memory.get("history_competition", []), competition)
        memory["history_home_team"] = _add_to_history(memory.get("history_home_team", []), home_team)
        memory["history_away_team"] = _add_to_history(memory.get("history_away_team", []), away_team)
        save_memory(memory)
        self.accept()