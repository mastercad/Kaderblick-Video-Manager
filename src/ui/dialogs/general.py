"""General application settings dialog."""

from datetime import date
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QCheckBox, QComboBox, QDialog, QDialogButtonBox, QFileDialog, QFormLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit, QPlainTextEdit, QPushButton, QVBoxLayout

from ...settings import AppSettings
from ...integrations.youtube_title_editor import MatchData, SegmentData, _add_to_history, build_playlist_title, build_video_description, build_video_title, load_memory, save_memory
from ...ui import ClearableDateField


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
            "Datum, Wettbewerb, Teams, Austragungsort und Kaderblick-Spiel-ID gelten als zentrale Vorgaben.\n"
            "Leere Felder in Workflow-Nodes übernehmen diese Werte automatisch; nur explizite Node-Werte überschreiben sie."
        )
        match_hint.setEnabled(False)
        memory = load_memory()
        today = date.today()
        default_date_iso = (settings.default_match_date or "").strip() or today.isoformat()

        self.match_date_edit = ClearableDateField(self)
        self.match_date_edit.setText(default_date_iso)
        self.match_competition_edit = self._make_history_combo(memory.get("history_competition", []), "z. B. Kreispokal")
        self.match_home_edit = self._make_history_combo(memory.get("history_home_team", []), "Heimmannschaft")
        self.match_away_edit = self._make_history_combo(memory.get("history_away_team", []), "Auswärtsmannschaft")
        self.match_location_edit = self._make_history_combo(memory.get("history_location", []), "Austragungsort")
        self.kb_game_id_edit = QLineEdit(settings.default_kaderblick_game_id)
        self.kb_game_id_edit.setPlaceholderText("z. B. 42")
        self.match_competition_edit.setCurrentText(settings.default_match_competition)
        self.match_home_edit.setCurrentText(settings.default_match_home_team)
        self.match_away_edit.setCurrentText(settings.default_match_away_team)
        self.match_location_edit.setCurrentText(settings.default_match_location)
        match_form.addRow("Spieldatum:", self.match_date_edit)
        match_form.addRow("Wettbewerb:", self.match_competition_edit)
        match_form.addRow("Heimmannschaft:", self.match_home_edit)
        match_form.addRow("Auswärtsmannschaft:", self.match_away_edit)
        match_form.addRow("Austragungsort:", self.match_location_edit)
        match_form.addRow("Kaderblick-Spiel-ID:", self.kb_game_id_edit)
        match_form.addRow("", match_hint)
        match_group.setLayout(match_form)
        layout.addWidget(match_group)

        preview_group = QGroupBox("YouTube-Vorschau")
        preview_form = QFormLayout(preview_group)
        preview_hint = QLabel(
            "Vorschau auf Basis der globalen Spieldaten. Titel und Beschreibung werden exemplarisch\n"
            "für einen Standard-Abschnitt '1. Halbzeit' ohne Kamera erzeugt."
        )
        preview_hint.setEnabled(False)
        preview_form.addRow("", preview_hint)

        mono = QFont("Monospace", 9)

        self.playlist_preview = QLabel("–")
        self.playlist_preview.setWordWrap(True)
        self.playlist_preview.setFont(mono)
        self.playlist_preview.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.playlist_len_lbl = QLabel()
        self.playlist_len_lbl.setFixedWidth(55)
        self.playlist_len_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        playlist_row = QHBoxLayout()
        playlist_row.addWidget(self.playlist_preview, 1)
        playlist_row.addWidget(self.playlist_len_lbl)
        preview_form.addRow("Playlist:", playlist_row)

        self.title_preview = QLabel("–")
        self.title_preview.setWordWrap(True)
        self.title_preview.setFont(mono)
        self.title_preview.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.title_len_lbl = QLabel()
        self.title_len_lbl.setFixedWidth(55)
        self.title_len_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        title_row = QHBoxLayout()
        title_row.addWidget(self.title_preview, 1)
        title_row.addWidget(self.title_len_lbl)
        preview_form.addRow("Titel:", title_row)

        self.description_preview = QPlainTextEdit()
        self.description_preview.setReadOnly(True)
        self.description_preview.setFont(mono)
        self.description_preview.setFixedHeight(120)
        preview_form.addRow("Beschreibung:", self.description_preview)
        layout.addWidget(preview_group)

        self.match_date_edit.effectiveTextChanged.connect(self._update_preview)
        self.match_competition_edit.currentTextChanged.connect(self._update_preview)
        self.match_home_edit.currentTextChanged.connect(self._update_preview)
        self.match_away_edit.currentTextChanged.connect(self._update_preview)
        self.match_location_edit.currentTextChanged.connect(self._update_preview)
        self._update_preview()

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

    def _current_match(self) -> MatchData:
        return MatchData(
            date_iso=self.match_date_edit.isoValue(),
            competition=self.match_competition_edit.currentText().strip(),
            home_team=self.match_home_edit.currentText().strip(),
            away_team=self.match_away_edit.currentText().strip(),
            location=self.match_location_edit.currentText().strip(),
        )

    def _update_preview(self, *_args) -> None:
        match = self._current_match()
        segment = SegmentData(type_name="1. Halbzeit", half=1)

        playlist = build_playlist_title(match)
        title = build_video_title(match, segment)
        description = build_video_description(match, segment)

        self.playlist_preview.setText(playlist or "–")
        self.title_preview.setText(title or "–")
        self.description_preview.setPlainText(description)
        self._set_len_label(self.playlist_len_lbl, len(playlist))
        self._set_len_label(self.title_len_lbl, len(title))

    @staticmethod
    def _set_len_label(label: QLabel, length: int) -> None:
        label.setText(f"{length} / 100")
        if length > 100:
            label.setStyleSheet("color: red; font-weight: bold;")
        elif length > 88:
            label.setStyleSheet("color: orange; font-weight: bold;")
        else:
            label.setStyleSheet("color: green;")

    def _save(self):
        self.settings.restore_last_workflow = self.restore_cb.isChecked()
        self.settings.workflow_output_root = self.output_root_edit.text().strip()
        date_iso = self.match_date_edit.isoValue()
        competition = self.match_competition_edit.currentText().strip()
        home_team = self.match_home_edit.currentText().strip()
        away_team = self.match_away_edit.currentText().strip()
        location = self.match_location_edit.currentText().strip()
        self.settings.default_match_date = date_iso
        self.settings.default_match_competition = competition
        self.settings.default_match_home_team = home_team
        self.settings.default_match_away_team = away_team
        self.settings.default_match_location = location
        self.settings.default_kaderblick_game_id = self.kb_game_id_edit.text().strip()
        self.settings.save()

        memory = load_memory()
        memory["last_match"] = {
            "date_iso": date_iso,
            "competition": competition,
            "home_team": home_team,
            "away_team": away_team,
            "location": location,
        }
        memory["history_competition"] = _add_to_history(memory.get("history_competition", []), competition)
        memory["history_home_team"] = _add_to_history(memory.get("history_home_team", []), home_team)
        memory["history_away_team"] = _add_to_history(memory.get("history_away_team", []), away_team)
        memory["history_location"] = _add_to_history(memory.get("history_location", []), location)
        save_memory(memory)
        self.accept()