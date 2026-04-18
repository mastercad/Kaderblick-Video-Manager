from __future__ import annotations

import copy
from datetime import date
import re

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QFormLayout,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QRadioButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from ...integrations.youtube_title_editor import (
    MatchData,
    SegmentData,
    _add_to_history,
    build_playlist_title,
    build_video_description,
    build_video_tags,
    build_video_title,
    load_memory,
    save_memory,
)
from ...settings import AppSettings
from ...ui import ClearableDateField, format_display_date, normalize_date_text


class OutputMetadataPanel(QWidget):
    metadata_changed = Signal()

    _DDMMYYYY_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")
    _YYYYMMDD_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")

    def __init__(self, parent=None, *, info_text: str, settings: AppSettings | None = None):
        super().__init__(parent)
        self._info_text = info_text
        self._settings = settings
        self._kb_video_types: list[dict] = []
        self._kb_cameras: list[dict] = []
        self._initial_kb_type_id = 0
        self._initial_kb_camera_id = 0
        today_iso = (settings.default_match_date if settings is not None else "") or date.today().isoformat()
        self._default_match = MatchData(date_iso=today_iso)
        self._match_field_overrides = {
            "date_iso": False,
            "competition": False,
            "home_team": False,
            "away_team": False,
            "location": False,
        }
        self._build_ui()
        self._load_memory_defaults()
        self._update_previews()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        info = QLabel(self._info_text)
        info.setWordWrap(True)
        info.setStyleSheet("color: #475569;")
        layout.addWidget(info)

        layout.addWidget(self._build_match_group())
        layout.addWidget(self._build_segment_group())

        sep = QFrame(self)
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setFrameShadow(QFrame.Shadow.Sunken)
        layout.addWidget(sep)

        layout.addWidget(self._build_preview_group())

    def _build_match_group(self) -> QWidget:
        group = QGroupBox("Spieldaten", self)
        form = QFormLayout(group)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)

        self._date_edit = ClearableDateField(self)
        self._date_edit.effectiveTextChanged.connect(self._on_field_changed)
        form.addRow("Datum:", self._date_edit)

        self._competition_combo = self._make_history_combo("history_competition", "z. B. Sparkassenpokal")
        form.addRow("Wettbewerb:", self._competition_combo)

        self._home_combo = self._make_history_combo("history_home_team", "Heimteam")
        form.addRow("Heimteam:", self._home_combo)

        self._away_combo = self._make_history_combo("history_away_team", "Auswärtsteam")
        form.addRow("Auswärtsteam:", self._away_combo)

        self._location_combo = self._make_history_combo("history_location", "Austragungsort")
        form.addRow("Austragungsort:", self._location_combo)
        return group

    def _build_segment_group(self) -> QWidget:
        group = QGroupBox("Video-Abschnitt", self)
        layout = QVBoxLayout(group)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)
        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(8)

        self._camera_combo = QComboBox(self)
        self._camera_combo.currentIndexChanged.connect(self._on_field_changed)
        self._camera_combo.editTextChanged.connect(self._on_field_changed)
        form.addRow("Kamera:", self._camera_combo)

        side_row = QHBoxLayout()
        self._side_group = QButtonGroup(self)
        for label, value in (("Keine", ""), ("Links", "Links"), ("Rechts", "Rechts")):
            button = QRadioButton(label, self)
            button.setProperty("side_value", value)
            self._side_group.addButton(button)
            side_row.addWidget(button)
        side_row.addStretch()
        self._side_group.buttonToggled.connect(lambda *_args: self._on_field_changed())
        form.addRow("Seite:", side_row)

        self._video_type_combo = QComboBox(self)
        self._video_type_combo.currentIndexChanged.connect(self._on_field_changed)
        form.addRow("Video-Typ:", self._video_type_combo)

        part_row = QHBoxLayout()
        self._part_cb = QCheckBox("Teil-Nummer:", self)
        self._part_spin = QSpinBox(self)
        self._part_spin.setRange(1, 20)
        self._part_spin.setValue(1)
        self._part_spin.setFixedWidth(65)
        self._part_spin.setEnabled(False)
        self._part_cb.toggled.connect(self._part_spin.setEnabled)
        self._part_cb.toggled.connect(self._on_field_changed)
        self._part_spin.valueChanged.connect(self._on_field_changed)
        part_row.addWidget(self._part_cb)
        part_row.addWidget(self._part_spin)
        part_row.addWidget(QLabel("leer = komplette Halbzeit in einem Video", self))
        part_row.addStretch()
        form.addRow("Teil:", part_row)

        layout.addLayout(form)
        return group

    def _build_preview_group(self) -> QWidget:
        group = QGroupBox("Vorschau", self)
        form = QFormLayout(group)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)
        mono = QFont("Monospace", 9)

        self._playlist_preview = QLabel("–", self)
        self._playlist_preview.setWordWrap(True)
        self._playlist_preview.setFont(mono)
        self._playlist_preview.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._playlist_len = QLabel(self)
        self._playlist_len.setFixedWidth(55)
        self._playlist_len.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        playlist_row = QHBoxLayout()
        playlist_row.addWidget(self._playlist_preview, 1)
        playlist_row.addWidget(self._playlist_len)
        form.addRow("Playlist:", playlist_row)

        self._title_preview = QLabel("–", self)
        self._title_preview.setWordWrap(True)
        self._title_preview.setFont(mono)
        self._title_preview.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._title_len = QLabel(self)
        self._title_len.setFixedWidth(55)
        self._title_len.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        title_row = QHBoxLayout()
        title_row.addWidget(self._title_preview, 1)
        title_row.addWidget(self._title_len)
        form.addRow("Titel:", title_row)

        self._description_preview = QLabel("", self)
        self._description_preview.setWordWrap(True)
        self._description_preview.setFont(mono)
        self._description_preview.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._description_preview.setStyleSheet("padding: 6px 8px; border: 1px solid #D7E0EA; border-radius: 8px; background: #F8FAFC;")
        form.addRow("Beschreibung:", self._description_preview)

        self._kaderblick_preview = QLabel("–", self)
        self._kaderblick_preview.setWordWrap(True)
        form.addRow("API-Zuordnung:", self._kaderblick_preview)
        return group

    def _load_memory_defaults(self) -> None:
        self.set_default_match(self._settings_default_match())
        self._reload_kaderblick_combos()

    def _settings_default_match(self) -> MatchData:
        values = self._settings.default_match_values() if self._settings is not None else {}
        date_iso = str(values.get("date_iso") or "").strip() or date.today().isoformat()
        return MatchData(
            date_iso=date_iso,
            competition=str(values.get("competition") or "").strip(),
            home_team=str(values.get("home_team") or "").strip(),
            away_team=str(values.get("away_team") or "").strip(),
            location=str(values.get("location") or "").strip(),
        )

    def set_default_match(self, match: MatchData) -> None:
        self._default_match = MatchData(
            date_iso=(match.date_iso or date.today().isoformat()).strip(),
            competition=(match.competition or "").strip(),
            home_team=(match.home_team or "").strip(),
            away_team=(match.away_team or "").strip(),
            location=(match.location or "").strip(),
        )
        self._date_edit.setPlaceholderText(self._default_match.date_iso)

        if not self._match_field_overrides["date_iso"]:
            self._date_edit.setText("")

        for key, combo in (
            ("competition", self._competition_combo),
            ("home_team", self._home_combo),
            ("away_team", self._away_combo),
            ("location", self._location_combo),
        ):
            combo.lineEdit().setPlaceholderText(getattr(self._default_match, key))
            if not self._match_field_overrides[key]:
                combo.blockSignals(True)
                combo.setCurrentText("")
                combo.blockSignals(False)

        self._apply_match_field_styles()

    def _make_history_combo(self, memory_key: str, placeholder: str) -> QComboBox:
        combo = QComboBox(self)
        combo.setEditable(True)
        combo.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        combo.lineEdit().setPlaceholderText(placeholder)
        for item in load_memory().get(memory_key, []):
            combo.addItem(str(item))
        combo.currentTextChanged.connect(self._on_field_changed)
        return combo

    def _reload_kaderblick_combos(self) -> None:
        self._camera_combo.blockSignals(True)
        self._camera_combo.clear()
        if self._kb_cameras:
            self._camera_combo.addItem("–", 0)
            for camera in self._kb_cameras:
                self._camera_combo.addItem(str(camera.get("name", "")), int(camera.get("id") or 0))
            self._camera_combo.setEditable(False)
            index = max(self._camera_combo.findData(self._initial_kb_camera_id), 0)
            self._camera_combo.setCurrentIndex(index)
        else:
            self._camera_combo.setEditable(True)
            self._camera_combo.addItem("(Keine Kamera)", "")
            for label in ("DJI Osmo Action5 Pro", "Kaderblick Links", "Kaderblick Rechts"):
                self._camera_combo.addItem(label, label)
        self._camera_combo.blockSignals(False)

        self._video_type_combo.blockSignals(True)
        self._video_type_combo.clear()
        if self._kb_video_types:
            self._video_type_combo.addItem("–", 0)
            for video_type in self._kb_video_types:
                self._video_type_combo.addItem(str(video_type.get("name", "")), int(video_type.get("id") or 0))
            index = self._video_type_combo.findData(self._initial_kb_type_id)
            self._video_type_combo.setCurrentIndex(index if index >= 0 else 0)
        else:
            for name in ("1. Halbzeit", "2. Halbzeit", "Vorbereitung", "Nachbereitung"):
                self._video_type_combo.addItem(name, 0)
        self._video_type_combo.blockSignals(False)
        self._update_previews()

    def set_kaderblick_options(self, video_types: list[dict], cameras: list[dict]) -> None:
        self._kb_video_types = list(video_types)
        self._kb_cameras = list(cameras)
        self._reload_kaderblick_combos()

    def _apply_match_field_styles(self) -> None:
        self._date_edit.setStyleSheet("")
        for _key, combo in (
            ("competition", self._competition_combo),
            ("home_team", self._home_combo),
            ("away_team", self._away_combo),
            ("location", self._location_combo),
        ):
            combo.lineEdit().setStyleSheet("")

    def _refresh_match_override_state(self) -> None:
        self._match_field_overrides["date_iso"] = bool(self._normalize_date_override(self._date_edit.text()))
        for key, combo in (
            ("competition", self._competition_combo),
            ("home_team", self._home_combo),
            ("away_team", self._away_combo),
            ("location", self._location_combo),
        ):
            value = combo.currentText().strip()
            self._match_field_overrides[key] = bool(value) and value != getattr(self._default_match, key)
        self._apply_match_field_styles()

    def load_from_job(
        self,
        *,
        match_data: dict,
        segment_data: dict,
        kb_type_id: int,
        kb_camera_id: int,
        fallback_match: MatchData,
        fallback_title: str,
        fallback_playlist: str,
        fallback_description: str,
    ) -> None:
        self._initial_kb_type_id = kb_type_id
        self._initial_kb_camera_id = kb_camera_id
        self.set_default_match(fallback_match)
        match_overrides = MatchData(**match_data) if match_data else MatchData()
        segment = SegmentData(**segment_data) if segment_data else SegmentData()

        self._date_edit.setText(self._format_display_date(match_overrides.date_iso))
        self._match_field_overrides["date_iso"] = bool(match_overrides.date_iso.strip())

        for key, combo, text in (
            ("competition", self._competition_combo, match_overrides.competition),
            ("home_team", self._home_combo, match_overrides.home_team),
            ("away_team", self._away_combo, match_overrides.away_team),
            ("location", self._location_combo, match_overrides.location),
        ):
            combo.blockSignals(True)
            combo.setCurrentText(text or "")
            combo.blockSignals(False)
            self._match_field_overrides[key] = bool((text or "").strip())

        self._reload_kaderblick_combos()

        if self._kb_cameras:
            camera_index = self._camera_combo.findData(kb_camera_id)
            self._camera_combo.setCurrentIndex(camera_index if camera_index >= 0 else 0)
        else:
            self._camera_combo.setCurrentText(segment.camera)

        for button in self._side_group.buttons():
            button.blockSignals(True)
            if str(button.property("side_value")) == segment.side:
                button.setChecked(True)
            elif not segment.side and str(button.property("side_value")) == "":
                button.setChecked(True)
            button.blockSignals(False)

        if self._kb_video_types:
            type_index = self._video_type_combo.findData(kb_type_id)
            self._video_type_combo.setCurrentIndex(type_index if type_index >= 0 else 0)
        else:
            type_index = self._video_type_combo.findText(segment.type_name or f"{segment.half}. Halbzeit")
            self._video_type_combo.setCurrentIndex(type_index if type_index >= 0 else 0)

        self._part_cb.blockSignals(True)
        self._part_spin.blockSignals(True)
        self._part_cb.setChecked(bool(segment.part))
        self._part_spin.setValue(segment.part or 1)
        self._part_spin.setEnabled(bool(segment.part))
        self._part_cb.blockSignals(False)
        self._part_spin.blockSignals(False)

        if not match_data and not segment_data:
            self._playlist_preview.setText(fallback_playlist or "–")
            self._title_preview.setText(fallback_title or "–")
            self._description_preview.setText(fallback_description or "")
        self._apply_match_field_styles()
        self._update_previews()

    def current_match(self) -> MatchData:
        return MatchData(
            date_iso=(self._normalize_date_override(self._date_edit.text()) or self._default_match.date_iso),
            competition=(self._competition_combo.currentText().strip() or self._default_match.competition),
            home_team=(self._home_combo.currentText().strip() or self._default_match.home_team),
            away_team=(self._away_combo.currentText().strip() or self._default_match.away_team),
            location=(self._location_combo.currentText().strip() or self._default_match.location),
        )

    def current_match_overrides(self) -> dict[str, str]:
        payload: dict[str, str] = {}
        date_override = self._normalize_date_override(self._date_edit.text())
        if self._match_field_overrides["date_iso"] and date_override:
            payload["date_iso"] = date_override
        for key, combo in (
            ("competition", self._competition_combo),
            ("home_team", self._home_combo),
            ("away_team", self._away_combo),
            ("location", self._location_combo),
        ):
            value = combo.currentText().strip()
            if self._match_field_overrides[key] and value:
                payload[key] = value
        return payload

    def current_segment(self) -> SegmentData:
        if self._kb_cameras:
            camera = self._camera_combo.currentText()
            if camera == "–":
                camera = ""
        else:
            current_text = self._camera_combo.currentText().strip()
            current_data = self._camera_combo.currentData()
            camera = current_text or str(current_data if current_data is not None else "").strip()

        side = ""
        for button in self._side_group.buttons():
            if button.isChecked():
                side = str(button.property("side_value"))
                break

        type_name = self._video_type_combo.currentText()
        if type_name == "–":
            type_name = ""
        half = 2 if type_name.startswith("2.") else 1
        part = self._part_spin.value() if self._part_cb.isChecked() else 0
        return SegmentData(camera=camera, side=side, half=half, part=part, type_name=type_name)

    def export_state(self) -> dict:
        match = self.current_match()
        segment = self.current_segment()
        return {
            "match_data": copy.deepcopy(self.current_match_overrides()),
            "segment_data": copy.deepcopy(segment.__dict__),
            "title": build_video_title(match, segment),
            "playlist": build_playlist_title(match),
            "description": build_video_description(match, segment),
            "tags": build_video_tags(match, segment),
            "kaderblick_video_type_id": int(self._video_type_combo.currentData() or 0) if self._kb_video_types else self._initial_kb_type_id,
            "kaderblick_camera_id": int(self._camera_combo.currentData() or 0) if self._kb_cameras else self._initial_kb_camera_id,
            "merge_match_data": copy.deepcopy(self.current_match_overrides()),
            "merge_segment_data": copy.deepcopy(segment.__dict__),
            "merge_output_title": build_video_title(match, segment),
            "merge_output_playlist": build_playlist_title(match),
            "merge_output_description": build_video_description(match, segment),
            "merge_output_kaderblick_video_type_id": int(self._video_type_combo.currentData() or 0) if self._kb_video_types else self._initial_kb_type_id,
            "merge_output_kaderblick_camera_id": int(self._camera_combo.currentData() or 0) if self._kb_cameras else self._initial_kb_camera_id,
        }

    def apply_match_data(self, match: MatchData) -> None:
        self._match_field_overrides["date_iso"] = bool((match.date_iso or "").strip())
        self._date_edit.setText(self._format_display_date(match.date_iso))
        for key, combo, value in (
            ("competition", self._competition_combo, match.competition),
            ("home_team", self._home_combo, match.home_team),
            ("away_team", self._away_combo, match.away_team),
            ("location", self._location_combo, getattr(match, "location", "")),
        ):
            combo.setCurrentText(value)
            self._match_field_overrides[key] = bool((value or "").strip())
        self._apply_match_field_styles()

    def persist_memory(self) -> None:
        match = self.current_match()
        segment = self.current_segment()
        state = self.export_state()
        memory = load_memory()
        memory["last_match"] = {
            "date_iso": match.date_iso,
            "competition": match.competition,
            "home_team": match.home_team,
            "away_team": match.away_team,
            "location": match.location,
        }
        memory["last_segment"] = {
            "camera": segment.camera,
            "camera_id": state["kaderblick_camera_id"],
            "video_type_id": state["kaderblick_video_type_id"],
            "side": segment.side,
            "half": segment.half,
            "part": segment.part,
            "type_name": segment.type_name,
        }
        memory["history_competition"] = _add_to_history(memory.get("history_competition", []), match.competition)
        memory["history_home_team"] = _add_to_history(memory.get("history_home_team", []), match.home_team)
        memory["history_away_team"] = _add_to_history(memory.get("history_away_team", []), match.away_team)
        memory["history_location"] = _add_to_history(memory.get("history_location", []), match.location)
        save_memory(memory)

    def _on_field_changed(self, *_args) -> None:
        self._refresh_match_override_state()
        self._update_previews()
        self.metadata_changed.emit()

    @classmethod
    def _normalize_date_override(cls, raw: str) -> str:
        return normalize_date_text(raw)

    @staticmethod
    def _format_display_date(date_iso: str) -> str:
        return format_display_date(date_iso) or (date_iso or "")

    def _update_previews(self) -> None:
        match = self.current_match()
        segment = self.current_segment()

        playlist = build_playlist_title(match)
        title = build_video_title(match, segment)
        description = build_video_description(match, segment)
        self._playlist_preview.setText(playlist or "–")
        self._title_preview.setText(title or "–")
        self._description_preview.setText(description)
        self._set_len_label(self._playlist_len, len(playlist))
        self._set_len_label(self._title_len, len(title))

        kb_bits = []
        kb_type_id = int(self._video_type_combo.currentData() or 0) if self._kb_video_types else self._initial_kb_type_id
        kb_camera_id = int(self._camera_combo.currentData() or 0) if self._kb_cameras else self._initial_kb_camera_id
        if kb_type_id:
            kb_bits.append(f"Typ-ID {kb_type_id}")
        if kb_camera_id:
            kb_bits.append(f"Kamera-ID {kb_camera_id}")
        self._kaderblick_preview.setText(" | ".join(kb_bits) if kb_bits else "–")

    @staticmethod
    def _set_len_label(label: QLabel, length: int) -> None:
        label.setText(f"{length} / 100")
        if length > 100:
            label.setStyleSheet("color: red; font-weight: bold;")
        elif length > 88:
            label.setStyleSheet("color: orange; font-weight: bold;")
        else:
            label.setStyleSheet("color: green;")


class MergeMetadataPanel(OutputMetadataPanel):
    def __init__(self, parent=None, *, settings: AppSettings | None = None):
        super().__init__(
            parent,
            settings=settings,
            info_text="Merge-Metadaten gehören an den Merge-Node. Hier definierst du den gemeinsamen Titel, die Playlist, die Beschreibung und optionale Kaderblick-Zuordnung für das zusammengeführte Ergebnis.",
        )


class YouTubeMetadataPanel(OutputMetadataPanel):
    def __init__(self, parent=None, *, settings: AppSettings | None = None):
        super().__init__(
            parent,
            settings=settings,
            info_text="Für direkte YouTube-Uploads pflegst du hier dieselben Spieldaten, Abschnittsdaten und die Vorschau wie beim Merge. Titel, Playlist, Beschreibung und Tags werden daraus zentral abgeleitet.",
        )