"""YouTube-Metadaten-Editor: Strukturierter Generator für Titel und Playlists.

Generiert konsistente YouTube-Titel und Playlist-Namen für Spielberichte:

  Playlist:  DD.MM.YYYY | Wettbewerb | Heimteam vs Auswärtsteam
  Titel:     YYYY-MM-DD | Heimteam vs Auswärtsteam | [Kamera | ][Seite ]N. Halbzeit[ Teil X]

Verwendung
----------
  dlg = YouTubeTitleEditorDialog(parent)
  if dlg.exec():
      playlist = dlg.playlist_title   # → "28.03.2026 | Sparkassenpokal | ..."
      title    = dlg.video_title      # → "2026-03-28 | ... | 1. Halbzeit Teil 1"
"""

import json
import re
from dataclasses import dataclass
from datetime import date
from typing import Optional

from PySide6.QtCore import QDate, Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QButtonGroup, QCheckBox, QComboBox, QDialog, QDialogButtonBox,
    QDateEdit, QFormLayout, QFrame, QGroupBox, QHBoxLayout, QLabel,
    QRadioButton, QSpinBox, QVBoxLayout,
)

from .state_store import load_section, save_section

CAMERA_PRESETS = [
    ("(Keine Kamera)", ""),
    ("DJI Osmo Action5 Pro",  "DJI Osmo Action5 Pro"),
    ("Kaderblick Links",       "Kaderblick Links"),
    ("Kaderblick Rechts",      "Kaderblick Rechts"),
]

MAX_LEN = 100


# ═════════════════════════════════════════════════════════════════
#  Datenklassen
# ═════════════════════════════════════════════════════════════════

@dataclass
class MatchData:
    """Metadaten eines Spiels (gemeinsam für Playlist und alle Videos)."""
    date_iso: str = ""       # YYYY-MM-DD
    competition: str = ""
    home_team: str = ""
    away_team: str = ""


@dataclass
class SegmentData:
    """Metadaten eines einzelnen Video-Abschnitts."""
    camera: str = ""         # "" = keine Kamera
    side: str = ""           # "" | "Links" | "Rechts"
    half: int = 1            # 1 oder 2
    part: int = 0            # 0 = kein Teil
    type_name: str = ""      # vollständiger Typ-Name (z. B. "1. Halbzeit", "Vorbereitung")


# ═════════════════════════════════════════════════════════════════
#  Generator-Funktionen (auch extern nutzbar)
# ═════════════════════════════════════════════════════════════════

def _teams_str(match: MatchData) -> str:
    """Gibt 'Heimteam vs Auswärtsteam' zurück, oder einen der Teamnamen falls nur einer gesetzt."""
    if match.home_team and match.away_team:
        return f"{match.home_team} vs {match.away_team}"
    return match.home_team or match.away_team


def _unique_nonempty(items: list[str]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in items:
        value = str(item or "").strip()
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(value)
    return cleaned


def _hashtag_from_tag(tag: str) -> str:
    compact = "".join(char for char in tag if char.isalnum() or char in "äöüÄÖÜß")
    return f"#{compact}" if compact else ""


def build_playlist_title(match: MatchData) -> str:
    """DD.MM.YYYY | Wettbewerb | Heimteam vs Auswärtsteam"""
    parts = []
    if match.date_iso:
        try:
            d = date.fromisoformat(match.date_iso)
            parts.append(d.strftime("%d.%m.%Y"))
        except ValueError:
            parts.append(match.date_iso)
    if match.competition:
        parts.append(match.competition)
    teams = _teams_str(match)
    if teams:
        parts.append(teams)
    return " | ".join(parts)[:MAX_LEN]


def build_video_title(match: MatchData, seg: SegmentData) -> str:
    """Baut den YouTube-Titel mit Prioritäts-Fallback (max. 100 Zeichen).

    Pflichtfelder: Datum, Teams, Video-Typ
    Fallback-Reihenfolge: Kamera → Teil → Seite
    """
    type_label = seg.type_name or f"{seg.half}. Halbzeit"
    date_str = match.date_iso or ""
    teams = _teams_str(match)

    def _fmt(with_camera: bool, with_part: bool, with_side: bool) -> str:
        section = (f"{seg.side} " if with_side and seg.side else "") + type_label
        if with_part and seg.part:
            section += f" Teil {seg.part}"
        return " | ".join(p for p in [
            date_str, teams, seg.camera if with_camera else "", section
        ] if p)

    for flags in [
        (True,  True,  True),   # vollständig
        (False, True,  True),   # ohne Kamera
        (False, False, True),   # ohne Kamera + Teil
        (False, False, False),  # nur Datum | Teams | Typ
    ]:
        t = _fmt(*flags)
        if len(t) <= MAX_LEN:
            return t

    return _fmt(False, False, False)[:MAX_LEN]


def build_video_description(match: MatchData, seg: SegmentData) -> str:
    """Generiert die YouTube-Beschreibung mit vollständigen Metadaten und Hashtags."""
    lines = []

    if match.date_iso:
        try:
            d = date.fromisoformat(match.date_iso)
            lines.append(f"📅 {d.strftime('%d.%m.%Y')}")
        except ValueError:
            lines.append(f"📅 {match.date_iso}")

    teams = _teams_str(match)
    if teams:
        lines.append(f"⚽ {teams}")

    if match.competition:
        lines.append(f"🏆 {match.competition}")

    type_label = seg.type_name or f"{seg.half}. Halbzeit"
    section = type_label
    if seg.part:
        section += f" Teil {seg.part}"
    if seg.side:
        section += f" | Seite: {seg.side}"
    lines.append(f"🎬 {section}")

    if seg.camera:
        lines.append(f"📷 {seg.camera}")

    hashtags = _unique_nonempty([_hashtag_from_tag(tag) for tag in build_video_tags(match, seg)])

    lines.append("")
    if hashtags:
        lines.append(" ".join(hashtags))

    return "\n".join(lines)


def build_video_tags(match: MatchData, seg: SegmentData) -> list[str]:
    """Generiert YouTube-Tags (separates Feld, nicht in der Beschreibung)."""
    tags = ["Fußball", "Fussball", "Sport"]
    tags.extend([match.competition, match.home_team, match.away_team])

    if seg.camera:
        tags.append(seg.camera)
    if seg.side:
        tags.append(seg.side)

    type_label = seg.type_name or f"{seg.half}. Halbzeit"
    tags.append(type_label)
    if seg.part:
        tags.append(f"Teil {seg.part}")

    compact_tags = []
    for raw in list(tags):
        compact = "".join(c for c in raw if c.isalnum() or c in "äöüÄÖÜß")
        if compact and compact.casefold() != str(raw).strip().casefold():
            compact_tags.append(compact)

    return _unique_nonempty(tags + compact_tags)


_FILENAME_INVALID_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1F]+')


def build_output_filename_from_title(title: str, *, fallback: str = "video", max_len: int = 120) -> str:
    normalized = (title or "").strip()
    if not normalized:
        normalized = fallback
    normalized = normalized.replace(" | ", " - ")
    normalized = normalized.replace("|", "-")
    normalized = _FILENAME_INVALID_CHARS.sub(" ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip(" .")
    if not normalized:
        normalized = fallback
    return normalized[:max_len].rstrip(" .") or fallback


# ═════════════════════════════════════════════════════════════════
#  Persistenz (Verlauf / zuletzt verwendete Werte)
# ═════════════════════════════════════════════════════════════════

def load_memory() -> dict:
    return load_section("title_editor_memory")


def save_memory(data: dict) -> None:
    save_section("title_editor_memory", data)


def _add_to_history(history: list, value: str, max_items: int = 20) -> list:
    value = value.strip()
    if not value:
        return history
    return [value] + [v for v in history if v != value][:max_items - 1]


# ═════════════════════════════════════════════════════════════════
#  Dialog
# ═════════════════════════════════════════════════════════════════

class YouTubeTitleEditorDialog(QDialog):
    """Strukturierter Editor für YouTube-Titel und Playlist-Namen.

    Modi
    ----
    "full"     – Spieldaten + Video-Abschnitt (Standard, für pro-Datei-Editor)
    "playlist" – Nur Spieldaten (für Playlist-Feld im Job-Editor)
    """

    def __init__(self, parent=None, *,
                 mode: str = "full",
                 initial_match: Optional[MatchData] = None,
                 initial_segment: Optional[SegmentData] = None,
                 auto_increment_part: bool = False,
                 kb_video_types: Optional[list] = None,
                 kb_cameras: Optional[list] = None,
                 initial_kb_type_id: int = 0,
                 initial_kb_camera_id: int = 0):
        super().__init__(parent)
        self.setWindowTitle("YouTube-Metadaten")
        self.setMinimumWidth(540)
        self._mode = mode
        self._kb_video_types = kb_video_types or []
        self._kb_cameras = kb_cameras or []

        mem = load_memory()
        last_m = mem.get("last_match", {})
        last_s = mem.get("last_segment", {})

        self._match = initial_match or MatchData(
            date_iso=last_m.get("date_iso", date.today().isoformat()),
            competition=last_m.get("competition", ""),
            home_team=last_m.get("home_team", ""),
            away_team=last_m.get("away_team", ""),
        )

        # Segment aus Memory laden; bei auto_increment_part das Teil erhöhen
        _last_part = last_s.get("part", 0)
        _next_part  = (_last_part + 1) if (auto_increment_part and _last_part > 0) else _last_part
        self._segment = initial_segment or SegmentData(
            camera=last_s.get("camera", ""),
            side=last_s.get("side", ""),
            half=last_s.get("half", 1),
            part=_next_part,
            type_name=last_s.get("type_name", ""),
        )
        # KB-IDs aus Memory vorbelegen (werden in _populate per ID im Combo gesucht)
        if initial_kb_type_id == 0:
            initial_kb_type_id = last_s.get("video_type_id", 0)
        if initial_kb_camera_id == 0:
            initial_kb_camera_id = last_s.get("camera_id", 0)

        self._initial_kb_type_id = initial_kb_type_id
        self._initial_kb_camera_id = initial_kb_camera_id
        self._result_kb_type_id = initial_kb_type_id
        self._result_kb_camera_id = initial_kb_camera_id
        self._histories = {
            "competition": mem.get("history_competition", []),
            "home_team":   mem.get("history_home_team",   []),
            "away_team":   mem.get("history_away_team",   []),
        }

        # Ergebnisse (werden in _accept gesetzt)
        self._result_match   = self._match
        self._result_segment = self._segment

        self._build_ui()
        self._populate()
        self._update_previews()

    # ── Ergebnisse ────────────────────────────────────────────

    @property
    def kb_video_type_id(self) -> int:
        return self._result_kb_type_id

    @property
    def kb_camera_id(self) -> int:
        return self._result_kb_camera_id

    @property
    def playlist_title(self) -> str:
        return build_playlist_title(self._result_match)

    @property
    def video_title(self) -> str:
        return build_video_title(self._result_match, self._result_segment)

    @property
    def video_description(self) -> str:
        return build_video_description(self._result_match, self._result_segment)

    @property
    def match_data(self) -> MatchData:
        return self._result_match

    @property
    def segment_data(self) -> SegmentData:
        return self._result_segment

    # ── UI aufbauen ───────────────────────────────────────────

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        layout.addWidget(self._build_match_group())
        if self._mode == "full":
            layout.addWidget(self._build_segment_group())

        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setFrameShadow(QFrame.Sunken)
        layout.addWidget(sep)

        layout.addWidget(self._build_preview_group())

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.button(QDialogButtonBox.Ok).setText("Übernehmen")
        btns.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        btns.accepted.connect(self._accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _build_match_group(self) -> QGroupBox:
        grp = QGroupBox("Spieldaten")
        form = QFormLayout(grp)
        form.setLabelAlignment(Qt.AlignRight)

        self._date_edit = QDateEdit()
        self._date_edit.setCalendarPopup(True)
        self._date_edit.setDisplayFormat("dd.MM.yyyy")
        self._date_edit.dateChanged.connect(self._update_previews)
        form.addRow("Datum:", self._date_edit)

        self._competition_combo = self._make_history_combo("competition",
                                                            "z. B. Sparkassenpokal")
        form.addRow("Wettbewerb:", self._competition_combo)

        self._home_combo = self._make_history_combo("home_team",
                                                     "z. B. SpG Wurgwitz/SG 90 Braunsdorf")
        form.addRow("Heimteam:", self._home_combo)

        self._away_combo = self._make_history_combo("away_team",
                                                     "z. B. SSV 1862 Langburkersdorf U19")
        form.addRow("Auswärtsteam:", self._away_combo)

        return grp

    def _build_segment_group(self) -> QGroupBox:
        grp = QGroupBox("Video-Abschnitt")
        layout = QVBoxLayout(grp)
        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight)

        # Kamera → Kaderblick-Kamera (API-Werte) oder statische Presets als Fallback
        self._camera_combo = QComboBox()
        if self._kb_cameras:
            self._camera_combo.addItem("–", 0)
            for cam in self._kb_cameras:
                self._camera_combo.addItem(cam.get("name", ""), cam.get("id", 0))
        else:
            self._camera_combo.setEditable(True)
            self._camera_combo.setInsertPolicy(QComboBox.NoInsert)
            for label, val in CAMERA_PRESETS:
                self._camera_combo.addItem(label, val)
        self._camera_combo.currentIndexChanged.connect(self._update_previews)
        self._camera_combo.editTextChanged.connect(self._update_previews)
        form.addRow("Kamera:", self._camera_combo)

        # Seite
        side_row = QHBoxLayout()
        self._side_group = QButtonGroup(self)
        for label, val in [("Keine", ""), ("Links", "Links"), ("Rechts", "Rechts")]:
            rb = QRadioButton(label)
            rb.setProperty("side_value", val)
            self._side_group.addButton(rb)
            side_row.addWidget(rb)
        side_row.addStretch()
        self._side_group.buttonToggled.connect(lambda *_: self._update_previews())
        form.addRow("Seite:", side_row)

        # Video-Typ / Halbzeit → Kaderblick-Videotyp (API-Werte) oder statische Fallbacks
        self._video_type_combo = QComboBox()
        if self._kb_video_types:
            self._video_type_combo.addItem("–", 0)
            for vt in self._kb_video_types:
                self._video_type_combo.addItem(vt.get("name", ""), vt.get("id", 0))
        else:
            for name, id_ in [("1. Halbzeit", 0), ("2. Halbzeit", 0),
                               ("Vorbereitung", 0), ("Nachbereitung", 0)]:
                self._video_type_combo.addItem(name, id_)
        self._video_type_combo.currentIndexChanged.connect(self._update_previews)
        form.addRow("Video-Typ:", self._video_type_combo)

        # Teil (optional)
        part_row = QHBoxLayout()
        self._part_cb = QCheckBox("Teil-Nummer:")
        self._part_spin = QSpinBox()
        self._part_spin.setRange(1, 20)
        self._part_spin.setValue(1)
        self._part_spin.setFixedWidth(65)
        self._part_spin.setEnabled(False)
        self._part_cb.toggled.connect(self._part_spin.setEnabled)
        self._part_cb.toggled.connect(self._update_previews)
        self._part_spin.valueChanged.connect(self._update_previews)
        part_row.addWidget(self._part_cb)
        part_row.addWidget(self._part_spin)
        part_row.addWidget(QLabel("(leer = komplette Halbzeit in einem Video)"))
        part_row.addStretch()
        form.addRow("Teil:", part_row)

        layout.addLayout(form)
        return grp

    def _build_preview_group(self) -> QGroupBox:
        grp = QGroupBox("Vorschau")
        form = QFormLayout(grp)
        mono = QFont("Monospace", 9)

        self._playlist_preview = QLabel()
        self._playlist_preview.setWordWrap(True)
        self._playlist_preview.setFont(mono)
        self._playlist_preview.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self._playlist_len_lbl = QLabel()
        self._playlist_len_lbl.setFixedWidth(55)
        self._playlist_len_lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        pl_row = QHBoxLayout()
        pl_row.addWidget(self._playlist_preview, 1)
        pl_row.addWidget(self._playlist_len_lbl)
        form.addRow("Playlist:", pl_row)

        if self._mode == "full":
            self._title_preview = QLabel()
            self._title_preview.setWordWrap(True)
            self._title_preview.setFont(mono)
            self._title_preview.setTextInteractionFlags(Qt.TextSelectableByMouse)
            self._title_len_lbl = QLabel()
            self._title_len_lbl.setFixedWidth(55)
            self._title_len_lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
            tt_row = QHBoxLayout()
            tt_row.addWidget(self._title_preview, 1)
            tt_row.addWidget(self._title_len_lbl)
            form.addRow("Titel:", tt_row)

            from PySide6.QtWidgets import QPlainTextEdit
            self._desc_preview = QPlainTextEdit()
            self._desc_preview.setReadOnly(True)
            self._desc_preview.setFont(mono)
            self._desc_preview.setFixedHeight(110)
            self._desc_preview.setToolTip(
                "Automatisch generierte YouTube-Beschreibung mit Spieldaten und Hashtags.\n"
                "Diese Beschreibung wird beim Upload übernommen.")
            form.addRow("Beschreibung:", self._desc_preview)

        return grp

    # ── Felder befüllen ───────────────────────────────────────

    def _populate(self) -> None:
        m = self._match
        s = self._segment

        # Spieldaten
        d_iso = m.date_iso or date.today().isoformat()
        try:
            d = date.fromisoformat(d_iso)
            self._date_edit.setDate(QDate(d.year, d.month, d.day))
        except ValueError:
            self._date_edit.setDate(QDate.currentDate())

        self._competition_combo.setCurrentText(m.competition)
        self._home_combo.setCurrentText(m.home_team)
        self._away_combo.setCurrentText(m.away_team)

        if self._mode != "full":
            return

        # Kamera: erst per ID (API), dann per Name (Fallback)
        if self._kb_cameras:
            idx = self._camera_combo.findData(self._initial_kb_camera_id)
        else:
            idx = self._camera_combo.findData(s.camera)
        if idx >= 0:
            self._camera_combo.setCurrentIndex(idx)
        elif hasattr(self._camera_combo, 'setCurrentText'):
            self._camera_combo.setCurrentText(s.camera)

        # Seite
        for btn in self._side_group.buttons():
            if btn.property("side_value") == s.side:
                btn.setChecked(True)
                break

        # Video-Typ: erst per initialem KB-Typ, dann per half-Ableitung
        idx = self._video_type_combo.findData(self._initial_kb_type_id)
        if idx < 0:
            # Halbzeit aus Memory auf Typnamen mappen
            idx = self._video_type_combo.findText(f"{s.half}. Halbzeit")
        if idx >= 0:
            self._video_type_combo.setCurrentIndex(idx)

        # Teil
        if s.part:
            self._part_cb.setChecked(True)
            self._part_spin.setValue(s.part)

    # ── Live-Vorschau ─────────────────────────────────────────

    def _current_match(self) -> MatchData:
        qd = self._date_edit.date()
        return MatchData(
            date_iso=f"{qd.year():04d}-{qd.month():02d}-{qd.day():02d}",
            competition=self._competition_combo.currentText().strip(),
            home_team=self._home_combo.currentText().strip(),
            away_team=self._away_combo.currentText().strip(),
        )

    def _current_segment(self) -> SegmentData:
        if self._mode != "full":
            return SegmentData()

        # Kamera: bei API-Daten den Namen aus dem Combo-Text, sonst Data-Wert
        if self._kb_cameras:
            camera = self._camera_combo.currentText()
            if camera == "–":
                camera = ""
        else:
            cam_data = self._camera_combo.currentData()
            camera = cam_data if cam_data is not None else self._camera_combo.currentText().strip()

        side = ""
        for btn in self._side_group.buttons():
            if btn.isChecked():
                side = btn.property("side_value")
                break

        # half aus dem Typnamen ableiten: "1." → 1, "2." → 2, sonst 1
        type_name = self._video_type_combo.currentText()
        if type_name == "–":
            type_name = ""
        half = 2 if type_name.startswith("2.") else 1

        part = self._part_spin.value() if self._part_cb.isChecked() else 0
        return SegmentData(camera=camera, side=side, half=half, part=part,
                           type_name=type_name)

    def _update_previews(self) -> None:
        m = self._current_match()
        s = self._current_segment()

        pl = build_playlist_title(m)
        self._playlist_preview.setText(pl or "–")
        self._set_len_label(self._playlist_len_lbl, len(pl))

        if self._mode == "full":
            vt = build_video_title(m, s)
            self._title_preview.setText(vt or "–")
            self._set_len_label(self._title_len_lbl, len(vt))
            self._desc_preview.setPlainText(build_video_description(m, s))

    @staticmethod
    def _set_len_label(label: QLabel, n: int) -> None:
        label.setText(f"{n} / 100")
        if n > 100:
            label.setStyleSheet("color: red; font-weight: bold;")
        elif n > 88:
            label.setStyleSheet("color: orange; font-weight: bold;")
        else:
            label.setStyleSheet("color: green;")

    # ── ComboBox mit History ──────────────────────────────────

    def _make_history_combo(self, key: str, placeholder: str = "") -> QComboBox:
        combo = QComboBox()
        combo.setEditable(True)
        combo.setInsertPolicy(QComboBox.NoInsert)
        combo.lineEdit().setPlaceholderText(placeholder)
        for v in self._histories.get(key, []):
            combo.addItem(v)
        combo.setCurrentText("")
        combo.currentTextChanged.connect(self._update_previews)
        return combo

    # ── Akzeptieren & Speichern ───────────────────────────────

    def _accept(self) -> None:
        self._result_match   = self._current_match()
        self._result_segment = self._current_segment()
        if self._mode == "full":
            self._result_kb_type_id   = self._video_type_combo.currentData()  or 0
            if self._kb_cameras:
                self._result_kb_camera_id = self._camera_combo.currentData() or 0
            else:
                self._result_kb_camera_id = self._initial_kb_camera_id

        mem = load_memory()
        mem["last_match"] = {
            "date_iso":    self._result_match.date_iso,
            "competition": self._result_match.competition,
            "home_team":   self._result_match.home_team,
            "away_team":   self._result_match.away_team,
        }
        mem["last_segment"] = {
            "camera":         self._result_segment.camera,
            "camera_id":      self._result_kb_camera_id,
            "video_type_id":  self._result_kb_type_id,
            "side":           self._result_segment.side,
            "half":           self._result_segment.half,
            "part":           self._result_segment.part,
            "type_name":      self._result_segment.type_name,
        }
        for key, val in [
            ("history_competition", self._result_match.competition),
            ("history_home_team",   self._result_match.home_team),
            ("history_away_team",   self._result_match.away_team),
        ]:
            mem[key] = _add_to_history(mem.get(key, []), val)

        save_memory(mem)
        self.accept()
