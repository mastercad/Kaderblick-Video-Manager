from __future__ import annotations

from collections.abc import Callable

from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)


def _panel_style() -> str:
    return (
        "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
        "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
    )


class YouTubeUploadPanel(QGroupBox):
    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        on_title_changed: Callable[[str], None],
        on_playlist_changed: Callable[[str], None],
        on_competition_changed: Callable[[str], None],
        on_playlist_helper: Callable[[], None],
    ) -> None:
        super().__init__("YouTube-Upload", parent)
        self.setStyleSheet(_panel_style())
        self._merge_output_mode = False
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(10)

        self._mode_hint = QLabel(
            "Standard-Titel und Playlist gelten für direkte Uploads ohne Merge."
        )
        self._mode_hint.setWordWrap(True)
        self._mode_hint.setStyleSheet("color: #475569;")
        layout.addWidget(self._mode_hint)

        self._standard_fields = QWidget(self)
        form = QFormLayout(self._standard_fields)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)

        self._yt_title_edit = QLineEdit()
        self._yt_title_edit.setPlaceholderText("leer = Dateiname")
        self._yt_title_edit.textChanged.connect(on_title_changed)
        form.addRow("Standard-YT-Titel:", self._yt_title_edit)

        self._yt_playlist_edit = QLineEdit()
        self._yt_playlist_edit.setPlaceholderText("leer = keine Playlist")
        self._yt_playlist_edit.textChanged.connect(on_playlist_changed)
        playlist_row = QHBoxLayout()
        playlist_row.addWidget(self._yt_playlist_edit, 1)
        self._playlist_helper_btn = QPushButton("🎬 Spieldaten …")
        self._playlist_helper_btn.clicked.connect(on_playlist_helper)
        playlist_row.addWidget(self._playlist_helper_btn)
        form.addRow("YouTube-Playlist:", playlist_row)

        self._yt_competition_edit = QLineEdit()
        self._yt_competition_edit.setPlaceholderText("z. B. Sparkassenpokal")
        self._yt_competition_edit.textChanged.connect(on_competition_changed)
        form.addRow("Wettbewerb:", self._yt_competition_edit)
        layout.addWidget(self._standard_fields)

        self._merge_metadata_hint = QLabel(
            "Wenn der Upload aus einem Merge kommt, übernimmt er Titel, Playlist, Beschreibung und lokalen Dateinamen aus den Ausgabe-Metadaten des Merge-Nodes."
        )
        self._merge_metadata_hint.setWordWrap(True)
        self._merge_metadata_hint.setStyleSheet("color: #475569;")
        layout.addWidget(self._merge_metadata_hint)
        self._merge_metadata_hint.hide()

    def set_merge_output_mode(self, enabled: bool) -> None:
        self._merge_output_mode = enabled
        if enabled:
            self._mode_hint.setText(
                "Dieser Upload erhält sein finales Ergebnis aus einem Merge. Die Standardfelder unten sind dafür nicht maßgeblich; pflege stattdessen die Merge-Ausgabe-Metadaten direkt hier im Upload-Bereich."
            )
        else:
            self._mode_hint.setText(
                "Standard-Titel und Playlist gelten für direkte Uploads ohne Merge."
            )
        self._standard_fields.setVisible(not enabled)
        self._merge_metadata_hint.setVisible(enabled)

    def is_merge_output_mode(self) -> bool:
        return self._merge_output_mode


class KaderblickPanel(QGroupBox):
    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        on_game_id_changed: Callable[[str], None],
        on_type_changed: Callable[[int], None],
        on_camera_changed: Callable[[int], None],
        on_reload: Callable[[], None],
    ) -> None:
        super().__init__("Kaderblick", parent)
        self.setStyleSheet(_panel_style())
        form = QFormLayout(self)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)

        self._kb_game_id_edit = QLineEdit()
        self._kb_game_id_edit.setPlaceholderText("z. B. 42")
        self._kb_game_id_edit.textChanged.connect(on_game_id_changed)
        form.addRow("Spiel-ID:", self._kb_game_id_edit)

        self._kb_type_combo = QComboBox()
        self._kb_type_combo.currentIndexChanged.connect(on_type_changed)
        form.addRow("Kaderblick-Video-Typ:", self._kb_type_combo)

        self._kb_camera_combo = QComboBox()
        self._kb_camera_combo.currentIndexChanged.connect(on_camera_changed)
        form.addRow("Kaderblick-Kamera:", self._kb_camera_combo)

        kb_row = QHBoxLayout()
        self._kb_reload_btn = QPushButton("↺ Typen & Kameras laden")
        self._kb_reload_btn.clicked.connect(on_reload)
        kb_row.addWidget(self._kb_reload_btn)
        kb_row.addStretch()
        form.addRow("API-Daten:", kb_row)

        self._kb_status_label = QLabel("")
        self._kb_status_label.setWordWrap(True)
        self._kb_status_label.setStyleSheet("color: #64748B;")
        form.addRow("", self._kb_status_label)


class TitlecardPanel(QGroupBox):
    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        on_home_changed: Callable[[str], None],
        on_away_changed: Callable[[str], None],
        on_date_changed: Callable[[str], None],
        on_duration_changed: Callable[[float], None],
        on_logo_changed: Callable[[str], None],
        on_bg_changed: Callable[[str], None],
        on_fg_changed: Callable[[str], None],
    ) -> None:
        super().__init__("Titelkarte", parent)
        self.setStyleSheet(_panel_style())
        form = QFormLayout(self)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)

        self._tc_home_edit = QLineEdit()
        self._tc_home_edit.textChanged.connect(on_home_changed)
        form.addRow("Heimteam:", self._tc_home_edit)

        self._tc_away_edit = QLineEdit()
        self._tc_away_edit.textChanged.connect(on_away_changed)
        form.addRow("Auswärtsteam:", self._tc_away_edit)

        self._tc_date_edit = QLineEdit()
        self._tc_date_edit.textChanged.connect(on_date_changed)
        form.addRow("Datum:", self._tc_date_edit)

        self._tc_duration_spin = QDoubleSpinBox()
        self._tc_duration_spin.setRange(0.5, 10.0)
        self._tc_duration_spin.setSingleStep(0.5)
        self._tc_duration_spin.setSuffix(" s")
        self._tc_duration_spin.valueChanged.connect(on_duration_changed)
        form.addRow("Dauer:", self._tc_duration_spin)

        self._tc_logo_edit = QLineEdit()
        self._tc_logo_edit.setPlaceholderText("Pfad zum Logo-Bild")
        self._tc_logo_edit.textChanged.connect(on_logo_changed)
        form.addRow("Logo:", self._tc_logo_edit)

        self._tc_bg_edit = QLineEdit()
        self._tc_bg_edit.setPlaceholderText("#000000")
        self._tc_bg_edit.textChanged.connect(on_bg_changed)
        form.addRow("Hintergrund:", self._tc_bg_edit)

        self._tc_fg_edit = QLineEdit()
        self._tc_fg_edit.setPlaceholderText("#FFFFFF")
        self._tc_fg_edit.textChanged.connect(on_fg_changed)
        form.addRow("Schrift:", self._tc_fg_edit)


class YTVersionPanel(QGroupBox):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("YT-Version", parent)
        self.setStyleSheet(_panel_style())
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)
        info = QLabel(
            "Die YouTube-Version erzeugt eine upload-optimierte Ausgabe auf Basis der aktuellen Verarbeitungskette. Eigene Zusatzparameter sind hier derzeit nicht nötig."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: #475569;")
        layout.addWidget(info)
        layout.addStretch()


class RepairPanel(QGroupBox):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("Reparatur", parent)
        self.setStyleSheet(_panel_style())
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)
        info = QLabel(
            "Der Reparatur-Node erzeugt aus dem aktuellen Ergebnis eine bereinigte MP4-Arbeitskopie. "
            "Bereits kompatible H.264/AAC-Dateien werden bevorzugt verlustfrei neu gemuxt; ansonsten wird eine "
            "standardisierte Ersatzdatei gebaut und sofort validiert."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: #475569;")
        layout.addWidget(info)
        note = QLabel(
            "Praktisch vor YT-Version oder Upload, wenn vorhandene MP4-Dateien problematische Zusatzstreams, "
            "Zeitstempelprobleme oder unklare Containerzustände haben."
        )
        note.setWordWrap(True)
        note.setStyleSheet("color: #64748B;")
        layout.addWidget(note)
        layout.addStretch()


class ValidationPanel(QGroupBox):
    def __init__(self, title: str, description: str, parent: QWidget | None = None) -> None:
        super().__init__(title, parent)
        self.setStyleSheet(_panel_style())
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)
        info = QLabel(description)
        info.setWordWrap(True)
        info.setStyleSheet("color: #475569;")
        layout.addWidget(info)
        note = QLabel(
            "Jeder Prüf-Node hat drei Ausgänge: OK, reparierbar und irreparabel. Verbinde Branches explizit im Canvas, idealerweise mit Cleanup, Reparatur oder Stop / Log."
        )
        note.setWordWrap(True)
        note.setStyleSheet("color: #64748B;")
        layout.addWidget(note)
        layout.addStretch()


class CleanupPanel(QGroupBox):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("Cleanup", parent)
        self.setStyleSheet(_panel_style())
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)
        info = QLabel(
            "Der Cleanup-Node entfernt alte abgeleitete Dateien wie _youtube, _repaired, _titlecard oder temporäre Altlasten, bevor der Branch neue Ergebnisse erzeugt."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: #475569;")
        layout.addWidget(info)
        note = QLabel(
            "Gedacht als sicherer Aufräumschritt vor Reparatur, YT-Version oder Upload. Die Quelldatei selbst bleibt unangetastet."
        )
        note.setWordWrap(True)
        note.setStyleSheet("color: #64748B;")
        layout.addWidget(note)
        layout.addStretch()


class StopPanel(QGroupBox):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("Stop / Log", parent)
        self.setStyleSheet(_panel_style())
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)
        info = QLabel(
            "Der Stop / Log-Node schreibt einen klaren Abschluss ins Log und beendet diesen Branch ohne weitere Verarbeitung oder Uploads."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: #475569;")
        layout.addWidget(info)
        note = QLabel(
            "Praktisch als Ziel für den irreparabel-Branch eines Prüf-Nodes oder als bewusstes Branch-Ende nach Cleanup."
        )
        note.setWordWrap(True)
        note.setStyleSheet("color: #64748B;")
        layout.addWidget(note)
        layout.addStretch()


class ProcessingPanel(QGroupBox):
    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        encoder_choices: list[tuple[str, str]],
        on_crf_changed: Callable[[int], None],
        on_encoder_changed: Callable[[int], None],
        on_preset_changed: Callable[[str], None],
        on_fps_changed: Callable[[int], None],
        on_format_changed: Callable[[str], None],
        on_merge_audio_changed: Callable[[bool], None],
        on_amplify_toggled: Callable[[bool], None],
        on_amplify_db_changed: Callable[[float], None],
        on_audio_sync_changed: Callable[[bool], None],
    ) -> None:
        super().__init__("Verarbeitung und Audio", parent)
        self.setStyleSheet(_panel_style())
        form = QFormLayout(self)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)

        self._crf_spin = QSpinBox()
        self._crf_spin.setRange(0, 51)
        self._crf_spin.valueChanged.connect(on_crf_changed)
        form.addRow("CRF:", self._crf_spin)

        self._encoder_combo = QComboBox()
        for enc_id, enc_name in encoder_choices:
            self._encoder_combo.addItem(enc_name, enc_id)
        self._encoder_combo.currentIndexChanged.connect(on_encoder_changed)
        form.addRow("Encoder:", self._encoder_combo)

        self._preset_combo = QComboBox()
        self._preset_combo.addItems([
            "ultrafast", "superfast", "veryfast", "faster", "fast",
            "medium", "slow", "slower", "veryslow",
        ])
        self._preset_combo.currentTextChanged.connect(on_preset_changed)
        form.addRow("Preset:", self._preset_combo)

        self._fps_spin = QSpinBox()
        self._fps_spin.setRange(1, 120)
        self._fps_spin.valueChanged.connect(on_fps_changed)
        form.addRow("Framerate:", self._fps_spin)

        self._format_combo = QComboBox()
        self._format_combo.addItems(["mp4", "avi"])
        self._format_combo.currentTextChanged.connect(on_format_changed)
        form.addRow("Format:", self._format_combo)

        self._merge_audio_cb = QCheckBox("Separate Audio-Spur zusammenführen")
        self._merge_audio_cb.toggled.connect(on_merge_audio_changed)
        form.addRow("Audio-Mix:", self._merge_audio_cb)

        amp_row = QHBoxLayout()
        self._amplify_audio_cb = QCheckBox("Lautstärke anpassen")
        self._amplify_audio_cb.toggled.connect(on_amplify_toggled)
        amp_row.addWidget(self._amplify_audio_cb)
        self._amplify_db_spin = QDoubleSpinBox()
        self._amplify_db_spin.setRange(-20.0, 40.0)
        self._amplify_db_spin.setSingleStep(1.0)
        self._amplify_db_spin.setDecimals(1)
        self._amplify_db_spin.setSuffix(" dB")
        self._amplify_db_spin.valueChanged.connect(on_amplify_db_changed)
        amp_row.addWidget(self._amplify_db_spin)
        amp_row.addStretch()
        form.addRow("Pegel:", amp_row)

        self._audio_sync_cb = QCheckBox("Audio-Sync / Frame-Drop-Korrektur")
        self._audio_sync_cb.toggled.connect(on_audio_sync_changed)
        form.addRow("", self._audio_sync_cb)