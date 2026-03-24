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

from .merge import YouTubeMetadataPanel
from ...settings.profiles import (
    PROFILES,
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
    VIDEO_TOOLTIP_PRESET,
    VIDEO_TOOLTIP_RESOLUTION,
    matching_profile_name,
)


STEP_CONTAINER_OPTIONS = [("source", "Originalcontainer"), *VIDEO_FORMAT_OPTIONS]


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
        on_metadata_changed: Callable[[], None],
        on_playlist_helper: Callable[[], None],
    ) -> None:
        super().__init__("YouTube-Upload", parent)
        self.setStyleSheet(_panel_style())
        self._merge_output_mode = False
        self._upload_enabled = False
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(10)

        self._mode_hint = QLabel(
            "Für direkte Uploads bearbeitest du hier dieselben Metadaten wie beim Merge."
        )
        self._mode_hint.setWordWrap(True)
        self._mode_hint.setStyleSheet("color: #475569;")
        layout.addWidget(self._mode_hint)

        helper_row = QHBoxLayout()
        helper_row.addWidget(QLabel("Spieldaten:", self))
        self._playlist_helper_btn = QPushButton("🎬 Editor öffnen")
        self._playlist_helper_btn.clicked.connect(on_playlist_helper)
        helper_row.addWidget(self._playlist_helper_btn)
        helper_row.addStretch()
        layout.addLayout(helper_row)

        self._metadata_panel = YouTubeMetadataPanel(self)
        self._metadata_panel.metadata_changed.connect(on_metadata_changed)
        layout.addWidget(self._metadata_panel)

        self._merge_metadata_hint = QLabel(
            "Wenn der Upload aus einem Merge kommt, zeigt dieser Bereich dieselben Merge-Metadaten nur zur Kontrolle an. Die Bearbeitung bleibt am Merge-Node zentralisiert."
        )
        self._merge_metadata_hint.setWordWrap(True)
        self._merge_metadata_hint.setStyleSheet("color: #475569;")
        layout.addWidget(self._merge_metadata_hint)
        self._merge_metadata_hint.hide()

    def set_merge_output_mode(self, enabled: bool) -> None:
        self._merge_output_mode = enabled
        if enabled:
            self._mode_hint.setText(
                "Dieser Upload erhält sein finales Ergebnis aus einem Merge. Deshalb siehst du hier dieselbe zentrale Metadaten-Maske, sie bleibt aber am Merge-Node bearbeitbar."
            )
        else:
            self._mode_hint.setText(
                "Für direkte Uploads bearbeitest du hier dieselben Metadaten wie beim Merge."
            )
        self._metadata_panel.setEnabled(self._upload_enabled and not enabled)
        self._playlist_helper_btn.setVisible(not enabled)
        self._playlist_helper_btn.setEnabled(self._upload_enabled and not enabled)
        self._merge_metadata_hint.setVisible(enabled)

    def is_merge_output_mode(self) -> bool:
        return self._merge_output_mode

    def sync_enabled_state(self, enabled: bool) -> None:
        self._upload_enabled = enabled
        self._metadata_panel.setEnabled(enabled and not self._merge_output_mode)
        self._playlist_helper_btn.setEnabled(enabled and not self._merge_output_mode)


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

        hint = QLabel(
            "Video-Typ und Kamera folgen automatisch den aktiven Output-Metadaten aus YouTube-Upload oder Merge."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #475569;")
        form.addRow("", hint)

        self._kb_game_id_edit = QLineEdit()
        self._kb_game_id_edit.setPlaceholderText("z. B. 42")
        self._kb_game_id_edit.textChanged.connect(on_game_id_changed)
        form.addRow("Spiel-ID:", self._kb_game_id_edit)

        self._kb_type_combo = QComboBox()
        self._kb_type_combo.setEnabled(False)
        self._kb_type_combo.currentIndexChanged.connect(on_type_changed)
        form.addRow("Kaderblick-Video-Typ:", self._kb_type_combo)

        self._kb_camera_combo = QComboBox()
        self._kb_camera_combo.setEnabled(False)
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


class StepEncodingPanel(QGroupBox):
    def __init__(
        self,
        title: str,
        parent: QWidget | None = None,
        *,
        on_preset_changed: Callable[[str], None],
        on_no_bframes_changed: Callable[[bool], None],
        on_format_changed: Callable[[str], None],
        on_resolution_changed: Callable[[str], None],
    ) -> None:
        super().__init__(title, parent)
        self.setStyleSheet(_panel_style())
        self._on_preset_changed = on_preset_changed
        self._on_no_bframes_changed = on_no_bframes_changed
        self._on_format_changed = on_format_changed
        self._on_resolution_changed = on_resolution_changed
        self._updating_profile = False

        form = QFormLayout(self)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)

        self._profile_combo = QComboBox()
        self._profile_combo.addItems(list(PROFILES.keys()))
        self._profile_combo.currentTextChanged.connect(self._apply_profile)
        form.addRow(VIDEO_LABEL_PROFILE, self._profile_combo)

        self._preset_combo = QComboBox()
        self._preset_combo.addItems(VIDEO_PRESET_OPTIONS)
        self._preset_combo.currentTextChanged.connect(self._handle_preset_changed)
        self._preset_combo.setToolTip(VIDEO_TOOLTIP_PRESET)
        form.addRow(VIDEO_LABEL_PRESET, self._preset_combo)

        self._resolution_combo = QComboBox()
        for value, label in VIDEO_RESOLUTION_OPTIONS:
            self._resolution_combo.addItem(label, value)
        self._resolution_combo.currentIndexChanged.connect(self._handle_resolution_changed)
        self._resolution_combo.setToolTip(VIDEO_TOOLTIP_RESOLUTION)
        form.addRow(VIDEO_LABEL_RESOLUTION, self._resolution_combo)

        self._format_combo = QComboBox()
        for value, label in STEP_CONTAINER_OPTIONS:
            self._format_combo.addItem(label, value)
        self._format_combo.currentIndexChanged.connect(self._handle_format_changed)
        self._format_combo.setToolTip(VIDEO_TOOLTIP_CONTAINER)
        form.addRow(VIDEO_LABEL_CONTAINER, self._format_combo)

        self._no_bframes_cb = QCheckBox(VIDEO_TEXT_NO_BFRAMES)
        self._no_bframes_cb.setToolTip(VIDEO_TOOLTIP_NO_BFRAMES)
        self._no_bframes_cb.toggled.connect(self._handle_no_bframes_changed)
        form.addRow("", self._no_bframes_cb)

    def _set_profile_name(self, profile_name: str) -> None:
        self._profile_combo.blockSignals(True)
        self._profile_combo.setCurrentText(profile_name)
        self._profile_combo.blockSignals(False)

    def _mark_custom_profile(self) -> None:
        if self._updating_profile:
            return
        self._set_profile_name("Benutzerdefiniert")

    def _apply_profile(self, profile_name: str) -> None:
        values = PROFILES.get(profile_name, {})
        if not values:
            return
        self._updating_profile = True
        try:
            if "preset" in values:
                self._preset_combo.setCurrentText(str(values["preset"]))
            if "no_bframes" in values:
                self._no_bframes_cb.setChecked(bool(values["no_bframes"]))
        finally:
            self._updating_profile = False

    def load_values(self, *, preset: str, no_bframes: bool, output_format: str, output_resolution: str) -> None:
        self._updating_profile = True
        try:
            self._preset_combo.setCurrentText(preset)
            self._no_bframes_cb.setChecked(no_bframes)
            self._format_combo.setCurrentIndex(max(self._format_combo.findData(output_format), 0))
            self._resolution_combo.setCurrentIndex(max(self._resolution_combo.findData(output_resolution), 0))
            self._set_profile_name(
                matching_profile_name(
                    {"preset": preset, "no_bframes": no_bframes},
                    ("preset", "no_bframes"),
                )
            )
        finally:
            self._updating_profile = False

    def _handle_preset_changed(self, value: str) -> None:
        self._mark_custom_profile()
        self._on_preset_changed(value)

    def _handle_no_bframes_changed(self, checked: bool) -> None:
        self._mark_custom_profile()
        self._on_no_bframes_changed(checked)

    def _handle_resolution_changed(self, index: int) -> None:
        self._on_resolution_changed(str(self._resolution_combo.itemData(index) or "source"))

    def _handle_format_changed(self, index: int) -> None:
        self._on_format_changed(str(self._format_combo.itemData(index) or "source"))


class YTVersionPanel(QGroupBox):
    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        on_preset_changed: Callable[[str], None],
        on_no_bframes_changed: Callable[[bool], None],
        on_format_changed: Callable[[str], None],
        on_resolution_changed: Callable[[str], None],
    ) -> None:
        super().__init__("YT-Version", parent)
        self.setStyleSheet(_panel_style())
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)
        info = QLabel(
            "Die YouTube-Version erzeugt eine upload-optimierte Ausgabe auf Basis der aktuellen Verarbeitungskette. Hier stellst du Preset, Auflösung, Container und B-Frames für die Upload-Datei ein."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: #475569;")
        layout.addWidget(info)
        self._encoding_panel = StepEncodingPanel(
            "Videoeinstellungen",
            self,
            on_preset_changed=on_preset_changed,
            on_no_bframes_changed=on_no_bframes_changed,
            on_format_changed=on_format_changed,
            on_resolution_changed=on_resolution_changed,
        )
        layout.addWidget(self._encoding_panel)
        layout.addStretch()

    def load_values(self, *, preset: str, no_bframes: bool, output_format: str, output_resolution: str) -> None:
        self._encoding_panel.load_values(
            preset=preset,
            no_bframes=no_bframes,
            output_format=output_format,
            output_resolution=output_resolution,
        )


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
        on_no_bframes_changed: Callable[[bool], None],
        on_fps_changed: Callable[[int], None],
        on_format_changed: Callable[[str], None],
        on_resolution_changed: Callable[[str], None],
        on_merge_audio_changed: Callable[[bool], None],
        on_amplify_toggled: Callable[[bool], None],
        on_amplify_db_changed: Callable[[float], None],
        on_audio_sync_changed: Callable[[bool], None],
    ) -> None:
        super().__init__("Verarbeitung und Audio", parent)
        self.setStyleSheet(_panel_style())
        self._on_crf_changed = on_crf_changed
        self._on_encoder_changed = on_encoder_changed
        self._on_preset_changed = on_preset_changed
        self._on_no_bframes_changed = on_no_bframes_changed
        self._on_fps_changed = on_fps_changed
        self._on_format_changed = on_format_changed
        self._on_resolution_changed = on_resolution_changed
        self._updating_profile = False
        form = QFormLayout(self)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)

        self._profile_combo = QComboBox()
        self._profile_combo.addItems(list(PROFILES.keys()))
        self._profile_combo.currentTextChanged.connect(self._apply_profile)
        form.addRow(VIDEO_LABEL_PROFILE, self._profile_combo)

        self._crf_spin = QSpinBox()
        self._crf_spin.setRange(0, 51)
        self._crf_spin.valueChanged.connect(self._handle_crf_changed)
        form.addRow("CRF:", self._crf_spin)

        self._encoder_combo = QComboBox()
        for enc_id, enc_name in encoder_choices:
            self._encoder_combo.addItem(enc_name, enc_id)
        self._encoder_combo.currentIndexChanged.connect(self._handle_encoder_changed)
        form.addRow("Encoder:", self._encoder_combo)

        self._preset_combo = QComboBox()
        self._preset_combo.addItems(VIDEO_PRESET_OPTIONS)
        self._preset_combo.currentTextChanged.connect(self._handle_preset_changed)
        self._preset_combo.setToolTip(VIDEO_TOOLTIP_PRESET)
        form.addRow(VIDEO_LABEL_PRESET, self._preset_combo)

        self._resolution_combo = QComboBox()
        for value, label in VIDEO_RESOLUTION_OPTIONS:
            self._resolution_combo.addItem(label, value)
        self._resolution_combo.currentIndexChanged.connect(self._handle_resolution_changed)
        self._resolution_combo.setToolTip(VIDEO_TOOLTIP_RESOLUTION)
        form.addRow(VIDEO_LABEL_RESOLUTION, self._resolution_combo)

        self._no_bframes_cb = QCheckBox(VIDEO_TEXT_NO_BFRAMES)
        self._no_bframes_cb.setToolTip(VIDEO_TOOLTIP_NO_BFRAMES)
        self._no_bframes_cb.toggled.connect(self._handle_no_bframes_changed)
        form.addRow("", self._no_bframes_cb)

        self._fps_spin = QSpinBox()
        self._fps_spin.setRange(1, 120)
        self._fps_spin.valueChanged.connect(self._handle_fps_changed)
        form.addRow(VIDEO_LABEL_FPS, self._fps_spin)

        self._format_combo = QComboBox()
        for value, label in VIDEO_FORMAT_OPTIONS:
            self._format_combo.addItem(label, value)
        self._format_combo.currentIndexChanged.connect(self._handle_format_changed)
        self._format_combo.setToolTip(VIDEO_TOOLTIP_CONTAINER)
        form.addRow(VIDEO_LABEL_CONTAINER, self._format_combo)

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
        self._audio_sync_cb.setToolTip(VIDEO_TOOLTIP_AUDIO_SYNC)
        self._audio_sync_cb.toggled.connect(on_audio_sync_changed)
        form.addRow("", self._audio_sync_cb)

    def _set_profile_name(self, profile_name: str) -> None:
        self._profile_combo.blockSignals(True)
        self._profile_combo.setCurrentText(profile_name)
        self._profile_combo.blockSignals(False)

    def _mark_custom_profile(self) -> None:
        if self._updating_profile:
            return
        self._set_profile_name("Benutzerdefiniert")

    def _apply_profile(self, profile_name: str) -> None:
        values = PROFILES.get(profile_name, {})
        if not values:
            return
        self._updating_profile = True
        try:
            if "encoder" in values:
                index = max(self._encoder_combo.findData(values["encoder"]), 0)
                self._encoder_combo.setCurrentIndex(index)
            if "crf" in values:
                self._crf_spin.setValue(int(values["crf"]))
            if "preset" in values:
                self._preset_combo.setCurrentText(str(values["preset"]))
            if "output_resolution" in values:
                self._resolution_combo.setCurrentIndex(max(self._resolution_combo.findData(values["output_resolution"]), 0))
            if "no_bframes" in values:
                self._no_bframes_cb.setChecked(bool(values["no_bframes"]))
            if "output_format" in values:
                self._format_combo.setCurrentIndex(max(self._format_combo.findData(values["output_format"]), 0))
        finally:
            self._updating_profile = False

    def sync_profile_from_values(self) -> None:
        self._set_profile_name(
            matching_profile_name(
                {
                    "encoder": self._encoder_combo.currentData(),
                    "crf": self._crf_spin.value(),
                    "preset": self._preset_combo.currentText(),
                    "output_format": str(self._format_combo.currentData() or "mp4"),
                    "output_resolution": str(self._resolution_combo.currentData() or "source"),
                    "no_bframes": self._no_bframes_cb.isChecked(),
                },
                ("encoder", "crf", "preset", "output_format", "output_resolution", "no_bframes"),
            )
        )

    def _handle_crf_changed(self, value: int) -> None:
        self._mark_custom_profile()
        self._on_crf_changed(value)

    def _handle_encoder_changed(self, index: int) -> None:
        self._mark_custom_profile()
        self._on_encoder_changed(index)

    def _handle_preset_changed(self, value: str) -> None:
        self._mark_custom_profile()
        self._on_preset_changed(value)

    def _handle_no_bframes_changed(self, checked: bool) -> None:
        self._mark_custom_profile()
        self._on_no_bframes_changed(checked)

    def _handle_fps_changed(self, value: int) -> None:
        self._mark_custom_profile()
        self._on_fps_changed(value)

    def _handle_resolution_changed(self, index: int) -> None:
        self._mark_custom_profile()
        self._on_resolution_changed(str(self._resolution_combo.itemData(index) or "source"))

    def _handle_format_changed(self, index: int) -> None:
        self._mark_custom_profile()
        self._on_format_changed(str(self._format_combo.itemData(index) or "mp4"))