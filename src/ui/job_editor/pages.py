from pathlib import Path

from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import (
    QCheckBox,
    QColorDialog,
    QComboBox,
    QDoubleSpinBox,
    QFileDialog,
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

from ...media.encoder import available_encoder_choices
from ...settings import PROFILES
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


class JobEditorPagesMixin:
    def _build_page_processing(self) -> QWidget:
        page = QWidget()
        lay = QVBoxLayout(page)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        self._convert_enabled_cb = QCheckBox("Dateien konvertieren  (Encoding aktiv)")
        font = QFont()
        font.setPointSize(13)
        font.setBold(True)
        self._convert_enabled_cb.setFont(font)
        self._convert_enabled_cb.toggled.connect(lambda on: self._encoding_widget.setEnabled(on))
        lay.addWidget(self._convert_enabled_cb)

        self._encoding_widget = QGroupBox("Encoding-Einstellungen")
        enc_lay = QVBoxLayout(self._encoding_widget)

        prof_row = QHBoxLayout()
        prof_row.addWidget(QLabel(VIDEO_LABEL_PROFILE))
        for pname in PROFILES:
            btn = QPushButton(pname)
            btn.setFlat(True)
            btn.setStyleSheet("QPushButton{text-decoration:underline;color:#3a7bde;border:none;}")
            btn.clicked.connect(lambda _checked, name=pname: self._apply_profile(name))
            prof_row.addWidget(btn)
        prof_row.addStretch()
        enc_lay.addLayout(prof_row)

        enc_form = QFormLayout()
        enc_form.setContentsMargins(0, 4, 0, 0)

        self._encoder_combo = QComboBox()
        for enc_id, enc_name in available_encoder_choices():
            self._encoder_combo.addItem(enc_name, enc_id)
        enc_form.addRow("Encoder:", self._encoder_combo)

        self._preset_combo = QComboBox()
        self._preset_combo.addItems(VIDEO_PRESET_OPTIONS)
        self._preset_combo.setToolTip(VIDEO_TOOLTIP_PRESET)
        enc_form.addRow(VIDEO_LABEL_PRESET, self._preset_combo)

        self._resolution_combo = QComboBox()
        for value, label in VIDEO_RESOLUTION_OPTIONS:
            self._resolution_combo.addItem(label, value)
        self._resolution_combo.setToolTip(VIDEO_TOOLTIP_RESOLUTION)
        enc_form.addRow(VIDEO_LABEL_RESOLUTION, self._resolution_combo)

        crf_row = QHBoxLayout()
        self._crf_spin = QSpinBox()
        self._crf_spin.setRange(0, 51)
        self._crf_spin.setFixedWidth(70)
        crf_row.addWidget(self._crf_spin)
        hint = QLabel("  0 = verlustfrei  ·  18 = sehr gut  ·  23 = Standard")
        hint.setStyleSheet("color:#888; font-size:11px;")
        crf_row.addWidget(hint)
        crf_row.addStretch()
        enc_form.addRow("CRF:", crf_row)

        fps_row = QHBoxLayout()
        self._fps_spin = QSpinBox()
        self._fps_spin.setRange(1, 120)
        self._fps_spin.setFixedWidth(70)
        fps_row.addWidget(self._fps_spin)
        fps_row.addWidget(QLabel("fps"))
        fps_row.addStretch()
        enc_form.addRow(VIDEO_LABEL_FPS, fps_row)

        self._no_bframes_cb = QCheckBox(VIDEO_TEXT_NO_BFRAMES)
        self._no_bframes_cb.setToolTip(VIDEO_TOOLTIP_NO_BFRAMES)
        enc_form.addRow("", self._no_bframes_cb)

        self._format_combo = QComboBox()
        for value, label in VIDEO_FORMAT_OPTIONS:
            self._format_combo.addItem(label, value)
        self._format_combo.setToolTip(VIDEO_TOOLTIP_CONTAINER)
        enc_form.addRow(VIDEO_LABEL_CONTAINER, self._format_combo)

        self._overwrite_cb = QCheckBox(
            "Vorhandene Ausgabedateien überschreiben  (Skip-Schutz deaktivieren)"
        )
        self._overwrite_cb.setToolTip(
            VIDEO_TOOLTIP_OVERWRITE + "\nNormalerweise deaktiviert lassen – dann werden fertige Dateien übersprungen."
        )
        enc_form.addRow("", self._overwrite_cb)

        enc_lay.addLayout(enc_form)
        lay.addWidget(self._encoding_widget)

        audio_box = QGroupBox("Audio")
        audio_lay = QVBoxLayout(audio_box)

        self._merge_audio_cb = QCheckBox("Separate Audio-Spur zusammenführen  (.wav + Video)")
        audio_lay.addWidget(self._merge_audio_cb)

        amp_row = QHBoxLayout()
        self._amplify_audio_cb = QCheckBox("Lautstärke anpassen um")
        self._amplify_audio_cb.toggled.connect(lambda on: self._amplify_db_spin.setEnabled(on))
        amp_row.addWidget(self._amplify_audio_cb)
        self._amplify_db_spin = QDoubleSpinBox()
        self._amplify_db_spin.setRange(-20.0, 40.0)
        self._amplify_db_spin.setSingleStep(1.0)
        self._amplify_db_spin.setDecimals(1)
        self._amplify_db_spin.setSuffix(" dB")
        self._amplify_db_spin.setFixedWidth(90)
        amp_row.addWidget(self._amplify_db_spin)
        amp_row.addWidget(QLabel("  (+6 dB ≈ doppelte Lautstärke)"))
        amp_row.addStretch()
        audio_lay.addLayout(amp_row)

        self._audio_sync_cb = QCheckBox(
            "Audio-Sync aktivieren  (Frame-Drop-Korrektur für Pi-Kameras)"
        )
        self._audio_sync_cb.setToolTip(VIDEO_TOOLTIP_AUDIO_SYNC)
        audio_lay.addWidget(self._audio_sync_cb)

        lay.addWidget(audio_box)
        lay.addStretch()
        return page

    def _build_page_titlecard(self) -> QWidget:
        page = QWidget()
        lay = QVBoxLayout(page)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        tc_box = QGroupBox("Titelkarte / Intro-Bild")
        tc_lay = QVBoxLayout(tc_box)

        self._tc_enabled_cb = QCheckBox("Titelkarte vor jedem Video einblenden")
        self._tc_enabled_cb.setStyleSheet("font-weight:bold; font-size:13px;")
        self._tc_enabled_cb.toggled.connect(lambda on: self._tc_details.setEnabled(on))
        tc_lay.addWidget(self._tc_enabled_cb)

        self._tc_details = QWidget()
        self._tc_details.setEnabled(False)
        form = QFormLayout(self._tc_details)
        form.setContentsMargins(20, 4, 0, 4)
        form.setSpacing(8)

        logo_row = QHBoxLayout()
        self._tc_logo_edit = QLineEdit()
        self._tc_logo_edit.setPlaceholderText("Pfad zum Logo-Bild (leer = kein Logo)")
        logo_row.addWidget(self._tc_logo_edit)
        browse_logo_btn = QPushButton("…")
        browse_logo_btn.setFixedWidth(32)
        browse_logo_btn.clicked.connect(self._browse_tc_logo)
        logo_row.addWidget(browse_logo_btn)
        form.addRow("Logo:", logo_row)

        self._tc_home_edit = QLineEdit()
        self._tc_home_edit.setPlaceholderText("z. B. FC Musterstadt")
        form.addRow("Heim:", self._tc_home_edit)

        self._tc_away_edit = QLineEdit()
        self._tc_away_edit.setPlaceholderText("z. B. FC Auswärts")
        form.addRow("Gast:", self._tc_away_edit)

        self._tc_date_edit = QLineEdit()
        self._tc_date_edit.setPlaceholderText("z. B. 15.03.2025")
        form.addRow("Datum:", self._tc_date_edit)

        dur_row = QHBoxLayout()
        self._tc_duration_spin = QDoubleSpinBox()
        self._tc_duration_spin.setRange(0.5, 10.0)
        self._tc_duration_spin.setSingleStep(0.5)
        self._tc_duration_spin.setDecimals(1)
        self._tc_duration_spin.setSuffix(" s")
        self._tc_duration_spin.setValue(3.0)
        self._tc_duration_spin.setFixedWidth(90)
        dur_row.addWidget(self._tc_duration_spin)
        dur_row.addStretch()
        form.addRow("Dauer:", dur_row)

        color_row = QHBoxLayout()
        self._tc_bg_btn = QPushButton("  ")
        self._tc_bg_btn.setFixedWidth(50)
        self._tc_bg_btn.setToolTip("Hintergrundfarbe wählen")
        self._tc_bg_btn.clicked.connect(lambda: self._pick_color("bg"))
        color_row.addWidget(QLabel("Hintergrund:"))
        color_row.addWidget(self._tc_bg_btn)
        color_row.addSpacing(20)
        self._tc_fg_btn = QPushButton("  ")
        self._tc_fg_btn.setFixedWidth(50)
        self._tc_fg_btn.setToolTip("Textfarbe wählen")
        self._tc_fg_btn.clicked.connect(lambda: self._pick_color("fg"))
        color_row.addWidget(QLabel("Text:"))
        color_row.addWidget(self._tc_fg_btn)
        color_row.addStretch()
        form.addRow("Farben:", color_row)

        hint = QLabel(
            "💡 Im »Dateien auswählen«-Modus kann pro Datei ein individueller "
            "Untertitel (z.B. \"1. Halbzeit\", \"Kamera 1\") in der Dateiliste "
            "eingetragen werden."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color:#666; font-style:italic; padding:4px 0;")
        form.addRow(hint)

        tc_lay.addWidget(self._tc_details)
        lay.addWidget(tc_box)
        lay.addStretch()

        self._tc_bg_color = "#000000"
        self._tc_fg_color = "#FFFFFF"
        self._update_color_btn("bg", "#000000")
        self._update_color_btn("fg", "#FFFFFF")

        default_logo = Path(__file__).parent.parent.parent.parent / "videoschnitt" / "assets" / "kaderblick.png"
        if default_logo.exists():
            self._tc_logo_edit.setText(str(default_logo))

        return page

    def _browse_tc_logo(self) -> None:
        start = self._tc_logo_edit.text().strip() or self._settings.last_directory or str(Path.home())
        path, _filter = QFileDialog.getOpenFileName(
            self,
            "Logo-Bild wählen",
            start,
            "Bilder (*.png *.jpg *.jpeg *.svg *.webp);;Alle Dateien (*)",
        )
        if path:
            self._tc_logo_edit.setText(path)

    def _pick_color(self, which: str) -> None:
        current = self._tc_bg_color if which == "bg" else self._tc_fg_color
        color = QColorDialog.getColor(QColor(current), self, "Farbe wählen")
        if color.isValid():
            hex_color = color.name()
            if which == "bg":
                self._tc_bg_color = hex_color
            else:
                self._tc_fg_color = hex_color
            self._update_color_btn(which, hex_color)

    def _update_color_btn(self, which: str, hex_color: str) -> None:
        btn = self._tc_bg_btn if which == "bg" else self._tc_fg_btn
        red, green, blue = int(hex_color[1:3], 16), int(hex_color[3:5], 16), int(hex_color[5:7], 16)
        fg = "#000000" if (0.299 * red + 0.587 * green + 0.114 * blue) > 128 else "#FFFFFF"
        btn.setStyleSheet(f"background-color: {hex_color}; color: {fg}; border: 1px solid #aaa;")
        btn.setText(hex_color)

    def _build_page_upload(self) -> QWidget:
        page = QWidget()
        lay = QVBoxLayout(page)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        yt_box = QGroupBox("YouTube")
        yt_lay = QVBoxLayout(yt_box)

        self._yt_upload_cb = QCheckBox("Auf YouTube hochladen")
        self._yt_upload_cb.setStyleSheet("font-weight:bold; font-size:13px;")
        self._yt_upload_cb.toggled.connect(self._on_yt_toggled)
        yt_lay.addWidget(self._yt_upload_cb)

        self._yt_details = QWidget()
        yt_d = QFormLayout(self._yt_details)
        yt_d.setContentsMargins(20, 4, 0, 4)

        self._yt_create_cb = QCheckBox(
            "YouTube-optimierte Version erstellen (separate Datei mit YT-Codec-Empfehlungen)"
        )
        yt_d.addRow(self._yt_create_cb)

        self._yt_title_edit = QLineEdit()
        self._yt_title_edit.setPlaceholderText("leer = Dateiname als Titel")
        yt_d.addRow("Standard-Titel:", self._yt_title_edit)

        yt_pl_row = QHBoxLayout()
        self._yt_playlist_edit = QLineEdit()
        self._yt_playlist_edit.setPlaceholderText("leer = keine Playlist")
        yt_pl_row.addWidget(self._yt_playlist_edit)
        yt_match_btn = QPushButton("🎬 Spieldaten …")
        yt_match_btn.setToolTip(
            "Datum, Wettbewerb und Teams eingeben → Playlist wird automatisch generiert."
        )
        yt_match_btn.clicked.connect(self._open_match_editor_for_playlist)
        yt_pl_row.addWidget(yt_match_btn)
        yt_d.addRow("Playlist:", yt_pl_row)

        self._yt_files_hint = QLabel(
            "💡 Im »Dateien auswählen«-Modus: Titel und Playlist pro Datei "
            "über den 🎬-Button in der Dateiliste setzen."
        )
        self._yt_files_hint.setWordWrap(True)
        self._yt_files_hint.setStyleSheet("color:#666; font-style:italic; padding:4px 0;")
        yt_d.addRow(self._yt_files_hint)

        yt_lay.addWidget(self._yt_details)
        lay.addWidget(yt_box)

        kb_box = QGroupBox("Kaderblick")
        kb_lay = QVBoxLayout(kb_box)

        self._kb_upload_cb = QCheckBox("Video nach YouTube-Upload auf Kaderblick eintragen")
        self._kb_upload_cb.setStyleSheet("font-weight:bold; font-size:13px;")
        self._kb_upload_cb.toggled.connect(self._on_kb_toggled)
        kb_lay.addWidget(self._kb_upload_cb)

        self._kb_details_widget = QWidget()
        kb_d = QFormLayout(self._kb_details_widget)
        kb_d.setContentsMargins(20, 4, 0, 4)

        self._kb_game_id_edit = QLineEdit()
        self._kb_game_id_edit.setPlaceholderText("z. B. 19")
        self._kb_game_id_edit.setMaximumWidth(120)
        kb_d.addRow("Spiel-ID:", self._kb_game_id_edit)

        reload_row = QHBoxLayout()
        self._kb_reload_btn = QPushButton("↺  Typen & Kameras neu laden")
        self._kb_reload_btn.clicked.connect(lambda: self._kb_load_api_data(force=True))
        reload_row.addWidget(self._kb_reload_btn)
        reload_row.addStretch()
        kb_d.addRow("", reload_row)

        self._kb_status_label = QLabel("")
        self._kb_status_label.setWordWrap(True)
        kb_d.addRow("", self._kb_status_label)

        kb_lay.addWidget(self._kb_details_widget)
        lay.addWidget(kb_box)
        lay.addStretch()
        return page

    def _sync_upload_visibility(self) -> None:
        yt_on = self._yt_upload_cb.isChecked()
        self._yt_details.setVisible(yt_on)
        if yt_on:
            is_files = self._mode_group.checkedId() == 0
            self._yt_title_edit.setVisible(not is_files)
            self._yt_playlist_edit.setVisible(not is_files)
            self._yt_files_hint.setVisible(is_files)
        self._kb_upload_cb.setEnabled(yt_on)
        if not yt_on:
            self._kb_upload_cb.setChecked(False)
        self._kb_details_widget.setVisible(self._kb_upload_cb.isChecked())

    def _on_yt_toggled(self, on: bool) -> None:
        self._sync_upload_visibility()

    def _on_kb_toggled(self, on: bool) -> None:
        self._kb_details_widget.setVisible(on)
        if on and not self._kb_api_loaded:
            self._kb_load_api_data()

    def _apply_profile(self, name: str) -> None:
        values = PROFILES.get(name, {})
        if not values:
            return
        if "encoder" in values:
            idx = self._encoder_combo.findData(values["encoder"])
            if idx >= 0:
                self._encoder_combo.setCurrentIndex(idx)
        if "preset" in values:
            self._preset_combo.setCurrentText(values["preset"])
        if "output_resolution" in values:
            self._resolution_combo.setCurrentIndex(max(self._resolution_combo.findData(values["output_resolution"]), 0))
        if "crf" in values:
            self._crf_spin.setValue(values["crf"])
        if "output_format" in values:
            self._format_combo.setCurrentIndex(max(self._format_combo.findData(values["output_format"]), 0))
        if "no_bframes" in values:
            self._no_bframes_cb.setChecked(bool(values["no_bframes"]))