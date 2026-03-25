from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import QRectF, QSize, Qt
from PySide6.QtGui import QColor, QFont, QFontMetrics, QPainter, QPainterPath, QPixmap
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QColorDialog,
    QDoubleSpinBox,
    QFileDialog,
    QFrame,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSizePolicy,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from .merge import YouTubeMetadataPanel
from ...media.ffmpeg_runner import get_resolution, get_video_stream_info, has_audio_stream
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
STEP_PROFILE_SOURCE = "Input übernehmen"
STEP_ENCODER_OPTIONS = [("inherit", "App-Standard übernehmen")]
_FORMAT_LABELS = {value: label for value, label in STEP_CONTAINER_OPTIONS}
_RESOLUTION_LABELS = {value: label for value, label in VIDEO_RESOLUTION_OPTIONS}


def _configure_wrapped_info_label(label: QLabel, *, color: str) -> None:
    label.setWordWrap(True)
    label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
    label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.MinimumExpanding)
    label.setContentsMargins(0, 2, 0, 2)
    label.setStyleSheet(f"color: {color}; padding: 2px 0;")


def _configure_step_field_width(widget: QWidget, *, minimum_width: int = 240) -> None:
    widget.setMinimumWidth(minimum_width)
    widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)


@dataclass(frozen=True)
class SourceMaterialSummary:
    signature: tuple[str, ...]
    source_label: str
    effective_resolution_label: str
    effective_format_label: str
    effective_fps_label: str


class SourceMaterialAnalyzer:
    def __init__(self) -> None:
        self._cache: dict[tuple[tuple[str, int, int], ...], SourceMaterialSummary] = {}

    @staticmethod
    def _file_state(paths: list[str]) -> tuple[tuple[str, int, int], ...]:
        states: list[tuple[str, int, int]] = []
        for raw in paths:
            if not raw:
                continue
            path = Path(raw)
            if not path.exists() or not path.is_file():
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            states.append((str(path), int(stat.st_mtime_ns), int(stat.st_size)))
        return tuple(states)

    def summarize(self, paths: list[str], *, force: bool = False) -> SourceMaterialSummary:
        file_state = self._file_state(paths)
        if not force and file_state in self._cache:
            return self._cache[file_state]
        summary = _summarize_source_material([item[0] for item in file_state])
        self._cache[file_state] = summary
        return summary


def build_configured_source_summary(
    *,
    step_label: str,
    output_format: str,
    output_resolution: str,
    fps: int,
    base_summary: SourceMaterialSummary | None = None,
) -> SourceMaterialSummary:
    format_label = (
        base_summary.effective_format_label
        if output_format == "source" and base_summary is not None
        else _FORMAT_LABELS.get(output_format, "Originalcontainer")
    )
    resolution_label = (
        base_summary.effective_resolution_label
        if output_resolution == "source" and base_summary is not None
        else _RESOLUTION_LABELS.get(output_resolution, "Originalauflösung")
    )
    fps_label = (
        base_summary.effective_fps_label
        if fps <= 0 and base_summary is not None
        else (f"{fps} fps" if fps > 0 else "Original-FPS")
    )
    source_label = (
        f"Input aus {step_label}: Container {format_label} | Auflösung {resolution_label} | FPS {fps_label}"
    )
    return SourceMaterialSummary(
        signature=("configured", step_label, format_label, resolution_label, fps_label),
        source_label=source_label,
        effective_resolution_label=resolution_label,
        effective_format_label=format_label,
        effective_fps_label=fps_label,
    )


def _format_unique_or_mixed(values: list[str], *, empty: str = "unbekannt") -> str:
    unique = [value for value in dict.fromkeys(values) if value]
    if not unique:
        return empty
    if len(unique) == 1:
        return unique[0]
    preview = " / ".join(unique[:3])
    if len(unique) > 3:
        preview += " / …"
    return f"gemischt ({preview})"


def _summarize_source_material(paths: list[str]) -> SourceMaterialSummary:
    existing_paths: list[Path] = []
    for raw in paths:
        if not raw:
            continue
        path = Path(raw)
        if path.exists() and path.is_file():
            existing_paths.append(path)

    signature = tuple(str(path) for path in existing_paths)
    if not existing_paths:
        return SourceMaterialSummary(
            signature=(),
            source_label="Keine analysierbaren Quelldateien geladen.",
            effective_resolution_label="Originalauflösung",
            effective_format_label="Originalcontainer",
            effective_fps_label="Original-FPS",
        )

    containers: list[str] = []
    codecs: list[str] = []
    resolutions: list[str] = []
    fps_values: list[str] = []
    audio_values: list[str] = []

    for path in existing_paths:
        containers.append((path.suffix or "?").lstrip(".").upper() or "?")
        video_info = get_video_stream_info(path)
        codec_name = str(video_info.get("codec_name") or "").strip().lower()
        codecs.append(codec_name.upper() if codec_name else "unbekannt")
        resolution = get_resolution(path)
        resolutions.append(f"{resolution[0]}x{resolution[1]}" if resolution else "unbekannt")
        fps = video_info.get("fps")
        fps_values.append(f"{float(fps):.3f} fps" if fps else "unbekannt")
        audio_values.append("Audio" if has_audio_stream(path) else "Kein Audio")

    source_label = (
        f"Quelle: {len(existing_paths)} Datei(en) | Container {_format_unique_or_mixed(containers)} | "
        f"Codec {_format_unique_or_mixed(codecs)} | Auflösung {_format_unique_or_mixed(resolutions)} | "
        f"FPS {_format_unique_or_mixed(fps_values)} | {_format_unique_or_mixed(audio_values, empty='Audio unbekannt')}"
    )

    return SourceMaterialSummary(
        signature=signature,
        source_label=source_label,
        effective_resolution_label=_format_unique_or_mixed(resolutions, empty="Originalauflösung"),
        effective_format_label=_format_unique_or_mixed(containers, empty="Originalcontainer"),
        effective_fps_label=_format_unique_or_mixed(fps_values, empty="Original-FPS"),
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


class TitlecardPreviewWidget(QFrame):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._bg_color = "#000000"
        self._fg_color = "#FFFFFF"
        self._title_text = ""
        self._subtitle_text = "Dateiname / Untertitel"
        self._logo_path = ""
        self._logo_pixmap = QPixmap()
        self.setMinimumHeight(220)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

    def sizeHint(self) -> QSize:
        return QSize(480, 270)

    def set_preview_data(
        self,
        *,
        bg_color: str,
        fg_color: str,
        title_text: str,
        subtitle_text: str,
        logo_path: str,
    ) -> None:
        self._bg_color = bg_color
        self._fg_color = fg_color
        self._title_text = title_text
        self._subtitle_text = subtitle_text
        if logo_path != self._logo_path:
            self._logo_path = logo_path
            self._logo_pixmap = QPixmap(logo_path) if logo_path else QPixmap()
        self.update()

    @staticmethod
    def _wrapped_lines(text: str, metrics: QFontMetrics, max_width: int) -> list[str]:
        normalized = " ".join((text or "").split()).strip()
        if not normalized:
            return []
        words = normalized.split(" ")
        lines: list[str] = []
        current = ""
        for word in words:
            candidate = word if not current else f"{current} {word}"
            if metrics.horizontalAdvance(candidate) <= max_width:
                current = candidate
                continue
            if current:
                lines.append(current)
            current = word
        if current:
            lines.append(current)
        while len(lines) > 3:
            lines[-2] = f"{lines[-2]} {lines[-1]}".strip()
            lines.pop()
        return lines

    def paintEvent(self, event) -> None:
        super().paintEvent(event)
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)

        outer_rect = QRectF(self.rect()).adjusted(1, 1, -1, -1)
        clip_path = QPainterPath()
        clip_path.addRoundedRect(outer_rect, 12, 12)
        painter.setClipPath(clip_path)
        painter.fillPath(clip_path, QColor(self._bg_color))

        title_font = QFont("DejaVu Sans")
        title_font.setBold(True)
        title_font.setPointSize(max(16, min(30, self.height() // 9)))
        subtitle_font = QFont("DejaVu Sans")
        subtitle_font.setPointSize(max(12, min(22, self.height() // 14)))

        content_rect = outer_rect.adjusted(
            max(18, int(self.width() * 0.05)),
            max(16, int(self.height() * 0.05)),
            -max(18, int(self.width() * 0.05)),
            -max(16, int(self.height() * 0.05)),
        )
        title_metrics = QFontMetrics(title_font)
        subtitle_metrics = QFontMetrics(subtitle_font)
        title_lines = self._wrapped_lines(self._title_text, title_metrics, int(content_rect.width()))
        subtitle_lines = self._wrapped_lines(self._subtitle_text, subtitle_metrics, int(content_rect.width()))
        gap_ts = 10 if title_lines and subtitle_lines else 0

        logo_target_height = 0
        draw_logo = not self._logo_pixmap.isNull()
        if draw_logo:
            logo_target_height = min(int(self.height() * 0.25), 72)

        title_line_height = title_metrics.lineSpacing()
        subtitle_line_height = subtitle_metrics.lineSpacing()
        text_height = title_line_height * len(title_lines) + subtitle_line_height * len(subtitle_lines) + gap_ts
        gap_logo = 14 if draw_logo and text_height > 0 else 0
        block_height = logo_target_height + gap_logo + text_height
        block_top = content_rect.top() + max(0.0, (content_rect.height() - block_height) / 2.0)

        current_y = block_top
        if draw_logo:
            scaled_logo = self._logo_pixmap.scaledToHeight(logo_target_height, Qt.TransformationMode.SmoothTransformation)
            logo_x = content_rect.left() + (content_rect.width() - scaled_logo.width()) / 2.0
            painter.drawPixmap(int(logo_x), int(current_y), scaled_logo)
            current_y += logo_target_height + gap_logo

        if title_lines or subtitle_lines:
            text_block_height = title_line_height * len(title_lines) + subtitle_line_height * len(subtitle_lines) + gap_ts
            box_rect = QRectF(
                content_rect.left(),
                current_y - 10,
                content_rect.width(),
                text_block_height + 20,
            )
            painter.fillRect(box_rect, QColor(0, 0, 0, 178))

        painter.setPen(QColor(self._fg_color))
        for index, line in enumerate(title_lines):
            line_rect = QRectF(content_rect.left(), current_y + index * title_line_height, content_rect.width(), title_line_height)
            painter.setFont(title_font)
            painter.drawText(line_rect, Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter, line)

        current_y += title_line_height * len(title_lines)
        if title_lines and subtitle_lines:
            current_y += gap_ts

        for index, line in enumerate(subtitle_lines):
            line_rect = QRectF(content_rect.left(), current_y + index * subtitle_line_height, content_rect.width(), subtitle_line_height)
            painter.setFont(subtitle_font)
            painter.drawText(line_rect, Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter, line)

        painter.setClipping(False)
        painter.setPen(QColor("#D7E0EA"))
        painter.drawRoundedRect(outer_rect, 12, 12)


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
        self._preview_subtitle = "Dateiname / Untertitel"
        form = QFormLayout(self)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)

        self._tc_home_edit = QLineEdit()
        self._tc_home_edit.textChanged.connect(on_home_changed)
        self._tc_home_edit.textChanged.connect(lambda _text: self._update_preview())
        form.addRow("Heimteam:", self._tc_home_edit)

        self._tc_away_edit = QLineEdit()
        self._tc_away_edit.textChanged.connect(on_away_changed)
        self._tc_away_edit.textChanged.connect(lambda _text: self._update_preview())
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
        self._tc_logo_edit.textChanged.connect(lambda _text: self._update_preview())
        logo_row = QHBoxLayout()
        logo_row.setContentsMargins(0, 0, 0, 0)
        logo_row.setSpacing(8)
        logo_row.addWidget(self._tc_logo_edit, 1)
        self._tc_logo_browse_btn = QPushButton("...")
        self._tc_logo_browse_btn.setFixedWidth(36)
        self._tc_logo_browse_btn.setToolTip("Logo-Bild auswählen")
        self._tc_logo_browse_btn.clicked.connect(self._browse_logo)
        logo_row.addWidget(self._tc_logo_browse_btn)
        form.addRow("Logo:", logo_row)

        self._tc_bg_edit = QLineEdit()
        self._tc_bg_edit.setPlaceholderText("#000000")
        self._tc_bg_edit.textChanged.connect(on_bg_changed)
        self._tc_bg_edit.textChanged.connect(lambda _text: self._update_preview())
        bg_row = QHBoxLayout()
        bg_row.setContentsMargins(0, 0, 0, 0)
        bg_row.setSpacing(8)
        bg_row.addWidget(self._tc_bg_edit, 1)
        self._tc_bg_pick_btn = QPushButton("Farbe wählen")
        self._tc_bg_pick_btn.clicked.connect(lambda: self._pick_color(self._tc_bg_edit, "Hintergrundfarbe wählen"))
        bg_row.addWidget(self._tc_bg_pick_btn)
        form.addRow("Hintergrund:", bg_row)

        self._tc_fg_edit = QLineEdit()
        self._tc_fg_edit.setPlaceholderText("#FFFFFF")
        self._tc_fg_edit.textChanged.connect(on_fg_changed)
        self._tc_fg_edit.textChanged.connect(lambda _text: self._update_preview())
        fg_row = QHBoxLayout()
        fg_row.setContentsMargins(0, 0, 0, 0)
        fg_row.setSpacing(8)
        fg_row.addWidget(self._tc_fg_edit, 1)
        self._tc_fg_pick_btn = QPushButton("Farbe wählen")
        self._tc_fg_pick_btn.clicked.connect(lambda: self._pick_color(self._tc_fg_edit, "Schriftfarbe wählen"))
        fg_row.addWidget(self._tc_fg_pick_btn)
        form.addRow("Schrift:", fg_row)

        self._tc_preview_frame = TitlecardPreviewWidget(self)
        self._tc_preview_frame.setObjectName("titlecardPreview")
        self._tc_duration_spin.valueChanged.connect(lambda _value: self._update_preview())
        form.addRow("Titelbild-Vorschau:", self._tc_preview_frame)
        self._update_preview()

    def set_preview_subtitle(self, subtitle: str) -> None:
        self._preview_subtitle = " ".join((subtitle or "").split()).strip() or "Dateiname / Untertitel"
        self._update_preview()

    def _browse_logo(self) -> None:
        start = self._tc_logo_edit.text().strip() or str(Path.home())
        chosen, _selected_filter = QFileDialog.getOpenFileName(
            self,
            "Logo-Bild wählen",
            start,
            "Bilder (*.png *.jpg *.jpeg *.svg *.webp);;Alle Dateien (*)",
        )
        if chosen:
            self._tc_logo_edit.setText(chosen)

    def _pick_color(self, target_edit: QLineEdit, title: str) -> None:
        current = QColor(target_edit.text().strip() or "#000000")
        dialog = QColorDialog(self)
        dialog.setWindowTitle(title)
        dialog.setCurrentColor(current)
        dialog.setOption(QColorDialog.ColorDialogOption.DontUseNativeDialog, True)
        if dialog.exec():
            color = dialog.currentColor()
            if color.isValid():
                target_edit.setText(color.name().upper())

    @staticmethod
    def _preview_color(raw: str, fallback: str) -> str:
        color = QColor((raw or "").strip())
        return color.name().upper() if color.isValid() else fallback

    def _update_preview(self) -> None:
        bg_color = self._preview_color(self._tc_bg_edit.text(), "#000000")
        fg_color = self._preview_color(self._tc_fg_edit.text(), "#FFFFFF")
        home = self._tc_home_edit.text().strip()
        away = self._tc_away_edit.text().strip()
        title = ""
        if home and away:
            title = f"{home} vs {away}"
        elif home or away:
            title = home or away
        self._tc_preview_frame.set_preview_data(
            bg_color=bg_color,
            fg_color=fg_color,
            title_text=title,
            subtitle_text=self._preview_subtitle,
            logo_path=self._tc_logo_edit.text().strip(),
        )


class StepEncodingPanel(QGroupBox):
    def __init__(
        self,
        title: str,
        parent: QWidget | None = None,
        *,
        source_analyzer: SourceMaterialAnalyzer,
        encoder_choices: list[tuple[str, str]],
        on_crf_changed: Callable[[int], None],
        on_encoder_changed: Callable[[int], None],
        on_preset_changed: Callable[[str], None],
        on_no_bframes_changed: Callable[[bool], None],
        on_fps_changed: Callable[[int], None],
        on_format_changed: Callable[[str], None],
        on_resolution_changed: Callable[[str], None],
    ) -> None:
        super().__init__(title, parent)
        self.setStyleSheet(_panel_style())
        self._source_analyzer = source_analyzer
        self._on_crf_changed = on_crf_changed
        self._on_encoder_changed = on_encoder_changed
        self._on_preset_changed = on_preset_changed
        self._on_no_bframes_changed = on_no_bframes_changed
        self._on_fps_changed = on_fps_changed
        self._on_format_changed = on_format_changed
        self._on_resolution_changed = on_resolution_changed
        self._updating_profile = False
        self._base_encoder = "auto"
        self._base_crf = 18
        self._encoder_reference_label = "App-Standard übernehmen"
        self._crf_reference_label = "App-Standard-CRF übernehmen"
        self._fps_reference_label = "Von Quelle übernehmen"
        self._format_reference_label = "Von Quelle übernehmen"
        self._resolution_reference_label = "Von Quelle übernehmen"
        self._current_source_paths: list[str] = []
        self._source_summary = SourceMaterialSummary((), "Keine analysierbaren Quelldateien geladen.", "Originalauflösung", "Originalcontainer", "Original-FPS")

        form = QFormLayout(self)
        form.setContentsMargins(14, 18, 14, 14)
        form.setSpacing(8)
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)

        self._profile_combo = QComboBox()
        _configure_step_field_width(self._profile_combo)
        self._profile_combo.addItem(STEP_PROFILE_SOURCE)
        self._profile_combo.addItems(list(PROFILES.keys()))
        self._profile_combo.currentTextChanged.connect(self._apply_profile)
        form.addRow(VIDEO_LABEL_PROFILE, self._profile_combo)

        source_row = QHBoxLayout()
        source_row.setContentsMargins(0, 2, 0, 2)
        source_row.setSpacing(8)
        self._source_info_label = QLabel("Keine analysierbaren Quelldateien geladen.")
        _configure_wrapped_info_label(self._source_info_label, color="#475569")
        source_row.addWidget(self._source_info_label, 1)
        self._refresh_source_btn = QPushButton("Analyse neu laden")
        self._refresh_source_btn.clicked.connect(self._refresh_source_material)
        source_row.addWidget(self._refresh_source_btn, 0, Qt.AlignmentFlag.AlignTop)
        form.addRow("Input:", source_row)

        self._effective_info_label = QLabel("")
        _configure_wrapped_info_label(self._effective_info_label, color="#64748B")
        form.addRow("Effektiv:", self._effective_info_label)

        self._encoder_combo = QComboBox()
        _configure_step_field_width(self._encoder_combo, minimum_width=270)
        for enc_id, enc_name in [*STEP_ENCODER_OPTIONS, *encoder_choices]:
            self._encoder_combo.addItem(enc_name, enc_id)
        self._encoder_combo.currentIndexChanged.connect(self._handle_encoder_changed)
        form.addRow("Encoder:", self._encoder_combo)

        self._crf_spin = QSpinBox()
        _configure_step_field_width(self._crf_spin, minimum_width=270)
        self._crf_spin.setRange(0, 51)
        self._crf_spin.setSpecialValueText(self._crf_reference_label)
        self._crf_spin.valueChanged.connect(self._handle_crf_changed)
        form.addRow("CRF:", self._crf_spin)

        self._preset_combo = QComboBox()
        _configure_step_field_width(self._preset_combo)
        self._preset_combo.addItems(VIDEO_PRESET_OPTIONS)
        self._preset_combo.currentTextChanged.connect(self._handle_preset_changed)
        self._preset_combo.setToolTip(VIDEO_TOOLTIP_PRESET)
        form.addRow(VIDEO_LABEL_PRESET, self._preset_combo)

        self._fps_spin = QSpinBox()
        _configure_step_field_width(self._fps_spin, minimum_width=270)
        self._fps_spin.setRange(0, 120)
        self._fps_spin.setSpecialValueText(self._fps_reference_label)
        self._fps_spin.valueChanged.connect(self._handle_fps_changed)
        form.addRow(VIDEO_LABEL_FPS, self._fps_spin)

        self._resolution_combo = QComboBox()
        _configure_step_field_width(self._resolution_combo, minimum_width=270)
        for value, label in VIDEO_RESOLUTION_OPTIONS:
            self._resolution_combo.addItem(label, value)
        self._resolution_combo.currentIndexChanged.connect(self._handle_resolution_changed)
        self._resolution_combo.setToolTip(VIDEO_TOOLTIP_RESOLUTION)
        form.addRow(VIDEO_LABEL_RESOLUTION, self._resolution_combo)

        self._format_combo = QComboBox()
        _configure_step_field_width(self._format_combo, minimum_width=270)
        for value, label in STEP_CONTAINER_OPTIONS:
            self._format_combo.addItem(label, value)
        self._format_combo.currentIndexChanged.connect(self._handle_format_changed)
        self._format_combo.setToolTip(VIDEO_TOOLTIP_CONTAINER)
        form.addRow(VIDEO_LABEL_CONTAINER, self._format_combo)

        self._no_bframes_cb = QCheckBox(VIDEO_TEXT_NO_BFRAMES)
        self._no_bframes_cb.setToolTip(VIDEO_TOOLTIP_NO_BFRAMES)
        self._no_bframes_cb.toggled.connect(self._handle_no_bframes_changed)
        form.addRow("", self._no_bframes_cb)

    def configure_reference_labels(
        self,
        *,
        encoder_label: str,
        crf_label: str,
        fps_label: str,
        format_label: str,
        resolution_label: str,
    ) -> None:
        self._encoder_reference_label = encoder_label
        self._crf_reference_label = crf_label
        self._fps_reference_label = fps_label
        self._format_reference_label = format_label
        self._resolution_reference_label = resolution_label

        encoder_idx = self._encoder_combo.findData("inherit")
        if encoder_idx >= 0:
            self._encoder_combo.setItemText(encoder_idx, encoder_label)

        self._crf_spin.setSpecialValueText(crf_label)
        self._fps_spin.setSpecialValueText(fps_label)

        format_idx = self._format_combo.findData("source")
        if format_idx >= 0:
            self._format_combo.setItemText(format_idx, format_label)

        resolution_idx = self._resolution_combo.findData("source")
        if resolution_idx >= 0:
            self._resolution_combo.setItemText(resolution_idx, resolution_label)

        self._update_effective_summary()

    def _set_profile_name(self, profile_name: str) -> None:
        self._profile_combo.blockSignals(True)
        self._profile_combo.setCurrentText(profile_name)
        self._profile_combo.blockSignals(False)

    def _mark_custom_profile(self) -> None:
        if self._updating_profile:
            return
        self._sync_profile_from_values()

    def update_source_material(self, paths: list[str], *, force: bool = False) -> None:
        self._current_source_paths = list(paths)
        self._refresh_source_btn.setVisible(bool(self._current_source_paths))
        summary = self._source_analyzer.summarize(self._current_source_paths, force=force)
        self.update_source_summary(summary)

    def update_source_summary(self, summary: SourceMaterialSummary) -> None:
        if summary.signature == self._source_summary.signature:
            self._update_effective_summary()
            return
        self._source_summary = summary
        self._source_info_label.setText(summary.source_label)
        self._update_effective_summary()

    def _refresh_source_material(self) -> None:
        self.update_source_material(self._current_source_paths, force=True)

    def _update_effective_summary(self) -> None:
        resolution_label = (
            self._source_summary.effective_resolution_label
            if str(self._resolution_combo.currentData() or "source") == "source"
            else self._resolution_combo.currentText()
        )
        format_value = str(self._format_combo.currentData() or "source")
        format_label = self._source_summary.effective_format_label if format_value == "source" else self._format_combo.currentText()
        fps_label = self._source_summary.effective_fps_label if self._fps_spin.value() == 0 else f"{self._fps_spin.value()} fps"
        encoder_value = str(self._encoder_combo.currentData() or "inherit")
        if encoder_value == "inherit":
            encoder_label = next((self._encoder_combo.itemText(i) for i in range(self._encoder_combo.count()) if self._encoder_combo.itemData(i) == "inherit"), self._encoder_reference_label)
        else:
            encoder_label = self._encoder_combo.currentText()
        crf_label = (
            f"CRF {self._crf_spin.value()}"
            if self._crf_spin.value() > 0
            else f"CRF {self._crf_reference_label} ({self._base_crf})"
        )
        self._effective_info_label.setText(
            f"Encoder {encoder_label} | {crf_label} | Preset {self._preset_combo.currentText()} | {fps_label} | {format_label} | {resolution_label}"
        )

    def _sync_profile_from_values(self) -> None:
        if (
            str(self._encoder_combo.currentData() or "inherit") == "inherit"
            and self._crf_spin.value() == 0
            and self._fps_spin.value() == 0
            and str(self._format_combo.currentData() or "source") == "source"
            and str(self._resolution_combo.currentData() or "source") == "source"
        ):
            self._set_profile_name(STEP_PROFILE_SOURCE)
            return
        self._set_profile_name(
            matching_profile_name(
                {
                    "encoder": self._encoder_combo.currentData(),
                    "crf": self._crf_spin.value(),
                    "preset": self._preset_combo.currentText(),
                    "output_format": str(self._format_combo.currentData() or "source"),
                    "output_resolution": str(self._resolution_combo.currentData() or "source"),
                    "no_bframes": self._no_bframes_cb.isChecked(),
                },
                ("encoder", "crf", "preset", "output_format", "output_resolution", "no_bframes"),
            )
        )

    def _apply_profile(self, profile_name: str) -> None:
        if profile_name == STEP_PROFILE_SOURCE:
            self._updating_profile = True
            try:
                self._encoder_combo.setCurrentIndex(max(self._encoder_combo.findData("inherit"), 0))
                self._crf_spin.setValue(0)
                self._fps_spin.setValue(0)
                self._format_combo.setCurrentIndex(max(self._format_combo.findData("source"), 0))
                self._resolution_combo.setCurrentIndex(max(self._resolution_combo.findData("source"), 0))
            finally:
                self._updating_profile = False
            self._update_effective_summary()
            return
        values = PROFILES.get(profile_name, {})
        if not values:
            return
        self._updating_profile = True
        try:
            if "encoder" in values:
                self._encoder_combo.setCurrentIndex(max(self._encoder_combo.findData(values["encoder"]), 0))
            if "crf" in values:
                self._crf_spin.setValue(int(values["crf"]))
            if "preset" in values:
                self._preset_combo.setCurrentText(str(values["preset"]))
            if "no_bframes" in values:
                self._no_bframes_cb.setChecked(bool(values["no_bframes"]))
            self._fps_spin.setValue(0)
        finally:
            self._updating_profile = False
        self._update_effective_summary()

    def load_values(
        self,
        *,
        encoder: str,
        crf: int,
        preset: str,
        no_bframes: bool,
        fps: int,
        output_format: str,
        output_resolution: str,
        base_encoder: str,
        base_crf: int,
    ) -> None:
        self._base_encoder = base_encoder
        self._base_crf = base_crf
        self._updating_profile = True
        try:
            self._encoder_combo.setCurrentIndex(max(self._encoder_combo.findData(encoder), 0))
            self._crf_spin.setValue(max(0, int(crf)))
            self._preset_combo.setCurrentText(preset)
            self._no_bframes_cb.setChecked(no_bframes)
            self._fps_spin.setValue(max(0, int(fps)))
            self._format_combo.setCurrentIndex(max(self._format_combo.findData(output_format), 0))
            self._resolution_combo.setCurrentIndex(max(self._resolution_combo.findData(output_resolution), 0))
        finally:
            self._updating_profile = False
        self._sync_profile_from_values()
        self._update_effective_summary()

    def _handle_encoder_changed(self, index: int) -> None:
        self._mark_custom_profile()
        self._on_encoder_changed(index)
        self._update_effective_summary()

    def _handle_crf_changed(self, value: int) -> None:
        self._mark_custom_profile()
        self._on_crf_changed(value)
        self._update_effective_summary()

    def _handle_preset_changed(self, value: str) -> None:
        self._mark_custom_profile()
        self._on_preset_changed(value)
        self._update_effective_summary()

    def _handle_no_bframes_changed(self, checked: bool) -> None:
        self._mark_custom_profile()
        self._on_no_bframes_changed(checked)
        self._update_effective_summary()

    def _handle_fps_changed(self, value: int) -> None:
        self._mark_custom_profile()
        self._on_fps_changed(value)
        self._update_effective_summary()

    def _handle_resolution_changed(self, index: int) -> None:
        self._on_resolution_changed(str(self._resolution_combo.itemData(index) or "source"))
        self._mark_custom_profile()
        self._update_effective_summary()

    def _handle_format_changed(self, index: int) -> None:
        self._on_format_changed(str(self._format_combo.itemData(index) or "source"))
        self._mark_custom_profile()
        self._update_effective_summary()


class YTVersionPanel(QGroupBox):
    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        source_analyzer: SourceMaterialAnalyzer,
        encoder_choices: list[tuple[str, str]],
        on_crf_changed: Callable[[int], None],
        on_encoder_changed: Callable[[int], None],
        on_preset_changed: Callable[[str], None],
        on_no_bframes_changed: Callable[[bool], None],
        on_fps_changed: Callable[[int], None],
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
            source_analyzer=source_analyzer,
            encoder_choices=encoder_choices,
            on_crf_changed=on_crf_changed,
            on_encoder_changed=on_encoder_changed,
            on_preset_changed=on_preset_changed,
            on_no_bframes_changed=on_no_bframes_changed,
            on_fps_changed=on_fps_changed,
            on_format_changed=on_format_changed,
            on_resolution_changed=on_resolution_changed,
        )
        layout.addWidget(self._encoding_panel)
        layout.addStretch()

    def load_values(
        self,
        *,
        encoder: str,
        crf: int,
        preset: str,
        no_bframes: bool,
        fps: int,
        output_format: str,
        output_resolution: str,
        base_encoder: str,
        base_crf: int,
    ) -> None:
        self._encoding_panel.load_values(
            encoder=encoder,
            crf=crf,
            preset=preset,
            no_bframes=no_bframes,
            fps=fps,
            output_format=output_format,
            output_resolution=output_resolution,
            base_encoder=base_encoder,
            base_crf=base_crf,
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