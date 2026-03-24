from __future__ import annotations

from pathlib import Path
from typing import Callable

from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from ...ui import AlwaysVisiblePlaceholderLineEdit
from ...ui.file_list_widget import FileListWidget
from ...settings import AppSettings
from ...workflow import WorkflowJob, graph_source_nodes, workflow_output_device_name


class WorkflowSourcePanel(QGroupBox):
    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        settings: AppSettings | None,
        update_text_field: Callable[[str, str], None],
        update_bool_field: Callable[[str, bool], None],
        on_file_pattern_changed: Callable[[str], None],
        on_device_changed: Callable[[int], None],
        on_files_changed: Callable[[], None],
        on_pi_files_changed: Callable[[], None],
        on_match_data_changed: Callable[[str, str, str], None],
        on_load_pi_camera_files: Callable[[], None],
    ):
        super().__init__("Quelle", parent)
        self._settings = settings
        self._update_text_field = update_text_field
        self._update_bool_field = update_bool_field
        self._on_file_pattern_changed = on_file_pattern_changed
        self._on_device_changed = on_device_changed
        self._on_files_changed = on_files_changed
        self._on_pi_files_changed = on_pi_files_changed
        self._on_match_data_changed = on_match_data_changed
        self._on_load_pi_camera_files = on_load_pi_camera_files
        self._build_ui()

    def _build_ui(self) -> None:
        self.setStyleSheet(
            "QGroupBox { font-weight: 700; color: #0F172A; border: 1px solid #D7E0EA; border-radius: 12px; margin-top: 8px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }"
        )
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)

        self._source_mode_label = QLabel()
        self._source_mode_label.setStyleSheet("color: #475569; font-weight: 600;")
        layout.addWidget(self._source_mode_label)

        self._source_detail_label = QLabel()
        self._source_detail_label.setWordWrap(True)
        self._source_detail_label.setStyleSheet("color: #64748B;")
        layout.addWidget(self._source_detail_label)

        self._source_fields = QWidget(self)
        fields_layout = QVBoxLayout(self._source_fields)
        fields_layout.setContentsMargins(0, 0, 0, 0)
        fields_layout.setSpacing(8)

        self._file_list_widget: FileListWidget | None = None
        self._source_mode_widgets: dict[str, QWidget] = {}
        self._source_mode_widgets["files"] = self._build_files_source_editor()
        self._source_mode_widgets["folder_scan"] = self._build_folder_source_editor()
        self._source_mode_widgets["pi_download"] = self._build_pi_source_editor()
        for widget in self._source_mode_widgets.values():
            fields_layout.addWidget(widget)
        layout.addWidget(self._source_fields)

    def _build_files_source_editor(self) -> QWidget:
        wrapper = QWidget(self)
        layout = QVBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        if self._settings is not None:
            self._file_list_widget = FileListWidget(
                last_dir_getter=lambda settings=self._settings: settings.last_directory if settings is not None else "",
                last_dir_setter=self._save_last_dir,
            )
            self._file_list_widget.match_data_changed.connect(self._on_match_data_changed)
            self._file_list_widget.files_changed.connect(self._on_files_changed)
            layout.addWidget(self._file_list_widget)
        else:
            hint = QLabel("Ohne geladene Einstellungen ist die Dateiliste hier nicht editierbar.")
            hint.setWordWrap(True)
            hint.setStyleSheet("color: #92400E;")
            layout.addWidget(hint)

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(6)

        self._files_dst_edit = AlwaysVisiblePlaceholderLineEdit()
        self._files_dst_edit.effectiveTextChanged.connect(lambda text: self._update_text_field("copy_destination", text))
        files_dst_btn = self._browse_btn(lambda: self.browse_dir(self._files_dst_edit, "Zielordner wählen"))
        form.addRow("Zielordner:", self._hbox(self._files_dst_edit, files_dst_btn))

        self._files_move_cb = QCheckBox("Quelldateien in Zielordner verschieben")
        self._files_move_cb.toggled.connect(lambda checked: self._update_bool_field("move_files", checked))
        form.addRow("", self._files_move_cb)
        layout.addLayout(form)
        return wrapper

    def _build_folder_source_editor(self) -> QWidget:
        wrapper = QWidget(self)
        form = QFormLayout(wrapper)
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(8)

        self._folder_src_edit = QLineEdit()
        self._folder_src_edit.setPlaceholderText("Quellordner")
        self._folder_src_edit.textChanged.connect(lambda text: self._update_text_field("source_folder", text))
        folder_src_btn = self._browse_btn(lambda: self.browse_dir(self._folder_src_edit, "Quellordner wählen"))
        form.addRow("Quellordner:", self._hbox(self._folder_src_edit, folder_src_btn))

        self._file_pattern_edit = QLineEdit()
        self._file_pattern_edit.setPlaceholderText("*.mp4")
        self._file_pattern_edit.textChanged.connect(self._on_file_pattern_changed)
        form.addRow("Datei-Muster:", self._file_pattern_edit)

        self._folder_dst_edit = AlwaysVisiblePlaceholderLineEdit()
        self._folder_dst_edit.effectiveTextChanged.connect(lambda text: self._update_text_field("copy_destination", text))
        folder_dst_btn = self._browse_btn(lambda: self.browse_dir(self._folder_dst_edit, "Zielordner wählen"))
        form.addRow("Zielordner:", self._hbox(self._folder_dst_edit, folder_dst_btn))

        self._move_files_cb = QCheckBox("Quelldateien nach Verarbeitung verschieben")
        self._move_files_cb.toggled.connect(lambda checked: self._update_bool_field("move_files", checked))
        form.addRow("", self._move_files_cb)

        self._folder_prefix_edit = QLineEdit()
        self._folder_prefix_edit.setPlaceholderText("optional")
        self._folder_prefix_edit.textChanged.connect(lambda text: self._update_text_field("output_prefix", text))
        form.addRow("Ausgabe-Präfix:", self._folder_prefix_edit)
        return wrapper

    def _build_pi_source_editor(self) -> QWidget:
        wrapper = QWidget(self)
        layout = QVBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(6)

        self._device_combo = QComboBox()
        self._device_combo.addItem("(Gerät wählen)", "")
        if self._settings is not None:
            for dev in self._settings.cameras.devices:
                self._device_combo.addItem(f"{dev.name}  ({dev.ip})", dev.name)
        self._device_combo.currentIndexChanged.connect(self._on_device_changed)
        form.addRow("Gerät:", self._device_combo)

        self._pi_dest_edit = AlwaysVisiblePlaceholderLineEdit()
        self._pi_dest_edit.effectiveTextChanged.connect(lambda text: self._update_text_field("download_destination", text))
        form.addRow("Zielverzeichnis:", self._pi_dest_edit)

        self._delete_after_dl_cb = QCheckBox("Aufnahmen nach Download löschen")
        self._delete_after_dl_cb.toggled.connect(lambda checked: self._update_bool_field("delete_after_download", checked))
        form.addRow("", self._delete_after_dl_cb)

        self._pi_prefix_edit = QLineEdit()
        self._pi_prefix_edit.setPlaceholderText("optional")
        self._pi_prefix_edit.textChanged.connect(lambda text: self._update_text_field("output_prefix", text))
        form.addRow("Ausgabe-Präfix:", self._pi_prefix_edit)
        layout.addLayout(form)

        load_row = QHBoxLayout()
        self._pi_load_btn = QPushButton("📋 Dateien von Kamera laden")
        self._pi_load_btn.clicked.connect(self._on_load_pi_camera_files)
        load_row.addWidget(self._pi_load_btn)
        self._pi_load_status = QLabel("")
        self._pi_load_status.setStyleSheet("color: #64748B;")
        load_row.addWidget(self._pi_load_status, 1)
        layout.addLayout(load_row)

        self._pi_file_list = FileListWidget(
            last_dir_getter=lambda settings=self._settings: settings.last_directory if settings is not None else "",
            last_dir_setter=lambda _directory: None,
        )
        self._pi_file_list.match_data_changed.connect(self._on_match_data_changed)
        self._pi_file_list.files_changed.connect(self._on_pi_files_changed)
        self._pi_file_list.setVisible(False)
        self._pi_selection_label = QLabel("Auswahl:")
        self._pi_selection_label.setStyleSheet("color: #475569; font-weight: 600;")
        self._pi_selection_label.setVisible(False)
        layout.addWidget(self._pi_selection_label)
        layout.addWidget(self._pi_file_list)
        return wrapper

    @staticmethod
    def _browse_btn(callback) -> QPushButton:
        btn = QPushButton("…")
        btn.setFixedWidth(32)
        btn.clicked.connect(callback)
        return btn

    @staticmethod
    def _hbox(*widgets) -> QWidget:
        wrapper = QWidget()
        layout = QHBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)
        for widget in widgets:
            if widget is None:
                layout.addStretch()
            else:
                layout.addWidget(widget)
        return wrapper

    def browse_dir(self, line_edit: QLineEdit, title: str) -> None:
        settings_last_dir = self._settings.last_directory if self._settings is not None else ""
        start = line_edit.text().strip() or settings_last_dir or str(Path.home())
        chosen = QFileDialog.getExistingDirectory(self, title, start)
        if chosen:
            line_edit.setText(chosen)
            self._save_last_dir(chosen)

    def _save_last_dir(self, directory: str) -> None:
        if self._settings is None:
            return
        self._settings.last_directory = directory
        self._settings.save()

    def set_mode(self, source_mode: str) -> None:
        for mode, widget in self._source_mode_widgets.items():
            widget.setVisible(mode == source_mode)

    def load_from_job(self, job: WorkflowJob) -> None:
        self._apply_destination_placeholders(job)
        self._files_dst_edit.setText(job.copy_destination)
        self._files_move_cb.setChecked(job.move_files)
        self._folder_src_edit.setText(job.source_folder)
        self._file_pattern_edit.setText(job.file_pattern or "*.mp4")
        self._folder_dst_edit.setText(job.copy_destination)
        self._move_files_cb.setChecked(job.move_files)
        self._folder_prefix_edit.setText(job.output_prefix)
        device_index = self._device_combo.findData(job.device_name)
        self._device_combo.setCurrentIndex(device_index if device_index >= 0 else 0)
        self._pi_dest_edit.setText(job.download_destination)
        self._delete_after_dl_cb.setChecked(job.delete_after_download)
        self._pi_prefix_edit.setText(job.output_prefix)
        if self._file_list_widget is not None:
            self._file_list_widget.load(job.files)
        if job.source_mode == "pi_download" and job.files:
            self._pi_file_list.load(job.files)
            self._pi_file_list.setVisible(True)
            self._pi_selection_label.setVisible(True)
            self._pi_load_status.setText(f"✓ {len(job.files)} Aufnahme(n) vorgemerkt.")
            self._pi_load_status.setStyleSheet("color: green;")
        else:
            self._pi_file_list.setVisible(False)
            self._pi_selection_label.setVisible(False)

    def refresh_from_job(self, job: WorkflowJob) -> None:
        self._apply_destination_placeholders(job)
        source_labels = {
            "files": "Direkte Dateiauswahl",
            "folder_scan": "Ordner-Scan",
            "pi_download": "Pi-Kamera-Download",
        }
        self._source_mode_label.setText(f"Quellmodus: {source_labels.get(job.source_mode, job.source_mode)}")
        if job.source_mode == "files":
            merge_count = len({file.merge_group_id for file in job.files if file.merge_group_id})
            detail = f"{len(job.files)} Datei(en) im Job, {merge_count} Merge-Gruppe(n). Gruppierung kommt aus dem Canvas; gemeinsamer Output-Titel liegt am Merge-Node."
        elif job.source_mode == "folder_scan":
            detail = f"Ordner: {job.source_folder or '–'} | Muster: {job.file_pattern or '*.mp4'}"
        else:
            file_count = len(job.files)
            default_dest = ""
            if self._settings is not None:
                default_dest = self._settings.workflow_raw_dir_for(job.name, job.device_name)
            detail = f"Gerät: {job.device_name or '–'} | Ziel: {job.download_destination or default_dest or '–'} | Auswahl: {file_count} Datei(en)"
        if len(graph_source_nodes(job)) > 1:
            detail += " | Mehrere Quellen werden gemeinsam über das Canvas orchestriert."
        self._source_detail_label.setText(detail)
        self.set_mode(job.source_mode)
        self._pi_selection_label.setVisible(job.source_mode == "pi_download" and bool(job.files))

    def _apply_destination_placeholders(self, job: WorkflowJob) -> None:
        if self._settings is None:
            self._files_dst_edit.setPlaceholderText("Dateien am Quellort verarbeiten")
            self._folder_dst_edit.setPlaceholderText("neben der Quelldatei")
            self._pi_dest_edit.setPlaceholderText("Lokales Zielverzeichnis")
            return

        job_name = job.name
        output_device_name = workflow_output_device_name(job)
        default_raw_dir = self._settings.workflow_raw_dir_for(job_name, output_device_name)
        if default_raw_dir:
            self._files_dst_edit.setPlaceholderText(default_raw_dir)
            self._folder_dst_edit.setPlaceholderText(default_raw_dir)
        else:
            self._files_dst_edit.setPlaceholderText("Dateien am Quellort verarbeiten")
            self._folder_dst_edit.setPlaceholderText("neben der Quelldatei")

        default_pi_dir = self._settings.workflow_raw_dir_for(job_name, job.device_name)
        self._pi_dest_edit.setPlaceholderText(default_pi_dir or "Lokales Zielverzeichnis")