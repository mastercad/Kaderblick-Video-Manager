from pathlib import Path

from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QRadioButton,
    QSizePolicy,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from ...ui import AlwaysVisiblePlaceholderLineEdit
from ...integrations.kaderblick import fetch_cameras, fetch_video_types
from ...workflow import FileEntry
from ..file_list_widget import FileListWidget
from .camera_worker import _CameraListWorker


class JobEditorSourceMixin:
    def _build_page_source(self) -> QWidget:
        page = QWidget()
        lay = QVBoxLayout(page)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        name_row = QFormLayout()
        self._name_edit = QLineEdit()
        self._name_edit.setPlaceholderText(
            "Kurzbezeichnung (wird automatisch generiert, wenn leer)"
        )
        self._name_edit.textChanged.connect(self._update_output_placeholders)
        name_row.addRow("Workflow-Name:", self._name_edit)
        lay.addLayout(name_row)

        cards_label = QLabel("Dateiquelle")
        cards_label.setStyleSheet("font-weight:bold; font-size:13px; color:#333;")
        lay.addWidget(cards_label)

        cards_row = QHBoxLayout()
        cards_row.setSpacing(10)
        self._mode_group = QButtonGroup(self)
        for mode_id, icon, title, desc in [
            (0, "📁", "Dateien auswählen", "Einzelne oder mehrere\nDateien direkt wählen"),
            (1, "📂", "Ordner scannen", "Alle passenden Dateien\nin einem Ordner verarbeiten"),
            (2, "📷", "Pi-Kamera", "Aufnahmen von einer\nRaspberry-Pi-Kamera laden"),
        ]:
            cards_row.addWidget(self._make_mode_card(mode_id, icon, title, desc))
        lay.addLayout(cards_row)

        self._source_stack = QStackedWidget()
        self._source_stack.addWidget(self._build_files_panel())
        self._source_stack.addWidget(self._build_folder_panel())
        self._source_stack.addWidget(self._build_pi_panel())
        self._mode_group.idToggled.connect(
            lambda mode_id, on: self._source_stack.setCurrentIndex(mode_id) if on else None
        )
        lay.addWidget(self._source_stack, stretch=1)
        return page

    def _make_mode_card(self, mode_id: int, icon: str, title: str, desc: str) -> QWidget:
        rb = QRadioButton()
        self._mode_group.addButton(rb, mode_id)

        frame = QFrame()
        frame.setCursor(Qt.PointingHandCursor)
        frame.setFrameShape(QFrame.StyledPanel)
        frame.setStyleSheet(
            "QFrame{border:2px solid #d0d7de;border-radius:10px;"
            "background:#fafbfc;padding:6px;}"
            "QFrame:hover{border-color:#3a7bde;background:#f0f5ff;}"
        )
        frame.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        inner = QVBoxLayout(frame)
        inner.setContentsMargins(10, 12, 10, 12)
        inner.setSpacing(4)

        icon_lbl = QLabel(icon)
        icon_lbl.setAlignment(Qt.AlignCenter)
        icon_lbl.setStyleSheet("font-size:32px; border:none; background:transparent;")
        inner.addWidget(icon_lbl)

        title_lbl = QLabel(title)
        title_lbl.setAlignment(Qt.AlignCenter)
        title_lbl.setStyleSheet(
            "font-weight:bold; font-size:12px; border:none; background:transparent;"
        )
        inner.addWidget(title_lbl)

        desc_lbl = QLabel(desc)
        desc_lbl.setAlignment(Qt.AlignCenter)
        desc_lbl.setWordWrap(True)
        desc_lbl.setStyleSheet(
            "color:#666; font-size:11px; border:none; background:transparent;"
        )
        inner.addWidget(desc_lbl)

        inner.addWidget(rb, alignment=Qt.AlignCenter)
        frame.mousePressEvent = lambda _e, button=rb: button.setChecked(True)
        return frame

    def _build_files_panel(self) -> QWidget:
        widget = QWidget()
        lay = QVBoxLayout(widget)
        lay.setContentsMargins(0, 4, 0, 0)
        lay.setSpacing(4)
        self._file_list = FileListWidget(
            last_dir_getter=lambda: self._settings.last_directory,
            last_dir_setter=self._save_last_dir,
        )
        self._file_list.match_data_changed.connect(self._on_match_data_from_files)
        lay.addWidget(self._file_list)

        form = QFormLayout()
        form.setContentsMargins(0, 4, 0, 0)
        form.setSpacing(6)

        self._files_dst_edit = AlwaysVisiblePlaceholderLineEdit()
        dst_btn = self._browse_btn(lambda: self._browse_dir(self._files_dst_edit, "Zielordner wählen"))
        form.addRow("Zielordner:", self._hbox(self._files_dst_edit, dst_btn))

        self._files_move_cb = QCheckBox("Quelldateien in Zielordner verschieben (statt kopieren)")
        form.addRow("", self._files_move_cb)

        lay.addLayout(form)
        return widget

    def _build_folder_panel(self) -> QWidget:
        widget = QWidget()
        form = QFormLayout(widget)
        form.setContentsMargins(0, 4, 0, 0)

        self._folder_src_edit = QLineEdit()
        self._folder_src_edit.setPlaceholderText("Quellordner …")
        src_btn = self._browse_btn(lambda: self._browse_dir(self._folder_src_edit, "Quellordner wählen"))
        form.addRow("Quellordner:", self._hbox(self._folder_src_edit, src_btn))

        self._file_pattern_edit = QLineEdit("*.mp4")
        self._file_pattern_edit.setPlaceholderText("*.mp4")
        form.addRow("Datei-Muster:", self._file_pattern_edit)

        self._folder_dst_edit = AlwaysVisiblePlaceholderLineEdit()
        dst_btn = self._browse_btn(lambda: self._browse_dir(self._folder_dst_edit, "Zielordner wählen"))
        form.addRow("Zielordner:", self._hbox(self._folder_dst_edit, dst_btn))

        self._move_files_cb = QCheckBox("Quelldateien nach Verarbeitung in Zielordner verschieben")
        form.addRow("", self._move_files_cb)

        self._folder_prefix_edit = QLineEdit()
        self._folder_prefix_edit.setPlaceholderText("leer = Originaldateiname behalten")
        form.addRow("Ausgabe-Präfix:", self._folder_prefix_edit)
        return widget

    def _build_pi_panel(self) -> QWidget:
        widget = QWidget()
        lay = QVBoxLayout(widget)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(4)

        form = QFormLayout()
        form.setContentsMargins(0, 4, 0, 0)
        form.setSpacing(6)

        self._device_combo = QComboBox()
        self._device_combo.addItem("(Gerät wählen)", "")
        for dev in self._settings.cameras.devices:
            self._device_combo.addItem(f"{dev.name}  ({dev.ip})", dev.name)
        self._device_combo.currentIndexChanged.connect(lambda _index: self._update_output_placeholders())
        form.addRow("Gerät:", self._device_combo)

        self._pi_dest_edit = AlwaysVisiblePlaceholderLineEdit()
        pi_btn = self._browse_btn(lambda: self._browse_dir(self._pi_dest_edit, "Zielverzeichnis wählen"))
        form.addRow("Zielverzeichnis:", self._hbox(self._pi_dest_edit, pi_btn))

        self._delete_after_dl_cb = QCheckBox("Aufnahmen nach Download von Kamera löschen")
        form.addRow("", self._delete_after_dl_cb)

        self._pi_prefix_edit = QLineEdit()
        self._pi_prefix_edit.setPlaceholderText("leer = Originaldateiname")
        form.addRow("Ausgabe-Präfix:", self._pi_prefix_edit)
        lay.addLayout(form)

        load_row = QHBoxLayout()
        self._pi_load_btn = QPushButton("📋 Dateien von Kamera laden")
        self._pi_load_btn.setToolTip(
            "Verbindet sich per SFTP mit der Kamera und listet vorhandene Aufnahmen.\n"
            "Anschließend kannst du YT-Titel und Playlist pro Datei setzen\n"
            "und nur ausgewählte Dateien herunterladen."
        )
        self._pi_load_btn.clicked.connect(self._load_pi_camera_files)
        load_row.addWidget(self._pi_load_btn)
        self._pi_load_status = QLabel("")
        self._pi_load_status.setStyleSheet("color:gray; font-style:italic;")
        load_row.addWidget(self._pi_load_status, stretch=1)
        lay.addLayout(load_row)

        self._pi_file_list = FileListWidget(
            last_dir_getter=lambda: self._settings.last_directory,
            last_dir_setter=lambda _directory: None,
        )
        self._pi_file_list.setVisible(False)
        lay.addWidget(self._pi_file_list)
        self._update_output_placeholders()
        return widget

    def _update_output_placeholders(self, *_args) -> None:
        workflow_name = self._name_edit.text().strip()
        default_raw_dir = self._settings.workflow_raw_dir_for(workflow_name)
        if default_raw_dir:
            self._files_dst_edit.setPlaceholderText(default_raw_dir)
            self._folder_dst_edit.setPlaceholderText(default_raw_dir)
        else:
            self._files_dst_edit.setPlaceholderText("Dateien am Quellort verarbeiten")
            self._folder_dst_edit.setPlaceholderText("neben der Quelldatei")

        device_name = self._device_combo.currentData() or ""
        default_pi_dir = self._settings.workflow_raw_dir_for(workflow_name, device_name)
        if default_pi_dir:
            self._pi_dest_edit.setPlaceholderText(default_pi_dir)
            return
        self._pi_dest_edit.setPlaceholderText("Lokales Zielverzeichnis …")

    def _load_pi_camera_files(self) -> None:
        from . import QMessageBox

        dev_name = self._device_combo.currentData()
        if not dev_name:
            QMessageBox.warning(self, "Kein Gerät", "Bitte zuerst ein Gerät auswählen.")
            return
        dev = next((d for d in self._settings.cameras.devices if d.name == dev_name), None)
        if dev is None:
            QMessageBox.warning(
                self,
                "Gerät nicht gefunden",
                f"Gerät '{dev_name}' nicht in der Konfiguration.",
            )
            return
        self._pi_load_btn.setEnabled(False)
        self._pi_load_status.setText("Verbinde …")
        self._pi_load_status.setStyleSheet("color:gray; font-style:italic;")
        QCoreApplication.processEvents()
        self._pi_list_worker = _CameraListWorker(dev, self._settings.cameras, self)
        self._pi_list_worker.finished.connect(self._on_camera_files_loaded)
        self._pi_list_worker.error.connect(self._on_camera_files_error)
        self._pi_list_worker.start()

    def _on_camera_files_loaded(self, files: list) -> None:
        self._pi_load_btn.setEnabled(True)
        if not files:
            self._pi_load_status.setText("Keine Aufnahmen auf der Kamera gefunden.")
            self._pi_load_status.setStyleSheet("color:orange;")
            return
        dev_name = self._device_combo.currentData()
        workflow_name = self._name_edit.text().strip()
        default_dest = self._settings.workflow_raw_dir_for(workflow_name, dev_name)
        dest = self._pi_dest_edit.text().strip() or default_dest
        entries = [
            FileEntry(
                source_path=str(Path(dest) / f"{file_info['base']}.mjpg"),
                source_size_bytes=int(file_info.get("total_size") or 0),
            )
            for file_info in files
        ]
        self._pi_file_list.load(entries)
        self._pi_file_list.setVisible(True)
        self._pi_load_status.setText(f"✓ {len(files)} Aufnahme(n) gefunden.")
        self._pi_load_status.setStyleSheet("color:green;")

    def _on_camera_files_error(self, msg: str) -> None:
        self._pi_load_btn.setEnabled(True)
        self._pi_load_status.setText(f"❌ {msg}")
        self._pi_load_status.setStyleSheet("color:#c0392b; font-weight:bold;")

    def _on_match_data_from_files(self, home: str, away: str, date_iso: str) -> None:
        if home:
            self._tc_home_edit.setText(home)
        if away:
            self._tc_away_edit.setText(away)
        if date_iso:
            try:
                year, month, day = date_iso.split("-")
                self._tc_date_edit.setText(f"{day}.{month}.{year}")
            except Exception:
                self._tc_date_edit.setText(date_iso)

    def _open_match_editor_for_playlist(self) -> None:
        from ...integrations.youtube_title_editor import MatchData, YouTubeTitleEditorDialog

        tc_date = self._tc_date_edit.text().strip()
        tc_date_iso = ""
        if tc_date:
            try:
                parts = tc_date.split(".")
                tc_date_iso = f"{parts[2]}-{parts[1]}-{parts[0]}" if len(parts) == 3 else tc_date
            except Exception:
                tc_date_iso = tc_date
        initial = MatchData(
            competition=self._yt_competition.strip(),
            home_team=self._tc_home_edit.text().strip(),
            away_team=self._tc_away_edit.text().strip(),
            date_iso=tc_date_iso,
        )
        dlg = YouTubeTitleEditorDialog(self, mode="playlist", initial_match=initial)
        if dlg.exec():
            self._yt_playlist_edit.setText(dlg.playlist_title)
            self._yt_competition = dlg.match_data.competition
            match = dlg.match_data
            if match.home_team:
                self._tc_home_edit.setText(match.home_team)
            if match.away_team:
                self._tc_away_edit.setText(match.away_team)
            if match.date_iso:
                try:
                    year, month, day = match.date_iso.split("-")
                    self._tc_date_edit.setText(f"{day}.{month}.{year}")
                except Exception:
                    self._tc_date_edit.setText(match.date_iso)

    def _kb_load_api_data(self, force: bool = False) -> None:
        if self._kb_api_loaded and not force:
            return
        kb = self._settings.kaderblick
        active_token = kb.jwt_token if kb.auth_mode == "jwt" else kb.bearer_token
        if not active_token:
            self._file_list.set_kaderblick_options([], [])
            mode_lbl = "JWT-Token" if kb.auth_mode == "jwt" else "Bearer-Token"
            self._kb_status_label.setText(
                f"⚠ Kein {mode_lbl} konfiguriert.\n"
                "Bitte unter Einstellungen → Kaderblick eintragen."
            )
            self._kb_status_label.setStyleSheet("color:orange;")
            self._kb_api_loaded = True
            return

        self._kb_reload_btn.setEnabled(False)
        self._kb_status_label.setText("⏳ Lade von API …")
        self._kb_status_label.setStyleSheet("color:gray;")
        QCoreApplication.processEvents()

        errors, types, cameras = [], [], []
        try:
            types = fetch_video_types(kb)
        except Exception as exc:
            errors.append(f"Video-Typen: {exc}")
        try:
            cameras = fetch_cameras(kb)
        except Exception as exc:
            errors.append(f"Kameras: {exc}")

        self._file_list.set_kaderblick_options(types, cameras)
        if errors:
            self._kb_status_label.setText("❌ Fehler:\n" + "\n".join(errors))
            self._kb_status_label.setStyleSheet("color:red;")
        else:
            self._kb_status_label.setText(f"✅ {len(types)} Typen, {len(cameras)} Kameras geladen.")
            self._kb_status_label.setStyleSheet("color:green;")
        self._kb_reload_btn.setEnabled(True)
        self._kb_api_loaded = True