from __future__ import annotations

from pathlib import Path
from typing import Callable

from PySide6.QtCore import QCoreApplication, QThread, Signal
from PySide6.QtWidgets import QMessageBox, QWidget

from ...integrations.kaderblick import fetch_cameras, fetch_video_types
from ...settings import AppSettings
from ...workflow import FileEntry


class CameraListWorker(QThread):
    finished = Signal(list)
    error = Signal(str)

    def __init__(self, device, config, parent=None):
        super().__init__(parent)
        self._device = device
        self._config = config

    def run(self):
        from ...transfer.downloader import list_camera_files

        try:
            self.finished.emit(list_camera_files(self._device, self._config))
        except Exception as exc:
            self.error.emit(str(exc))


class WorkflowExternalDataController:
    def __init__(
        self,
        *,
        parent: QWidget,
        get_settings: Callable[[], AppSettings | None],
        get_file_list_widget: Callable[[], object | None],
        get_pi_file_list: Callable[[], object | None],
        get_kaderblick_reload_button: Callable[[], object],
        get_kaderblick_status_label: Callable[[], object],
        get_device_name: Callable[[], str],
        get_workflow_name: Callable[[], str],
        get_pi_destination: Callable[[], str],
        get_pi_load_button: Callable[[], object],
        get_pi_load_status: Callable[[], object],
        fetch_video_types_fn: Callable[[object], list[dict]],
        fetch_cameras_fn: Callable[[object], list[dict]],
        on_kaderblick_options_loaded: Callable[[list[dict], list[dict]], None],
        on_pi_entries_loaded: Callable[[list[FileEntry]], None],
        on_pi_load_failed: Callable[[], None],
    ) -> None:
        self._parent = parent
        self._get_settings = get_settings
        self._get_file_list_widget = get_file_list_widget
        self._get_pi_file_list = get_pi_file_list
        self._get_kaderblick_reload_button = get_kaderblick_reload_button
        self._get_kaderblick_status_label = get_kaderblick_status_label
        self._get_device_name = get_device_name
        self._get_workflow_name = get_workflow_name
        self._get_pi_destination = get_pi_destination
        self._get_pi_load_button = get_pi_load_button
        self._get_pi_load_status = get_pi_load_status
        self._fetch_video_types = fetch_video_types_fn
        self._fetch_cameras = fetch_cameras_fn
        self._on_kaderblick_options_loaded = on_kaderblick_options_loaded
        self._on_pi_entries_loaded = on_pi_entries_loaded
        self._on_pi_load_failed = on_pi_load_failed
        self._kb_api_loaded = False
        self._pi_list_worker: CameraListWorker | None = None

    def load_kaderblick_api_data(self, force: bool = False) -> None:
        settings = self._get_settings()
        if settings is None:
            return
        if self._kb_api_loaded and not force:
            return

        kb = settings.kaderblick
        active_token = kb.jwt_token if kb.auth_mode == "jwt" else kb.bearer_token
        if not active_token:
            file_list_widget = self._get_file_list_widget()
            if file_list_widget is not None:
                file_list_widget.set_kaderblick_options([], [])
            pi_file_list = self._get_pi_file_list()
            if pi_file_list is not None:
                pi_file_list.set_kaderblick_options([], [])
            self._on_kaderblick_options_loaded([], [])
            mode_label = "JWT-Token" if kb.auth_mode == "jwt" else "Bearer-Token"
            status_label = self._get_kaderblick_status_label()
            status_label.setText(f"⚠ Kein {mode_label} konfiguriert.")
            status_label.setStyleSheet("color: orange;")
            self._kb_api_loaded = False
            return

        reload_button = self._get_kaderblick_reload_button()
        status_label = self._get_kaderblick_status_label()
        reload_button.setEnabled(False)
        status_label.setText("⏳ Lade von API …")
        status_label.setStyleSheet("color: #64748B;")

        errors: list[str] = []
        video_types: list[dict] = []
        cameras: list[dict] = []
        try:
            video_types = self._fetch_video_types(kb)
        except Exception as exc:
            errors.append(f"Video-Typen: {exc}")
        try:
            cameras = self._fetch_cameras(kb)
        except Exception as exc:
            errors.append(f"Kameras: {exc}")

        file_list_widget = self._get_file_list_widget()
        if file_list_widget is not None:
            file_list_widget.set_kaderblick_options(video_types, cameras)
        pi_file_list = self._get_pi_file_list()
        if pi_file_list is not None:
            pi_file_list.set_kaderblick_options(video_types, cameras)
        self._on_kaderblick_options_loaded(video_types, cameras)

        if errors:
            status_label.setText("❌ Fehler:\n" + "\n".join(errors))
            status_label.setStyleSheet("color: red;")
        else:
            status_label.setText(f"✅ {len(video_types)} Typen, {len(cameras)} Kameras geladen.")
            status_label.setStyleSheet("color: green;")
        reload_button.setEnabled(True)
        self._kb_api_loaded = True

    def load_pi_camera_files(self) -> None:
        settings = self._get_settings()
        if settings is None:
            return
        device_name = self._get_device_name()
        if not device_name:
            QMessageBox.warning(self._parent, "Kein Gerät", "Bitte zuerst ein Pi-Kamera-Gerät auswählen.")
            return
        device = next((item for item in settings.cameras.devices if item.name == device_name), None)
        if device is None:
            QMessageBox.warning(self._parent, "Gerät nicht gefunden", f"Gerät '{device_name}' nicht in der Konfiguration.")
            return
        load_button = self._get_pi_load_button()
        status_label = self._get_pi_load_status()
        load_button.setEnabled(False)
        status_label.setText("Verbinde …")
        status_label.setStyleSheet("color: #64748B;")
        QCoreApplication.processEvents()
        self._pi_list_worker = CameraListWorker(device, settings.cameras, self._parent)
        self._pi_list_worker.finished.connect(self.on_camera_files_loaded)
        self._pi_list_worker.error.connect(self.on_camera_files_error)
        self._pi_list_worker.start()

    def on_camera_files_loaded(self, files: list) -> None:
        settings = self._get_settings()
        load_button = self._get_pi_load_button()
        status_label = self._get_pi_load_status()
        load_button.setEnabled(True)
        if not files:
            status_label.setText("Keine Aufnahmen auf der Kamera gefunden.")
            status_label.setStyleSheet("color: orange;")
            self._on_pi_load_failed()
            return

        default_destination = ""
        if settings is not None:
            workflow_name = self._get_workflow_name()
            device_name = self._get_device_name() or ""
            default_destination = settings.workflow_raw_dir_for(workflow_name, device_name)
        destination = self._get_pi_destination().strip() or default_destination
        entries = [
            FileEntry(
                source_path=str(Path(destination) / f"{item['base']}.mjpg"),
                source_size_bytes=int(item.get("total_size") or 0),
            )
            for item in files
        ]
        self._on_pi_entries_loaded(entries)
        status_label.setText(f"✓ {len(entries)} Aufnahme(n) gefunden.")
        status_label.setStyleSheet("color: green;")

    def on_camera_files_error(self, msg: str) -> None:
        load_button = self._get_pi_load_button()
        status_label = self._get_pi_load_status()
        load_button.setEnabled(True)
        status_label.setText(f"❌ {msg}")
        status_label.setStyleSheet("color: red; font-weight: 700;")
        self._on_pi_load_failed()