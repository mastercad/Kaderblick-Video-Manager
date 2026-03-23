from PySide6.QtCore import QThread, Signal


class _CameraListWorker(QThread):
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