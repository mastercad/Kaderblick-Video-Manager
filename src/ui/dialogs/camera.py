"""Camera settings dialogs."""

from pathlib import Path

from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHeaderView,
    QHBoxLayout,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from ...settings import AppSettings, DeviceSettings


class _DeviceEditDialog(QDialog):
    def __init__(self, parent, device=None):
        super().__init__(parent)
        self.setWindowTitle("Gerät bearbeiten")
        self.setMinimumWidth(400)

        dev = device or DeviceSettings()
        layout = QVBoxLayout(self)
        group = QGroupBox("Verbindungsdetails")
        form = QFormLayout()
        self.name_edit = QLineEdit(dev.name)
        form.addRow("Name:", self.name_edit)
        self.ip_edit = QLineEdit(dev.ip)
        form.addRow("IP-Adresse:", self.ip_edit)
        self.port_spin = QSpinBox()
        self.port_spin.setRange(1, 65535)
        self.port_spin.setValue(dev.port)
        form.addRow("Port:", self.port_spin)
        self.user_edit = QLineEdit(dev.username)
        form.addRow("Benutzername:", self.user_edit)
        self.pw_edit = QLineEdit(dev.password)
        self.pw_edit.setPlaceholderText("(leer lassen wenn SSH-Key)")
        form.addRow("Passwort:", self.pw_edit)
        key_row = QHBoxLayout()
        self.key_edit = QLineEdit(dev.ssh_key)
        self.key_edit.setPlaceholderText("(leer lassen wenn Passwort)")
        key_row.addWidget(self.key_edit)
        key_browse = QPushButton("…")
        key_browse.setFixedWidth(32)
        key_browse.clicked.connect(self._browse_key)
        key_row.addWidget(key_browse)
        form.addRow("SSH-Key:", key_row)
        group.setLayout(form)
        layout.addWidget(group)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _browse_key(self):
        path, _ = QFileDialog.getOpenFileName(self, "SSH-Key wählen", str(Path.home() / ".ssh"))
        if path:
            self.key_edit.setText(path)

    def _accept(self):
        if not self.name_edit.text().strip():
            QMessageBox.warning(self, "Pflichtfeld", "Bitte einen Namen eingeben.")
            return
        if not self.ip_edit.text().strip():
            QMessageBox.warning(self, "Pflichtfeld", "Bitte eine IP-Adresse eingeben.")
            return
        self.accept()

    def result_device(self):
        return DeviceSettings(
            name=self.name_edit.text().strip(),
            ip=self.ip_edit.text().strip(),
            port=self.port_spin.value(),
            username=self.user_edit.text().strip(),
            password=self.pw_edit.text(),
            ssh_key=self.key_edit.text().strip(),
        )


class CameraSettingsDialog(QDialog):
    def __init__(self, parent, settings: AppSettings):
        super().__init__(parent)
        self.setWindowTitle("Kamera-Einstellungen")
        self.resize(700, 580)
        self.setMinimumSize(560, 460)
        self._settings = settings
        cam = settings.cameras

        layout = QVBoxLayout(self)
        layout.setSpacing(8)
        path_group = QGroupBox("Pfade")
        path_form = QFormLayout()
        self._source_edit = QLineEdit(cam.source)
        self._source_edit.setPlaceholderText("/home/kaderblick/camera_api/recordings")
        path_form.addRow("Quellpfad (Pi):", self._source_edit)
        dest_row = QHBoxLayout()
        self._dest_edit = QLineEdit(cam.destination)
        dest_row.addWidget(self._dest_edit)
        dest_browse = QPushButton("…")
        dest_browse.setFixedWidth(32)
        dest_browse.clicked.connect(self._browse_dest)
        dest_row.addWidget(dest_browse)
        path_form.addRow("Zielordner (lokal):", dest_row)
        path_group.setLayout(path_form)
        layout.addWidget(path_group)

        opt_group = QGroupBox("Optionen")
        opt_layout = QVBoxLayout()
        self._delete_chk = QCheckBox("Quelldateien nach erfolgreichem Download löschen")
        self._delete_chk.setChecked(cam.delete_after_download)
        opt_layout.addWidget(self._delete_chk)
        self._convert_chk = QCheckBox("Nach Download automatisch konvertieren")
        self._convert_chk.setChecked(cam.auto_convert)
        opt_layout.addWidget(self._convert_chk)
        opt_group.setLayout(opt_layout)
        layout.addWidget(opt_group)

        dev_group = QGroupBox("Geräte (Raspberry Pi SSH)")
        dev_layout = QVBoxLayout()
        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels(["Name", "IP", "Port", "Benutzer", "Passwort", "SSH-Key"])
        header = self._table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.Fixed)
        header.resizeSection(2, 60)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.Stretch)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        self._table.setSelectionMode(QTableWidget.SingleSelection)
        self._table.verticalHeader().setVisible(False)
        self._table.doubleClicked.connect(self._edit_device)
        dev_layout.addWidget(self._table)
        btn_row = QHBoxLayout()
        add_btn = QPushButton("＋ Hinzufügen")
        add_btn.clicked.connect(self._add_device)
        btn_row.addWidget(add_btn)
        edit_btn = QPushButton("✏ Bearbeiten")
        edit_btn.clicked.connect(self._edit_device)
        btn_row.addWidget(edit_btn)
        remove_btn = QPushButton("✕ Entfernen")
        remove_btn.clicked.connect(self._remove_device)
        btn_row.addWidget(remove_btn)
        btn_row.addStretch()
        dev_layout.addLayout(btn_row)
        dev_group.setLayout(dev_layout)
        layout.addWidget(dev_group, stretch=1)

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Save).setText("Speichern")
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self._populate_table()

    def _populate_table(self):
        devices = self._settings.cameras.devices
        self._table.setRowCount(len(devices))
        for row, dev in enumerate(devices):
            self._table.setItem(row, 0, QTableWidgetItem(dev.name))
            self._table.setItem(row, 1, QTableWidgetItem(dev.ip))
            self._table.setItem(row, 2, QTableWidgetItem(str(dev.port)))
            self._table.setItem(row, 3, QTableWidgetItem(dev.username))
            self._table.setItem(row, 4, QTableWidgetItem("***" if dev.password else "—"))
            self._table.setItem(row, 5, QTableWidgetItem(dev.ssh_key or "—"))

    def _browse_dest(self):
        folder = QFileDialog.getExistingDirectory(self, "Zielordner wählen", self._dest_edit.text() or str(Path.home()))
        if folder:
            self._dest_edit.setText(folder)

    def _add_device(self):
        dlg = _DeviceEditDialog(self)
        if dlg.exec():
            self._settings.cameras.devices.append(dlg.result_device())
            self._populate_table()

    def _edit_device(self):
        row = self._table.currentRow()
        if row < 0:
            return
        dlg = _DeviceEditDialog(self, self._settings.cameras.devices[row])
        if dlg.exec():
            self._settings.cameras.devices[row] = dlg.result_device()
            self._populate_table()

    def _remove_device(self):
        row = self._table.currentRow()
        if row < 0:
            return
        name = self._settings.cameras.devices[row].name
        if QMessageBox.question(self, "Gerät entfernen", f'Gerät "{name}" wirklich entfernen?', QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            self._settings.cameras.devices.pop(row)
            self._populate_table()

    def _save(self):
        cam = self._settings.cameras
        cam.source = self._source_edit.text().strip()
        cam.destination = self._dest_edit.text().strip()
        cam.delete_after_download = self._delete_chk.isChecked()
        cam.auto_convert = self._convert_chk.isChecked()
        self.accept()