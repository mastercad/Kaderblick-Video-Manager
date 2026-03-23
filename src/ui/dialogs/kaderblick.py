"""Kaderblick settings dialog."""

from PySide6.QtWidgets import (
    QButtonGroup,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QRadioButton,
    QVBoxLayout,
    QWidget,
)

from ...settings import AppSettings


class KaderblickSettingsDialog(QDialog):
    def __init__(self, parent, settings: AppSettings):
        super().__init__(parent)
        self.setWindowTitle("Kaderblick-Einstellungen")
        self.setMinimumWidth(520)
        self.settings = settings
        kb = settings.kaderblick

        layout = QVBoxLayout(self)
        conn_group = QGroupBox("API-Verbindung")
        form = QFormLayout()
        self.base_url_edit = QLineEdit(kb.base_url)
        self.base_url_edit.setPlaceholderText("https://api.kaderblick.de")
        self.base_url_edit.setToolTip("Basis-URL der Kaderblick-API.\nStandardwert: https://api.kaderblick.de")
        form.addRow("Base-URL:", self.base_url_edit)

        auth_row = QWidget()
        auth_layout = QHBoxLayout(auth_row)
        auth_layout.setContentsMargins(0, 0, 0, 0)
        self._rb_jwt = QRadioButton("JWT (Cookie)")
        self._rb_bearer = QRadioButton("Bearer-Token")
        self._auth_group = QButtonGroup(self)
        self._auth_group.addButton(self._rb_jwt, 0)
        self._auth_group.addButton(self._rb_bearer, 1)
        auth_layout.addWidget(self._rb_jwt)
        auth_layout.addWidget(self._rb_bearer)
        auth_layout.addStretch()
        form.addRow("Auth-Modus:", auth_row)
        conn_group.setLayout(form)
        layout.addWidget(conn_group)

        self._jwt_group = QGroupBox("JWT-Authentifizierung")
        jwt_form = QFormLayout()
        self.jwt_token_edit = QLineEdit(kb.jwt_token)
        self.jwt_token_edit.setPlaceholderText("eyJ… (Browser-Cookie jwt_token)")
        self.jwt_token_edit.setToolTip(
            "JWT-Token aus dem Browser-Cookie jwt_token.\n"
            "Browser → F12 → Application → Cookies → jwt_token\n"
            "Den langen eyJ...-Wert kopieren."
        )
        self.jwt_token_edit.setEchoMode(QLineEdit.Password)
        jwt_form.addRow("JWT-Token:", self.jwt_token_edit)
        self.jwt_refresh_edit = QLineEdit(kb.jwt_refresh_token)
        self.jwt_refresh_edit.setPlaceholderText("optional – für automatische Token-Erneuerung")
        self.jwt_refresh_edit.setToolTip(
            "Refresh-Token aus dem Browser-Cookie jwt_refresh_token.\n"
            "Damit wird der JWT automatisch erneuert wenn er abläuft."
        )
        self.jwt_refresh_edit.setEchoMode(QLineEdit.Password)
        jwt_form.addRow("Refresh-Token:", self.jwt_refresh_edit)
        self._jwt_group.setLayout(jwt_form)
        layout.addWidget(self._jwt_group)

        self._bearer_group = QGroupBox("Bearer-Authentifizierung")
        bearer_form = QFormLayout()
        self.bearer_token_edit = QLineEdit(kb.bearer_token)
        self.bearer_token_edit.setPlaceholderText("API-Key / statischer Bearer-Token")
        self.bearer_token_edit.setToolTip("Statischer Bearer-Token für den Authorization-Header.")
        self.bearer_token_edit.setEchoMode(QLineEdit.Password)
        bearer_form.addRow("Bearer-Token:", self.bearer_token_edit)
        self._bearer_group.setLayout(bearer_form)
        layout.addWidget(self._bearer_group)

        hint = QLabel(
            "Tokens werden lokal in settings.json gespeichert.\n"
            "Kaderblick-Eintrag erfolgt nur nach erfolgreichem YouTube-Upload."
        )
        hint.setEnabled(False)
        hint.setWordWrap(True)
        layout.addWidget(hint)

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Save).setText("Speichern")
        buttons.button(QDialogButtonBox.Cancel).setText("Abbrechen")
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self._set_auth_mode(kb.auth_mode)
        self._auth_group.idToggled.connect(self._update_auth_visibility)
        self._update_auth_visibility()

    def _set_auth_mode(self, auth_mode: str) -> None:
        mode = str(auth_mode or "").strip().lower()
        is_bearer = mode == "bearer"
        self._auth_group.setExclusive(False)
        self._rb_jwt.setChecked(False)
        self._rb_bearer.setChecked(False)
        self._rb_bearer.setChecked(is_bearer)
        self._rb_jwt.setChecked(not is_bearer)
        self._auth_group.setExclusive(True)

    def _current_auth_mode(self) -> str:
        return "bearer" if self._rb_bearer.isChecked() else "jwt"

    def _update_auth_visibility(self) -> None:
        jwt_on = self._current_auth_mode() == "jwt"
        self._jwt_group.setVisible(jwt_on)
        self._bearer_group.setVisible(not jwt_on)
        self.adjustSize()

    def _save(self):
        kb = self.settings.kaderblick
        kb.base_url = self.base_url_edit.text().strip() or "https://api.kaderblick.de"
        kb.auth_mode = self._current_auth_mode()
        kb.jwt_token = self.jwt_token_edit.text().strip()
        kb.jwt_refresh_token = self.jwt_refresh_edit.text().strip()
        kb.bearer_token = self.bearer_token_edit.text().strip()
        self.settings.save(preserve_existing_secrets=False)
        self.accept()