#!/usr/bin/env python3
"""MJPEG Converter – Haupteinstiegspunkt.

Startet die PySide6-GUI für die MJPEG-Konvertierung mit:
  - Profil-System (KI Auswertung / YouTube / Benutzerdefiniert)
  - NVIDIA NVENC Hardware-Encoding (falls verfügbar)
  - Halbzeiten zusammenführen mit Titelkarten
  - YouTube-Upload mit Playlist-Unterstützung

Aufruf:
    python main.py [OPTIONEN]

Optionen:
    --cameras-config PFAD   Kamera-YAML importieren statt gespeicherter Daten
    --workflow PFAD         Workflow-JSON laden und sofort ausführen
    --add DATEI [DATEI …]  Dateien beim Start in die Jobliste laden
    --restore-session       Session-Wiederherstellung erzwingen
    --no-restore-session    Session-Wiederherstellung unterdrücken
"""

import argparse
import sys

from pathlib import Path

from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication

from src.app import ConverterApp

_ICON = Path(__file__).resolve().parent / "assets" / "icon.svg"


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="MJPEG Converter – Video-Verarbeitung für Fußballverein-Kameras",
    )
    p.add_argument(
        "--cameras-config",
        metavar="PFAD",
        help="Pfad zu einer cameras.yaml, deren Einstellungen beim Start "
             "importiert werden (überschreibt die gespeicherten Kameradaten).",
    )
    p.add_argument(
        "--workflow",
        metavar="PFAD",
        help="Pfad zu einer Workflow-JSON-Datei. "
             "Der Workflow wird nach dem Start automatisch ausgeführt.",
    )
    p.add_argument(
        "--add",
        nargs="+",
        metavar="DATEI",
        help="Eine oder mehrere Video-Dateien, die beim Start "
             "in die Jobliste aufgenommen werden.",
    )
    session_grp = p.add_mutually_exclusive_group()
    session_grp.add_argument(
        "--restore-session",
        action="store_true",
        default=None,
        help="Session-Wiederherstellung erzwingen "
             "(unabhängig von der gespeicherten Einstellung).",
    )
    session_grp.add_argument(
        "--no-restore-session",
        action="store_true",
        default=None,
        help="Session-Wiederherstellung unterdrücken.",
    )
    return p


def main():
    parser = _build_parser()
    args, qt_args = parser.parse_known_args()

    # Qt bekommt nur die nicht von argparse konsumierten Argumente
    app = QApplication([sys.argv[0]] + qt_args)
    if _ICON.exists():
        app.setWindowIcon(QIcon(str(_ICON)))
    window = ConverterApp(cli_args=args)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
