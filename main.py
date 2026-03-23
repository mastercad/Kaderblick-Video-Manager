#!/usr/bin/env python3
"""Video Manager – Haupteinstiegspunkt.

Startet die PySide6-GUI für die Video-Verarbeitung mit:
  - Profil-System (KI Auswertung / YouTube / Benutzerdefiniert)
  - NVIDIA NVENC Hardware-Encoding (falls verfügbar)
  - Halbzeiten zusammenführen mit Titelkarten
  - YouTube-Upload mit Playlist-Unterstützung

Aufruf:
    python main.py [OPTIONEN]

Optionen:
    --workflow PFAD         Workflow-JSON laden und sofort ausführen
    --add DATEI [DATEI …]  Dateien beim Start in die Jobliste laden
    --restore-last-workflow       Wiederherstellung des letzten Workflows erzwingen
    --no-restore-last-workflow    Wiederherstellung unterdrücken
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
        description="Video Manager – Video-Verarbeitung für Fußballverein-Kameras",
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
        "--restore-last-workflow",
        action="store_true",
        default=None,
        help="Wiederherstellung des letzten Workflows erzwingen "
             "(unabhängig von der gespeicherten Einstellung).",
    )
    session_grp.add_argument(
        "--no-restore-last-workflow",
        action="store_true",
        default=None,
        help="Wiederherstellung des letzten Workflows unterdrücken.",
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
