"""pytest-Konfiguration für das video-manager Projekt."""

import sys
from pathlib import Path

# Projekt-Root zum Import-Pfad hinzufügen, damit `from src.xxx import …` funktioniert
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
