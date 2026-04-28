# Kaderblick — Video Manager

Der Kaderblick — Video Manager hilft Ihnen dabei, Fußball-Videos an einem Ort zu sammeln, zu verarbeiten und bei Bedarf auf YouTube und Kaderblick weiterzugeben.

<p align="center">
  <img src="assets/application_main.png" alt="Hauptfenster des Kaderblick — Video Managers mit Workflow-Liste und Protokoll" width="1200">
</p>

Diese Anleitung richtet sich an Menschen ohne Technik-Erfahrung. Sie erklärt nicht die interne Technik der App, sondern den einfachen Arbeitsablauf im Alltag.

Mit der App können Sie:

- einzelne Videos auswählen und verarbeiten
- einen ganzen Ordner mit Videos verarbeiten
- Aufnahmen direkt von einer Raspberry-Pi-Kamera laden
- Videos umwandeln
- mehrere zusammengehörige Videos zusammenführen
- eine Titelkarte vor ein Video setzen
- eine YouTube-Version erzeugen
- Videos auf YouTube hochladen
- den YouTube-Link bei Kaderblick eintragen

Weiterführende Anleitung für YouTube: [YouTube API – Credentials einrichten](docs/youtube_credentials.md)

---

## Portable Releases

Die App kann jetzt als portable Desktop-Version für Linux, Windows und macOS gebaut werden.

### Lokaler Build

Für einen lokalen Portable-Build wird PyInstaller verwendet:

```bash
.venv/bin/python -m pip install pyinstaller
.venv/bin/python scripts/build_portable.py
```

Ergebnis:

- entpackte App im Ordner `dist/kaderblick-video-manager`
- Release-Archiv im Ordner `dist-artifacts/`

Die Build-Ausgabe enthält zusätzlich:

- `config/`
- `data/`
- `workflows/`
- eingebettete `ffmpeg`-/`ffprobe`-Binaries, sofern sie beim Build im `PATH` gefunden werden

### GitHub Releases

Für jeden Push auf einen Branch und für jeden Pull Request gegen `main` läuft zusätzlich der Workflow `.github/workflows/qa.yml` auf Linux, Windows und macOS.

Für Tags wie `v1.2.3` baut der Workflow `.github/workflows/release-portable.yml` automatisch portable Archive für:

- Linux
- Windows
- macOS

Vor dem eigentlichen Release-Build wird dieselbe QA als Pflicht-Gate ausgeführt. Schlägt die QA fehl, startet kein Release-Build.

Die erzeugten ZIP-Dateien werden direkt an das GitHub Release angehängt.

---

## Zwei Wege durch diese Anleitung

Wenn Sie einfach nur loslegen möchten, lesen Sie zuerst nur diesen Teil:

- `Einsteiger-Schnellstart`

Wenn Sie die App danach genauer verstehen oder einen speziellen Ablauf nachlesen möchten, lesen Sie danach diesen Teil:

- `Ausführliche Anleitung`

---

## Einsteiger-Schnellstart

Dieser Teil ist für alle gedacht, die die App zum ersten Mal benutzen.

Sie müssen dafür nicht alles verstehen. Es reicht, wenn Sie die folgenden Schritte nacheinander machen.

### 1. App öffnen

Starten Sie die App.

Im Hauptfenster sehen Sie oben die grüne Leiste mit den Aktionen, in der Mitte die Workflow-Liste und unten das Protokoll.

### 2. Neuen Workflow anlegen

Klicken Sie oben auf `+ Neuer Workflow`.

Danach öffnet sich zuerst der grafische Workflow-Editor.

Für Einsteiger ist der einfachste Weg:

1. `+ Neuer Workflow` klicken
2. im geöffneten Fenster auf `Übernehmen` klicken
3. den neuen Workflow in der Liste markieren
4. auf `Bearbeiten` klicken

### 3. Quelle auswählen

Im Assistenten auf der Seite `1 Quelle` wählen Sie aus, woher das Material kommt:

- `Dateien auswählen`, wenn Sie einzelne Dateien haben
- `Ordner scannen`, wenn viele Dateien in einem Ordner liegen
- `Pi-Kamera`, wenn Aufnahmen erst von einer Kamera geholt werden sollen

### 4. Verarbeitung festlegen

Auf der Seite `2 Verarbeitung` legen Sie fest, wie das Video bearbeitet werden soll.

Wenn Sie unsicher sind, lassen Sie die Standardwerte stehen und ändern nur das, was Sie wirklich brauchen.

### 5. Optional Titelkarte und Upload einstellen

Auf der Seite `3 Titelkarte` können Sie eine Titelkarte aktivieren.

Auf der Seite `4 Upload` können Sie festlegen, ob:

- eine YouTube-Version erzeugt werden soll
- ein Upload zu YouTube erfolgen soll
- das Ergebnis bei Kaderblick eingetragen werden soll

### 6. Workflow speichern

Klicken Sie im Hauptfenster auf `Speichern`.

Damit haben Sie eine Workflow-Datei, die Sie später wieder laden oder weitergeben können.

### 7. Workflow starten

Markieren Sie den Workflow in der Liste und klicken Sie auf `Starten`.

Den Fortschritt sehen Sie:

- in der Spalte `Status`
- in der Spalte `Job`
- in der Spalte `Dauer`
- im Protokoll unten

### 8. Wenn Sie anhalten möchten

Klicken Sie auf `Abbrechen`.

Wenn Sie vorher bestimmte Workflows markiert haben, werden nur diese abgebrochen.

### 9. Wenn die App nach Fortsetzen fragt

Dann bedeutet:

- `Fortsetzen`: dort weitermachen, wo der letzte Lauf aufgehört hat
- `Neu starten`: wieder von vorne beginnen

---

## Ausführliche Anleitung

Ab hier wird die App genauer erklärt.

## So sieht die App aus

Das Hauptfenster besteht aus drei Bereichen:

- **oben:** die grüne Leiste mit den Aktionen `+ Neuer Workflow`, `Bearbeiten`, `Kopieren`, `Workflow`, `Entfernen`, `Starten`, `Abbrechen`, `Laden`, `Speichern` und `Rechner herunterfahren`
- **Mitte:** die Workflow-Liste mit den Spalten `#`, `Name`, `Quelle`, `Pipeline`, `Status`, `Job` und `Dauer`
- **unten:** das Protokoll mit allen laufenden Meldungen der aktiven Jobs

Jede Zeile in der Mitte ist ein Workflow.

Ein Workflow ist einfach ein kompletter Arbeitsauftrag, zum Beispiel:

- „Zwei Halbzeiten von der Kamera holen, zusammenführen und hochladen“
- „Drei vorhandene MP4-Dateien nur konvertieren“
- „Einen Ordner mit Aufnahmen prüfen und für YouTube vorbereiten“

---

## Schritt 1: Einen Workflow anlegen

Klicken Sie oben auf `+ Neuer Workflow`.

Danach öffnet die App zuerst den grafischen Workflow-Editor.

Das ist normal.

Für Einsteiger ist der einfachste Weg:

1. `+ Neuer Workflow` klicken
2. den geöffneten Workflow-Editor einfach mit `Übernehmen` bestätigen
3. danach den neuen Workflow in der Liste markieren
4. auf `Bearbeiten` klicken

Damit arbeiten Sie anschließend im einfachen Assistenten weiter.

Wenn Sie schon einen ähnlichen Workflow haben, ist es oft noch einfacher, diesen zu markieren und `Kopieren` zu klicken. Danach ändern Sie nur noch die Stellen, die anders sein sollen.

---

## Schritt 2: Einen Workflow bearbeiten

Markieren Sie einen Workflow in der Liste und klicken Sie oben auf `Bearbeiten`.

Dann öffnet sich ein Assistent mit 4 Seiten:

- `1 Quelle`
- `2 Verarbeitung`
- `3 Titelkarte`
- `4 Upload`

Gehen Sie diese Seiten von links nach rechts durch.

---

## Schritt 2a: Die Quelle festlegen

Auf der Seite `1 Quelle` legen Sie fest, woher das Material kommt.

Die App kennt genau drei Möglichkeiten:

- `Dateien auswählen`
- `Ordner scannen`
- `Pi-Kamera`

### Wenn Sie schon fertige Dateien haben

Wählen Sie `Dateien auswählen`.

Dann können Sie mit `＋ Dateien …` Ihre Videos in die Liste einfügen.

In dieser Liste können Sie pro Datei direkt pflegen:

- Ausgabename
- YouTube-Titel
- Playlist
- Kaderblick-Start in Sekunden

Wenn Sie viele Dateien auf einmal mit Spieldaten füllen möchten, nutzen Sie `🎬 Alle belegen …`.

### Wenn Ihre Dateien in einem Ordner liegen

Wählen Sie `Ordner scannen`.

Dann tragen Sie ein:

- den `Quellordner`
- das `Datei-Muster`, zum Beispiel `*.mp4`
- optional einen `Zielordner`
- optional ein `Ausgabe-Präfix`

Das ist sinnvoll, wenn Sie regelmäßig denselben Ordner verarbeiten.

### Wenn die Videos erst von einer Pi-Kamera geholt werden sollen

Wählen Sie `Pi-Kamera`.

Dann stellen Sie ein:

- welches Gerät verwendet werden soll
- wohin die Dateien gespeichert werden sollen
- ob die Aufnahmen nach dem Download von der Kamera gelöscht werden sollen

Mit `📋 Dateien von Kamera laden` liest die App die vorhandenen Aufnahmen von der gewählten Kamera ein.

Danach erscheint darunter eine Liste mit den gefundenen Dateien.

In dieser Liste können Sie dann wieder Titel, Playlist und weitere Angaben pflegen.

> Screenshot-Platzhalter: `assets/readme_job_editor_1_quelle.png`
> Zu sehen sein soll: Seite `1 Quelle` mit den drei Quellen-Karten und darunter der Eingabebereich.

---

## Schritt 2b: Festlegen, was mit dem Video gemacht werden soll

Auf der Seite `2 Verarbeitung` legen Sie fest, wie das Video bearbeitet werden soll.

Die wichtigste Schaltfläche dort ist `Dateien konvertieren`.

Wenn diese Option aktiv ist, wandelt die App das Material in das gewünschte Zielformat um.

Sie können dort unter anderem einstellen:

- `Encoder`
- `Preset`
- `CRF`
- `Framerate`
- `Auflösung`
- `Format`
- `Vorhandene Ausgabedateien überschreiben`

Darunter gibt es den Bereich `Audio`.

Dort können Sie zum Beispiel:

- eine separate Audio-Spur zusammenführen
- die Lautstärke anpassen
- `Audio-Sync aktivieren`

Wenn Sie sich unsicher sind, ändern Sie nur das Nötigste und arbeiten mit den Standardwerten weiter.

> Screenshot-Platzhalter: `assets/readme_job_editor_2_verarbeitung.png`
> Zu sehen sein soll: Seite `2 Verarbeitung` mit den Video-Einstellungen oben und dem Audio-Bereich unten.

---

## Schritt 2c: Eine Titelkarte verwenden

Auf der Seite `3 Titelkarte` können Sie eine Titelkarte vor das Video setzen.

Das ist sinnvoll, wenn am Anfang eines Videos zum Beispiel Mannschaften und Datum eingeblendet werden sollen.

Dort können Sie eintragen:

- Logo
- Heim
- Gast
- Datum
- Dauer
- Hintergrundfarbe
- Textfarbe

Wenn Sie keine Titelkarte brauchen, lassen Sie diese Funktion einfach ausgeschaltet.

> Screenshot-Platzhalter: `assets/readme_job_editor_3_titelkarte.png`
> Zu sehen sein soll: Seite `3 Titelkarte` mit aktivierter Titelkarte und sichtbaren Feldern für Logo, Teams, Datum, Dauer und Farben.

---

## Schritt 2d: YouTube und Kaderblick festlegen

Auf der Seite `4 Upload` bestimmen Sie, was nach der Verarbeitung passieren soll.

Sie können dort:

- `Auf YouTube hochladen`
- `YouTube-optimierte Version erstellen`
- einen `Standard-Titel` setzen
- eine `Playlist` setzen
- `Video nach YouTube-Upload auf Kaderblick eintragen`

Wenn Sie Kaderblick nutzen möchten, können Sie dort außerdem eine `Spiel-ID` eintragen.

Wichtig:

- Kaderblick ist nur sinnvoll, wenn das Video vorher auf YouTube hochgeladen wurde.
- Für YouTube braucht die App die Dateien `config/client_secret.json` und `data/youtube_token.json`.

> Screenshot-Platzhalter: `assets/readme_job_editor_4_upload.png`
> Zu sehen sein soll: Seite `4 Upload` mit dem Bereich `YouTube` und dem Bereich `Kaderblick`.

---

## Schritt 3: Den Workflow speichern

Wenn Ihr Workflow fertig eingerichtet ist, klicken Sie im Hauptfenster auf `Speichern`.

Die App speichert dann eine Workflow-Datei als JSON-Datei.

Diese Datei enthält:

- Ihren Workflow-Aufbau
- Ihre Quelle
- Ihre Einstellungen

Diese Datei enthält nicht:

- den aktuellen Fortschritt
- den aktuellen Bearbeitungsstand
- die aktuelle Laufzeit

Das ist wichtig, wenn Sie einen Workflow an andere Menschen weitergeben möchten.

Der Empfänger bekommt nur den Workflow selbst, nicht Ihren persönlichen Zwischenstand.

---

## Schritt 4: Den Workflow starten

Markieren Sie einen oder mehrere Workflows in der Liste.

Klicken Sie danach auf `Starten`.

Wenn Sie nichts markieren, startet die App alle wartenden Workflows.

Während der Verarbeitung sehen Sie in der Liste:

- in `Status`, was gerade passiert
- in `Job`, wie weit der Workflow insgesamt ist
- in `Dauer`, wie lange dieser Workflow schon gearbeitet hat

Im Protokoll unten sehen Sie zusätzlich die einzelnen Meldungen im Detail.

---

## Schritt 5: Einen laufenden Workflow abbrechen

Wenn Sie eine Verarbeitung anhalten möchten, klicken Sie auf `Abbrechen`.

Die App fragt dann zur Sicherheit nach.

Wenn Sie vorher bestimmte Zeilen markiert haben, werden nur diese Workflows abgebrochen.

Wenn nichts markiert ist, werden alle laufenden Workflows abgebrochen.

---

## Wenn die App fragt: Fortsetzen oder neu starten?

Manchmal meldet die App beim Start, dass es gespeicherte Fortschrittsdaten gibt.

Dann bietet sie an:

- `Fortsetzen`
- `Neu starten`
- `Abbrechen`

`Fortsetzen` bedeutet:

- die App macht dort weiter, wo der letzte Lauf aufgehört hat

`Neu starten` bedeutet:

- der gespeicherte Fortschritt wird verworfen
- der Workflow beginnt wieder von vorne

---

## Was der grafische Workflow-Editor macht

Wenn Sie im Hauptfenster einen Workflow markieren und `Workflow` in der Werkzeugleiste klicken, öffnet sich der grafische Workflow-Editor.

Dieser Bereich ist für den Ablauf des Workflows zuständig.

Dort können Sie festlegen, welche Schritte nacheinander ausgeführt werden.

Die App kennt dort unter anderem diese Bausteine:

- `Dateien`
- `Ordner-Scan`
- `Pi-Download`
- `Konvertierung`
- `Merge`
- `Titelkarte`
- `Quick-Check`
- `Deep-Scan`
- `Cleanup`
- `Reparatur`
- `YT-Version`
- `Stop / Log`
- `YouTube Upload`
- `Kaderblick`

Für Einsteiger gilt:

- Sie müssen den grafischen Editor nicht sofort im Detail verstehen.
- Wenn ein Workflow für Sie schon richtig aufgebaut ist, reicht oft `Bearbeiten` im Assistenten.
- Der grafische Editor ist vor allem dann wichtig, wenn der Ablauf selbst geändert werden soll.

> Screenshot-Platzhalter: `assets/readme_workflow_editor.png`
> Zu sehen sein soll: links die Baustein-Palette, in der Mitte der Graph und rechts die Einstellungen des ausgewählten Knotens.

---

## Einstellungen, die Sie kennen sollten

Im Menü `Einstellungen` finden Sie diese Bereiche:

- `Video …`
- `Audio …`
- `YouTube …`
- `Kaderblick …`
- `Kameras …`
- `Allgemein …`

### Video …

Hier legen Sie die allgemeinen Standardwerte für die Video-Verarbeitung fest.

Das ist der richtige Ort für Dinge wie:

- Profil
- Encoder
- GPU-Status
- CRF
- Auflösung
- Format
- Audio-Video-Sync

> Screenshot-Platzhalter: `assets/readme_settings_video.png`
> Zu sehen sein soll: Dialog `Video-Einstellungen` mit Profil, Encoder, GPU-Status und den wichtigsten Video-Feldern.

### Audio …

Hier legen Sie die allgemeinen Standardwerte für den Ton fest, zum Beispiel:

- ob Audio eingebunden wird
- ob Audio verstärkt wird
- wie stark verstärkt wird
- welche Audio-Bitrate genutzt wird

### YouTube …

Hier legen Sie allgemeine YouTube-Standardwerte fest, zum Beispiel:

- ob eine YouTube-Version erzeugt wird
- CRF
- maximale Bitrate
- Buffer-Größe
- Audio-Bitrate
- ob Uploads aktiviert sind

### Kaderblick …

Hier wird die Verbindung zu Kaderblick eingerichtet.

Sichtbar sind dort:

- `Base-URL`
- `Auth-Modus`
- `JWT-Token`
- `Refresh-Token`
- `Bearer-Token`

### Kameras …

Hier verwalten Sie die Raspberry-Pi-Geräte.

Pro Gerät gibt es diese Felder:

- Name
- IP-Adresse
- Port
- Benutzername
- Passwort
- SSH-Key

Außerdem gibt es dort den Quellpfad auf dem Pi.

> Screenshot-Platzhalter: `assets/readme_settings_kameras.png`
> Zu sehen sein soll: Dialog `Kamera-Einstellungen` mit Geräteliste und den Schaltflächen zum Hinzufügen, Bearbeiten und Entfernen.

### Allgemein …

Hier stellen Sie das allgemeine Verhalten der App ein.

Besonders wichtig sind:

- `Letzten Workflow-Stand beim Start wiederherstellen`
- der `Basisordner` für die globale Ausgabe
- die globalen Spieldaten

> Screenshot-Platzhalter: `assets/readme_settings_allgemein.png`
> Zu sehen sein soll: Dialog `Allgemeine Einstellungen` mit Wiederherstellung des letzten Workflow-Stands, Basisordner und globalen Spieldaten.

---

## Wenn Sie mit Pi-Kameras arbeiten

Dann ist dieser Ablauf im Alltag meistens der einfachste:

1. in `Einstellungen` → `Kameras …` die Geräte sauber eintragen
2. einen Workflow mit Quelle `Pi-Kamera` anlegen
3. das richtige Gerät auswählen
4. `📋 Dateien von Kamera laden` klicken
5. die gewünschten Einträge in der Liste prüfen
6. Workflow speichern
7. Workflow starten

> Screenshot-Platzhalter: `assets/readme_source_pi_dateiliste.png`
> Zu sehen sein soll: Seite `1 Quelle` im Modus `Pi-Kamera` mit eingeblendeter Dateiliste nach dem Laden der Aufnahmen.

---

## Welche Dateien und Ordner für Sie wichtig sind

Für normale Nutzer sind vor allem diese Orte wichtig:

```text
config/settings.json
config/client_secret.json
data/last_workflow.json
data/youtube_token.json
data/integration_state.json
workflows/
```

### `workflows/`

Hier können Sie Ihre gespeicherten Workflow-Dateien ablegen.

### `config/settings.json`

Hier speichert die App Ihre allgemeinen Einstellungen.

### `data/last_workflow.json`

Hier speichert die App automatisch den letzten bekannten Arbeitsstand, damit ein Workflow später fortgesetzt werden kann.

### `config/client_secret.json`

Diese Datei wird für YouTube gebraucht.

### `data/youtube_token.json`

Hier speichert die App das YouTube-Anmeldetoken.

---

## Starten der App

Die App wird mit diesem Befehl gestartet:

```bash
python main.py
```

Für normale Nutzer ist das der einzige Startbefehl, den Sie brauchen.

---

## Voraussetzungen

Im Projekt ist aktuell vorgesehen:

- Python 3.11 oder neuer
- `ffmpeg`
- `ffprobe`

Optional, aber für bestimmte Funktionen nützlich:

- `rsync`
- `sshpass`

Die Python-Abhängigkeiten stehen in `requirements.txt`.

---

## Welche Screenshots für die Doku noch sinnvoll sind

Wenn die README später noch mit echten Bildern ergänzt werden soll, sind diese Bilder am wichtigsten:

1. `assets/readme_job_editor_1_quelle.png`
2. `assets/readme_job_editor_2_verarbeitung.png`
3. `assets/readme_job_editor_3_titelkarte.png`
4. `assets/readme_job_editor_4_upload.png`
5. `assets/readme_workflow_editor.png`
6. `assets/readme_settings_video.png`
7. `assets/readme_settings_kameras.png`
8. `assets/readme_settings_allgemein.png`
9. `assets/readme_source_pi_dateiliste.png`

Diese Dateinamen sind in dieser README bereits passend vorbereitet.

---

## Schnellüberblick

Wenn Sie einfach nur loslegen möchten, genügt meistens dieser Ablauf:

1. `+ Neuer Workflow`
2. `Übernehmen`
3. Workflow markieren
4. `Bearbeiten`
5. Quelle auswählen
6. Verarbeitung und Upload einstellen
7. `Speichern`
8. `Starten`

So kommen Sie ohne Technik-Wissen am schnellsten ans Ziel.