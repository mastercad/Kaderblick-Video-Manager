# YouTube API – Credentials einrichten

Anleitung zur Einrichtung der Google/YouTube API-Credentials für den automatischen Upload aus der GUI (`main.py`).

[← Zurück zur Übersicht](../README.md)

---

## Übersicht

Der Upload verwendet die **YouTube Data API v3** mit **OAuth 2.0 (Desktop-App)**. Dafür werden zwei Dateien benötigt:

| Datei | Beschreibung | Erstellt von |
|-------|-------------|--------------|
| `config/client_secret.json` | API-Zugangsdaten (einmalig aus Google Cloud) | Du (manuell) |
| `data/youtube_token.json` | OAuth-Token (wird automatisch erzeugt) | Das Programm |

Die Dateien liegen in den Unterverzeichnissen `config/` und `data/`:

```
video-manager/
├── main.py                     ← GUI-Einstiegspunkt
├── src/                        ← Anwendungspaket
├── config/
│   └── client_secret.json      ← manuell hinterlegen
├── data/
│   ├── youtube_token.json      ← wird automatisch erstellt
│   └── settings.json
└── …
```

> **Wichtig:** `config/client_secret.json` und `data/youtube_token.json` enthalten sensible Zugangsdaten und dürfen **nicht** in Git eingecheckt werden. Beide sind in `.gitignore` eingetragen.

---

## Schritt 1: Google Cloud Projekt erstellen

1. Öffne die [Google Cloud Console](https://console.cloud.google.com/)
2. Klicke oben links auf das Projekt-Dropdown → **Neues Projekt**
3. Name: z. B. `Fussballverein Video Upload`
4. **Erstellen** klicken

## Schritt 2: YouTube Data API aktivieren

1. Im Projekt: **APIs & Services → Bibliothek**
2. Nach `YouTube Data API v3` suchen
3. **Aktivieren** klicken

## Schritt 3: OAuth-Zustimmungsbildschirm konfigurieren

1. **APIs & Services → OAuth-Zustimmungsbildschirm**
2. Nutzertyp: **Extern** (oder **Intern** bei Google Workspace)
3. App-Name: z. B. `Video Manager`
4. Support-E-Mail: deine eigene
5. Unter **Scopes** hinzufügen:
   - `https://www.googleapis.com/auth/youtube.upload`
   - `https://www.googleapis.com/auth/youtube` (für Playlist-Verwaltung)
6. Unter **Testnutzer** dein Google-Konto hinzufügen (solange die App nicht verifiziert ist)

## Schritt 4: OAuth-Client-ID erstellen

1. **APIs & Services → Anmeldedaten → + Anmeldedaten erstellen → OAuth-Client-ID**
2. Anwendungstyp: **Desktop-App**
3. Name: z. B. `Video Manager Desktop`
4. **Erstellen** klicken
5. Im Dialog auf **JSON herunterladen** klicken
6. Die heruntergeladene Datei umbenennen in **`client_secret.json`**
7. Die Datei in das `video-manager/config/`-Verzeichnis verschieben

## Schritt 5: Erster Upload (Token-Erstellung)

1. In der GUI: **Einstellungen → YouTube** → „Videos auf YouTube hochladen" aktivieren
2. Mindestens einen Job in der Jobliste anlegen und YouTube-Titel setzen (Doppelklick auf Job)
3. Optional: **Playlist-Name** eingeben – die App sucht automatisch nach einer existierenden Playlist mit diesem Namen und legt sie bei Bedarf als *nicht gelistet* neu an
4. **▶ Starten** klicken
5. Beim ersten Upload öffnet sich ein **Browser-Fenster** zur Google-Anmeldung
6. Mit dem Google-Konto anmelden, das als Testnutzer hinterlegt ist
7. Zugriff gewähren
8. Das Token wird automatisch als `data/youtube_token.json` gespeichert
9. Ab jetzt läuft der Upload **ohne erneute Anmeldung** (bis das Token abläuft)

---

## Fehlerbehebung

| Problem | Lösung |
|---------|--------|
| `client_secret.json nicht gefunden` | Datei muss unter `config/client_secret.json` liegen |
| `Token abgelaufen / ungültig` | `data/youtube_token.json` löschen und erneut anmelden |
| `Access blocked: App not verified` | Dein Google-Konto muss als Testnutzer eingetragen sein (Schritt 3.6) |
| `Quota exceeded` | YouTube API hat ein tägliches Limit von 10.000 Einheiten. Ein Upload kostet 1.600 Einheiten → max. ~6 Uploads/Tag mit Standard-Quota |
| `403 Forbidden` | Prüfe, ob die YouTube Data API v3 im Projekt aktiviert ist (Schritt 2) |

---

## Token erneuern

Das gespeicherte Token wird automatisch erneuert (Refresh-Token). Falls es trotzdem abläuft:

```bash
# Token löschen und beim nächsten Upload neu anmelden
rm data/youtube_token.json
```

---

## Sicherheitshinweise

- `client_secret.json` **niemals** teilen oder committen
- `youtube_token.json` berechtigt zum Upload auf den verknüpften YouTube-Kanal
- Bei Verdacht auf Missbrauch: Token in der [Google Cloud Console](https://console.cloud.google.com/) unter **APIs & Services → Anmeldedaten** widerrufen
