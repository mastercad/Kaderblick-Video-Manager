"""YouTube-Upload: Authentifizierung, Upload und Playlist-Management."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from .settings import CLIENT_SECRET_FILE, TOKEN_FILE, UPLOAD_REGISTRY_FILE, AppSettings


# ═════════════════════════════════════════════════════════════════
#  Upload-Registry  (verhindert Doppel-Uploads, ermöglicht Resume)
# ═════════════════════════════════════════════════════════════════

class UploadRegistry:
    """Liest/schreibt data/youtube_uploads.json.

    Jeder Eintrag hat eines von zwei Zuständen:

      Abgeschlossen:
        { "state": "done", "video_id": "...", "title": "...",
          "uploaded_at": "ISO-8601" }

      Unterbrochen (Resume möglich):
        { "state": "pending", "resume_uri": "...", "title": "...",
          "started_at": "ISO-8601" }

    Altes Format (nur video_id, kein state) wird beim Lesen als
    "done" behandelt (Rückwärtskompatibilität).
    """

    def __init__(self, path: Path = UPLOAD_REGISTRY_FILE):
        self._path = path
        self._data: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                self._data = json.loads(self._path.read_text(encoding="utf-8"))
            except Exception:
                self._data = {}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(self._data, indent=2, ensure_ascii=False),
            encoding="utf-8")

    def _key(self, file_path: Path) -> str:
        return str(file_path.resolve())

    def already_uploaded(self, file_path: Path) -> Optional[str]:
        """Video-ID wenn abgeschlossen, sonst None."""
        entry = self._data.get(self._key(file_path))
        if not entry:
            return None
        # Altes Format (kein state-Feld) → galt als done
        if "state" not in entry:
            return entry.get("video_id")
        if entry["state"] == "done":
            return entry.get("video_id")
        return None

    def get_pending(self, file_path: Path) -> Optional[str]:
        """Resume-URI wenn unterbrochen, sonst None."""
        entry = self._data.get(self._key(file_path))
        if entry and entry.get("state") == "pending":
            return entry.get("resume_uri")
        return None

    def record_pending(self, file_path: Path, resume_uri: str,
                       title: str) -> None:
        """Merkt unterbrochenen Upload (nach erstem Chunk)."""
        self._data[self._key(file_path)] = {
            "state": "pending",
            "resume_uri": resume_uri,
            "title": title,
            "started_at": datetime.now().isoformat(timespec="seconds"),
        }
        self._save()

    def record_done(self, file_path: Path, video_id: str,
                    title: str) -> None:
        """Markiert Upload als vollständig abgeschlossen."""
        self._data[self._key(file_path)] = {
            "state": "done",
            "video_id": video_id,
            "title": title,
            "uploaded_at": datetime.now().isoformat(timespec="seconds"),
        }
        self._save()

    def clear(self, file_path: Path) -> None:
        """Entfernt Eintrag (z. B. nach abgelaufener Session)."""
        self._data.pop(self._key(file_path), None)
        self._save()


# Modul-weite Singleton-Instanz (wird beim ersten Import erzeugt)
_registry = UploadRegistry()

YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]

# YouTube-API-Abhängigkeiten (optional)
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    YOUTUBE_AVAILABLE = True
except ImportError:
    YOUTUBE_AVAILABLE = False


# ═════════════════════════════════════════════════════════════════
#  Authentifizierung
# ═════════════════════════════════════════════════════════════════

def get_youtube_service(log_callback=None):
    """Erstellt einen authentifizierten YouTube-API-Service."""
    def log(msg: str):
        if log_callback:
            log_callback(msg)

    if not YOUTUBE_AVAILABLE:
        log("FEHLER: google-api-python-client / google-auth-oauthlib "
            "nicht installiert. Bitte: pip install -r requirements.txt")
        return None

    if not CLIENT_SECRET_FILE.exists():
        log(f"FEHLER: {CLIENT_SECRET_FILE.name} nicht gefunden!")
        log("Siehe docs/youtube_credentials.md für die Einrichtung.")
        return None

    creds = None
    if TOKEN_FILE.exists():
        try:
            creds = Credentials.from_authorized_user_file(
                str(TOKEN_FILE), YOUTUBE_SCOPES)
        except Exception:
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                log(f"Token-Refresh fehlgeschlagen: {e}")
                creds = None

        if not creds:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(CLIENT_SECRET_FILE), YOUTUBE_SCOPES)
                creds = flow.run_local_server(port=0)
            except Exception as e:
                log(f"OAuth-Anmeldung fehlgeschlagen: {e}")
                return None

        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_FILE.write_text(creds.to_json())
        log("YouTube-Token gespeichert.")

    try:
        return build("youtube", "v3", credentials=creds)
    except Exception as e:
        log(f"YouTube-Service konnte nicht erstellt werden: {e}")
        return None


# ═════════════════════════════════════════════════════════════════
#  Playlist-Management
# ═════════════════════════════════════════════════════════════════

def find_or_create_playlist(service, title: str,
                            log_callback=None) -> Optional[str]:
    """Sucht eine existierende Playlist oder erstellt eine neue."""
    def log(msg: str):
        if log_callback:
            log_callback(msg)

    if not title:
        return None

    try:
        request = service.playlists().list(
            part="snippet", mine=True, maxResults=50)
        while request:
            response = request.execute()
            for item in response.get("items", []):
                if item["snippet"]["title"] == title:
                    log(f"Playlist gefunden: {title}")
                    return item["id"]
            request = service.playlists().list_next(request, response)

        body = {
            "snippet": {"title": title, "description": ""},
            "status": {"privacyStatus": "unlisted"},
        }
        resp = service.playlists().insert(
            part="snippet,status", body=body).execute()
        playlist_id = resp["id"]
        log(f"Playlist erstellt: {title} ({playlist_id})")
        return playlist_id
    except Exception as e:
        log(f"Playlist-Fehler: {e}")
        return None


# ═════════════════════════════════════════════════════════════════
#  Resume-Hilfsfunktion
# ═════════════════════════════════════════════════════════════════

def _query_resume_offset(resume_uri: str, http) -> Optional[int]:
    """Fragt YouTube, wie viele Bytes einer unterbrochenen Session
    bereits empfangen wurden.

    Returns:
        Byte-Offset ab dem fortgesetzt werden soll (0 = von vorne),
        oder None wenn die Session nicht mehr existiert (> 24 h alt
        oder sonstiger Fehler).
    """
    try:
        resp, _ = http.request(
            resume_uri,
            method="PUT",
            body=b"",
            headers={"Content-Range": "bytes */*", "Content-Length": "0"},
        )
        code = int(resp.status)
        if code == 308:          # Resume Incomplete – teilweise empfangen
            rng = resp.get("range", "")
            return int(rng.split("-")[1]) + 1 if rng else 0
        if code in (200, 201):   # Bereits vollständig (sollte nicht vorkommen)
            return None
        # 404 / 410 / sonstige → Session abgelaufen
        return None
    except Exception:
        return None


# ═════════════════════════════════════════════════════════════════
#  Upload
# ═════════════════════════════════════════════════════════════════

def upload_to_youtube(job, settings: AppSettings,
                      yt_service=None, log_callback=None,
                      cancel_flag=None,
                      progress_callback=None) -> bool:
    """Lädt die YouTube-Version (oder das Hauptvideo) auf YouTube hoch.

    Ablauf:
      1. Bereits abgeschlossen (Registry state=done)  → Überspringen.
      2. Unterbrochener Upload (Registry state=pending) → Resume versuchen.
         Abgelaufene Session (> 24 h)                 → neu starten.
      3. Neuer Upload → nach erstem Chunk Resume-URI in Registry sichern.
      Nach erfolgreichem Abschluss → state=done + video_id speichern.
    """
    yt = settings.youtube

    def log(msg: str):
        if log_callback:
            log_callback(msg)

    if not yt.upload_to_youtube:
        return False

    if not yt_service:
        log("Kein YouTube-Service verfügbar – Upload übersprungen.")
        return False

    mp4 = job.output_path
    if not mp4 or not mp4.exists():
        log("Keine Ausgabedatei zum Hochladen vorhanden.")
        return False

    yt_version = mp4.with_stem(mp4.stem + "_youtube")
    if yt_version.exists():
        upload_file = yt_version
        log(f"Verwende YouTube-optimierte Version: {yt_version.name}")
    else:
        upload_file = mp4
        log(f"Verwende Originaldatei (keine _youtube-Version gefunden): {mp4.name}")

    # ── 1. Bereits vollständig hochgeladen? ───────────────────
    existing_id = _registry.already_uploaded(upload_file)
    if existing_id:
        log(f"⏭ Bereits hochgeladen (https://youtu.be/{existing_id}) – übersprungen.")
        return True

    title = job.youtube_title or upload_file.stem
    source_name = job.source_path.name if job.source_path else upload_file.name
    description = getattr(job, "youtube_description", "") or (
        f"Hochgeladen mit Kaderblick - Video Manager\n"
        f"Quelldatei: {source_name}"
    )
    tags = getattr(job, "youtube_tags", []) or []
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": "17",  # Sport
            "tags": tags,
        },
        "status": {
            "privacyStatus": "unlisted",
            "selfDeclaredMadeForKids": False,
        },
    }

    # ── 2. Unterbrochenen Upload fortsetzen? ──────────────────
    upload_request = None
    pending_saved = False

    resume_uri = _registry.get_pending(upload_file)
    if resume_uri:
        log("↩ Unterbrochener Upload gefunden – prüfe Session …")
        offset = _query_resume_offset(resume_uri, yt_service._http)
        if offset is not None:
            log(f"  Session gültig – setze fort bei {offset / (1024 * 1024):.1f} MB")
            media = MediaFileUpload(
                str(upload_file),
                mimetype="video/mp4",
                resumable=True,
                chunksize=10 * 1024 * 1024,
            )
            upload_request = yt_service.videos().insert(
                part="snippet,status", body=body, media_body=media)
            upload_request._resumable_uri = resume_uri
            if offset > 0:
                upload_request.resumable_progress = offset
                media._fd.seek(offset)
            pending_saved = True   # URI bereits in Registry
        else:
            log("  Session abgelaufen (> 24 h) – starte Upload neu.")
            _registry.clear(upload_file)

    # ── 3. Neuer Upload ───────────────────────────────────────
    if upload_request is None:
        log(f"YouTube-Upload: {upload_file.name} → \"{title}\"")
        media = MediaFileUpload(
            str(upload_file),
            mimetype="video/mp4",
            resumable=True,
            chunksize=10 * 1024 * 1024,
        )
        upload_request = yt_service.videos().insert(
            part="snippet,status", body=body, media_body=media)

    # ── Upload-Schleife ───────────────────────────────────────
    import time
    _RETRY_STATUS = {500, 502, 503, 504}
    _MAX_RETRIES = 5

    try:
        response = None
        retries = 0
        while response is None:
            if cancel_flag and cancel_flag.is_set():
                log("Upload abgebrochen.")
                return False
            try:
                chunk_status, response = upload_request.next_chunk()
                retries = 0  # Erfolgreicher Chunk → Zähler zurücksetzen
            except Exception as e:
                err_str = str(e)
                # Wiederherstellbare Fehler: Timeout, 5xx, Verbindungsabbruch
                retriable = (
                    "timed out" in err_str.lower()
                    or "connection" in err_str.lower()
                    or "HttpError" in type(e).__name__
                    and any(f" {s}" in err_str for s in _RETRY_STATUS)
                )
                if retriable and retries < _MAX_RETRIES:
                    retries += 1
                    wait = 2 ** retries  # 2, 4, 8, 16, 32 s
                    log(f"  ⚠ Upload-Fehler (Versuch {retries}/{_MAX_RETRIES}): "
                        f"{e} – nächster Versuch in {wait}s …")
                    time.sleep(wait)
                    continue
                raise

            # Resume-URI nach erstem Chunk in Registry sichern
            if not pending_saved:
                uri = getattr(upload_request, "_resumable_uri", None)
                if uri:
                    _registry.record_pending(upload_file, uri, title)
                    pending_saved = True

            if chunk_status:
                pct = int(chunk_status.progress() * 100)
                if progress_callback:
                    progress_callback(pct)

        video_id = response["id"]
        log(f"✓ Hochgeladen: https://youtu.be/{video_id}")
        _registry.record_done(upload_file, video_id, title)

        # In Playlist einordnen
        if job.youtube_playlist:
            playlist_id = find_or_create_playlist(
                yt_service, job.youtube_playlist, log_callback)
            if playlist_id:
                try:
                    yt_service.playlistItems().insert(
                        part="snippet",
                        body={
                            "snippet": {
                                "playlistId": playlist_id,
                                "resourceId": {
                                    "kind": "youtube#video",
                                    "videoId": video_id,
                                },
                            }
                        },
                    ).execute()
                    log(f"✓ Zur Playlist hinzugefügt: {job.youtube_playlist}")
                except Exception as e:
                    log(f"Playlist-Zuordnung fehlgeschlagen: {e}")

        return True
    except Exception as e:
        log(f"Upload-Fehler: {e}")
        return False


def get_video_id_for_output(output_path: Path) -> Optional[str]:
    """Gibt die YouTube-Video-ID für eine Ausgabedatei zurück.

    Prüft zunächst die _youtube-Version, dann die Originaldatei.
    Gibt None zurück wenn noch kein Upload registriert ist.
    """
    yt_version = output_path.with_stem(output_path.stem + "_youtube")
    vid = _registry.already_uploaded(yt_version)
    if vid:
        return vid
    return _registry.already_uploaded(output_path)
