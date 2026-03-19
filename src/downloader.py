"""Download-Logik: Videos von Raspberry Pi Kameras via SFTP/rsync herunterladen."""

import os
import shutil
import subprocess
import time
import threading
from pathlib import Path
from typing import Callable, Optional

import paramiko

from .settings import DeviceSettings, CameraSettings

# Abwärtskompatibilität
DeviceConfig = DeviceSettings
DownloadConfig = CameraSettings


class _CancelledError(Exception):
    """Raised inside SFTP callback to abort a running transfer."""


# Größe eines einzelnen SFTP-Read-Requests (64 KB, von allen SFTP-Servern unterstützt)
_REQUEST_SIZE = 65536
# Anzahl gleichzeitig gepipelineter Read-Requests pro Batch
# 256 × 64 KB = 16 MB pro Batch – guter Kompromiss aus Durchsatz und Cancel-Responsivität
_PIPELINE_DEPTH = 256


# ═════════════════════════════════════════════════════════════════
#  YAML-Import (Migration von cameras.yaml)
# ═════════════════════════════════════════════════════════════════

def import_from_yaml(path: str) -> CameraSettings:
    """
    Importiert Kamera-Konfiguration aus einer legacy cameras.yaml und gibt
    ein CameraSettings-Objekt zurück, das in AppSettings gespeichert werden kann.
    """
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    cfg = CameraSettings(
        destination=data.get("destination", ""),
        source=data.get("source", "/home/kaderblick/camera_api/recordings"),
    )
    for d in data.get("devices", []):
        cfg.devices.append(DeviceSettings(
            name=d.get("name", d.get("ip", "unknown")),
            ip=d["ip"],
            username=d.get("username", d.get("user", "")),
            password=d.get("password", ""),
            ssh_key=d.get("ssh_key", ""),
            port=int(d.get("port", 22)),
        ))
    return cfg


# ═════════════════════════════════════════════════════════════════
#  SSH / SFTP Verbindung
# ═════════════════════════════════════════════════════════════════

def _connect(device: DeviceSettings):
    """Baut SSH-Verbindung auf und gibt (SSHClient, SFTPClient) zurück."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    kwargs: dict = {
        "hostname": device.ip,
        "port": device.port,
        "username": device.username,
        "timeout": 30,
    }

    key_path = os.path.expanduser(device.ssh_key) if device.ssh_key else ""
    if key_path and os.path.exists(key_path):
        kwargs["key_filename"] = key_path
        kwargs["look_for_keys"] = False
        kwargs["allow_agent"] = False
    else:
        kwargs["look_for_keys"] = True
        kwargs["allow_agent"] = True

    if device.password:
        kwargs["password"] = device.password

    client.connect(**kwargs)

    # Keep-Alive alle 30 s – verhindert Timeout bei langen Transfers
    transport = client.get_transport()
    if transport:
        transport.set_keepalive(30)
        # Größeres SSH-Fenster für besseren Durchsatz bei großen Dateien
        # Default ist nur 2 MB – viel zu klein für 1-Gbit-Verbindungen
        transport.default_window_size = 2 ** 30          # 1 GB (max. erlaubt: 2^31-1)
        transport.default_max_packet_size = 2 ** 15      # 32 KB (SSH-Standard)

    sftp = client.open_sftp()
    # SFTP-Kanal-Fenster ebenfalls vergrößern
    chan = sftp.get_channel()
    if chan:
        chan.in_window_size = 2 ** 30
        chan.out_window_size = 2 ** 30
    return client, sftp


# ═════════════════════════════════════════════════════════════════
#  Hilfsfunktionen
# ═════════════════════════════════════════════════════════════════

def _fmt_size(n: int) -> str:
    """Formatiert Bytes als menschenlesbare Größe."""
    if n >= 1024**3:
        return f"{n / 1024**3:.2f} GB"
    elif n >= 1024**2:
        return f"{n / 1024**2:.1f} MB"
    elif n >= 1024:
        return f"{n / 1024:.0f} KB"
    return f"{n} B"


def _remote_size(sftp, remote_path: str) -> Optional[int]:
    try:
        return sftp.stat(remote_path).st_size
    except Exception:
        return None


def _ssh_exec(device: DeviceSettings, cmd: str, timeout: int = 20) -> bool:
    """Führt einen SSH-Befehl aus; gibt True bei rc=0 zurück."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        key_path = os.path.expanduser(device.ssh_key) if device.ssh_key else ""
        kwargs: dict = {
            "hostname": device.ip,
            "port": device.port,
            "username": device.username,
            "timeout": timeout,
        }
        if key_path and os.path.exists(key_path):
            kwargs["key_filename"] = key_path
            kwargs["look_for_keys"] = False
            kwargs["allow_agent"] = False
        else:
            kwargs["look_for_keys"] = True
            kwargs["allow_agent"] = True
        if device.password:
            kwargs["password"] = device.password
        client.connect(**kwargs)
        _, _, stderr = client.exec_command(cmd, timeout=timeout)
        rc = stderr.channel.recv_exit_status()
        return rc == 0
    except Exception:
        return False
    finally:
        try:
            client.close()
        except Exception:
            pass


def delete_remote_recording(
    device: DeviceSettings,
    source_dir: str,
    base: str,
    log_cb: Callable[[str], None] = print,
) -> bool:
    """
    Löscht .mjpg und .wav einer Aufnahme auf dem Gerät.
    Gibt True zurück, wenn beide Dateien erfolgreich gelöscht wurden.
    """
    src = source_dir.rstrip("/")
    ok_mjpg = _ssh_exec(device, f"rm -f {src}/{base}.mjpg")
    ok_wav  = _ssh_exec(device, f"rm -f {src}/{base}.wav")
    if ok_mjpg and ok_wav:
        log_cb(f"  Quelldateien gelöscht: {base}")
        return True
    else:
        log_cb(f"  [Warnung] Konnte Quelldateien nicht löschen: {base}")
        return False


# ═════════════════════════════════════════════════════════════════
#  SFTP-Download mit Fortsetzen-Unterstützung
# ═════════════════════════════════════════════════════════════════


def _sftp_delete_recording(
    sftp,
    source_dir: str,
    base: str,
    log_cb: Callable[[str], None] = print,
) -> bool:
    """Löscht .mjpg und .wav einer Aufnahme über die bestehende SFTP-Verbindung."""
    src = source_dir.rstrip("/")
    ok = True
    for ext in (".mjpg", ".wav"):
        try:
            sftp.remove(f"{src}/{base}{ext}")
        except FileNotFoundError:
            pass
        except Exception:
            ok = False
    if ok:
        log_cb(f"  Quelldateien gelöscht: {base}")
    else:
        log_cb(f"  [Warnung] Konnte Quelldateien nicht löschen: {base}")
    return ok


def _sftp_download_file(
    sftp,
    remote_path: str,
    local_path: str,
    total_size: Optional[int] = None,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    cancel_flag: Optional[threading.Event] = None,
    log_cb: Callable[[str], None] = print,
) -> bool:
    """
    Lädt eine Datei via SFTP herunter.

    Unterstützt Fortsetzen: Wenn eine teilweise heruntergeladene Datei
    existiert, wird der Download an der letzten Position fortgesetzt.

    Verwendet ``readv()`` mit gepipelineten Batch-Anfragen statt
    ``prefetch()``+``read()``, was bei großen Dateien (>1 GB) drastisch
    schneller ist und Cancel-Anfragen innerhalb von ~64 KB verarbeitet.

    Returns True wenn die Datei vollständig heruntergeladen wurde.
    """
    if total_size is None or total_size <= 0:
        total_size = sftp.stat(remote_path).st_size
    assert total_size is not None and total_size > 0, f"Cannot determine size of {remote_path}"

    local = Path(local_path)
    resume_offset = 0

    if local.exists():
        local_size = local.stat().st_size
        if local_size == total_size:
            if progress_cb:
                progress_cb(total_size, total_size)
            return True
        elif 0 < local_size < total_size:
            resume_offset = local_size
            log_cb(
                f"    Fortsetzen ab {_fmt_size(resume_offset)} "
                f"von {_fmt_size(total_size)}"
            )
        # Lokal größer als Remote → inkonsistent, neu herunterladen

    transferred = resume_offset
    open_mode = 'ab' if resume_offset > 0 else 'wb'

    with sftp.open(remote_path, 'rb') as rf, \
         open(local_path, open_mode) as lf:

        while transferred < total_size:
            if cancel_flag and cancel_flag.is_set():
                raise _CancelledError()

            # Batch von Read-Ranges aufbauen (absolute Offsets)
            chunks: list[tuple[int, int]] = []
            pos = transferred
            for _ in range(_PIPELINE_DEPTH):
                if pos >= total_size:
                    break
                size = min(_REQUEST_SIZE, total_size - pos)
                chunks.append((pos, size))
                pos += size

            if not chunks:
                break

            # readv() pipelinet alle Anfragen auf einmal und liefert
            # Antworten einzeln zurück – erlaubt Cancel zwischen Chunks
            for data in rf.readv(chunks):
                if cancel_flag and cancel_flag.is_set():
                    raise _CancelledError()
                lf.write(data)
                transferred += len(data)
                if progress_cb:
                    progress_cb(transferred, total_size)

    actual = Path(local_path).stat().st_size
    if actual != total_size:
        log_cb(
            f"    [Warnung] Größe stimmt nicht überein: "
            f"{_fmt_size(actual)} statt {_fmt_size(total_size)}"
        )
        return False
    return True


# ═════════════════════════════════════════════════════════════════
#  rsync-basierter Download (nativ, schnell)
# ═════════════════════════════════════════════════════════════════


def _can_use_rsync(device: DeviceSettings) -> bool:
    """Prüft ob rsync für dieses Gerät nutzbar ist."""
    if not shutil.which("rsync"):
        return False
    # Bei Passwort-Auth brauchen wir sshpass (egal ob ssh_key gesetzt ist)
    if device.password:
        return bool(shutil.which("sshpass"))
    return True


def _build_ssh_cmd(device: DeviceSettings) -> str:
    """Baut den SSH-Befehlsstring für rsync -e zusammen."""
    parts = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR",
    ]
    if device.port != 22:
        parts += ["-p", str(device.port)]
    if device.ssh_key:
        key = os.path.expanduser(device.ssh_key)
        parts += ["-i", key]
    # Ohne Passwort: BatchMode verhindert jegliche interaktive Abfrage
    if not device.password:
        parts += ["-o", "BatchMode=yes"]
    return " ".join(parts)


def _rsync_download_file(
    device: DeviceSettings,
    remote_path: str,
    local_path: str,
    total_size: int,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    cancel_flag: Optional[threading.Event] = None,
    log_cb: Callable[[str], None] = print,
) -> bool:
    """
    Lädt eine Datei via rsync über nativen SSH-Client herunter.

    Vorteile gegenüber Paramiko-SFTP:
    - Hardware-beschleunigte AES-NI Verschlüsselung (5–10x schneller)
    - Nativer C-Code statt reines Python
    - Eingebautes Resume mit --append --partial
    - Sofort abbrechbar (Prozess kill)

    Returns True wenn die Datei vollständig heruntergeladen wurde.
    """
    local = Path(local_path)

    # Bereits vollständig?
    if local.exists() and local.stat().st_size == total_size:
        if progress_cb:
            progress_cb(total_size, total_size)
        return True

    # Resume-Info loggen
    if local.exists() and local.stat().st_size > 0:
        log_cb(
            f"    Fortsetzen ab {_fmt_size(local.stat().st_size)} "
            f"von {_fmt_size(total_size)}"
        )

    ssh_cmd = _build_ssh_cmd(device)
    remote_uri = f"{device.username}@{device.ip}:{remote_path}"

    cmd: list[str] = [
        "rsync",
        "--partial",       # Partielle Dateien behalten
        "--append",        # An bestehende Datei anhängen (Resume)
        "--inplace",       # Direkt in Zieldatei schreiben (kein Temp)
        "-e", ssh_cmd,
        remote_uri,
        str(local_path),
    ]

    # Passwort-Auth über sshpass – auch wenn ssh_key gesetzt ist,
    # damit bei fehlgeschlagener Key-Auth das Passwort automatisch greift.
    if device.password:
        cmd = ["sshpass", "-p", device.password] + cmd

    # Umgebung: SSH_ASKPASS und DISPLAY unterdrücken,
    # damit SSH niemals ein GUI-Passwort-Dialog öffnet.
    env = os.environ.copy()
    env.pop("SSH_ASKPASS", None)
    env.pop("DISPLAY", None)

    try:
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
    except FileNotFoundError as exc:
        log_cb(f"    [Fehler] Befehl nicht gefunden: {exc}")
        return False

    try:
        while process.poll() is None:
            if cancel_flag and cancel_flag.is_set():
                process.kill()
                process.wait()
                raise _CancelledError()

            # Fortschritt über lokale Dateigröße melden
            try:
                current = local.stat().st_size if local.exists() else 0
            except OSError:
                current = 0

            if progress_cb and total_size > 0:
                progress_cb(current, total_size)

            # 4 Hz Polling – gut für Progress + reaktionsschneller Cancel
            time.sleep(0.25)

        rc = process.returncode
        if rc != 0:
            stderr_out = process.stderr
            err_msg = stderr_out.read().decode(errors="replace").strip() if stderr_out else ""
            log_cb(f"    [rsync Fehler] Exit-Code {rc}: {err_msg}")
            return False

        # Finale Größenprüfung
        actual = local.stat().st_size if local.exists() else 0
        if progress_cb:
            progress_cb(actual, total_size)

        if actual != total_size:
            log_cb(
                f"    [Warnung] Größe stimmt nicht überein: "
                f"{_fmt_size(actual)} statt {_fmt_size(total_size)}"
            )
            return False
        return True

    except _CancelledError:
        raise
    except Exception:
        try:
            process.kill()
            process.wait()
        except Exception:
            pass
        raise


# ═════════════════════════════════════════════════════════════════
#  Dateiliste (ohne Download)
# ═════════════════════════════════════════════════════════════════

def list_camera_files(
    device: DeviceSettings,
    config: CameraSettings,
) -> list[dict]:
    """Listet verfügbare Aufnahmen auf einer Kamera auf, ohne sie herunterzuladen.

    Gibt eine Liste von Dicts zurück:
      {"base": str, "size_mjpg": int, "size_wav": int,
       "total_size": int, "has_wav": bool}

    Raises RuntimeError bei Verbindungsproblemen.
    """
    try:
        client, sftp = _connect(device)
    except Exception as exc:
        raise RuntimeError(
            f"Verbindung zu {device.name} ({device.ip}) fehlgeschlagen: {exc}") from exc
    try:
        try:
            remote_files = sftp.listdir(config.source)
        except Exception as exc:
            raise RuntimeError(
                f"Verzeichnis {config.source} auf {device.name} "
                f"konnte nicht gelesen werden: {exc}") from exc

        src = config.source.rstrip("/")
        mjpg_bases = {os.path.splitext(f)[0] for f in remote_files
                      if f.lower().endswith(".mjpg")}
        wav_bases  = {os.path.splitext(f)[0] for f in remote_files
                      if f.lower().endswith(".wav")}

        result = []
        for base in sorted(mjpg_bases):
            size_m = _remote_size(sftp, f"{src}/{base}.mjpg") or 0
            size_w = _remote_size(sftp, f"{src}/{base}.wav") or 0 if base in wav_bases else 0
            result.append({
                "base":       base,
                "size_mjpg":  size_m,
                "size_wav":   size_w,
                "total_size": size_m + size_w,
                "has_wav":    base in wav_bases,
            })
        return result
    finally:
        try:
            sftp.close()
            client.close()
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════════
#  Download-Logik
# ═════════════════════════════════════════════════════════════════

def download_device(
    device: DeviceSettings,
    config: CameraSettings,
    log_cb: Callable[[str], None] = print,
    progress_cb: Optional[Callable[[str, str, int, int], None]] = None,
    cancel_flag: Optional[threading.Event] = None,
    destination_override: str = "",
    delete_after_download: bool = False,
    selective_bases: Optional[set] = None,
) -> list:
    """
    Lädt alle vollständigen Aufnahmen (.mjpg + .wav) von einem Gerät herunter.

    Rückgabe: Liste von (local_dir: str, base: str, mjpg_path: str)
    für jede erfolgreich vorhandene/heruntergeladene Aufnahme.

    progress_cb(device_name, filename, transferred_bytes, total_bytes)
    """
    dest_root = Path(destination_override or config.destination)
    dest_root.mkdir(parents=True, exist_ok=True)

    local_dir = dest_root / device.name
    local_dir.mkdir(parents=True, exist_ok=True)

    log_cb(f"Verbinde mit {device.name} ({device.ip}:{device.port}) ...")
    try:
        client, sftp = _connect(device)
    except Exception as exc:
        log_cb(f"[Fehler] Verbindung zu {device.name} fehlgeschlagen: {exc}")
        return []

    # Transfermethode wählen
    use_rsync = _can_use_rsync(device)
    if use_rsync:
        log_cb(f"  Transfermethode: rsync (nativ, schnell)")
    else:
        if not shutil.which("rsync"):
            log_cb(f"  Transfermethode: SFTP (rsync nicht installiert)")
        elif device.password and not shutil.which("sshpass"):
            log_cb(f"  Transfermethode: SFTP (sshpass fehlt für Passwort-Auth)")
        else:
            log_cb(f"  Transfermethode: SFTP")

    def _download_file(
        remote: str, local: str, size: Optional[int],
        prog_cb: Optional[Callable[[int, int], None]],
    ) -> bool:
        """Delegiert an rsync oder SFTP je nach Verfügbarkeit."""
        if use_rsync:
            return _rsync_download_file(
                device, remote, local,
                total_size=size or 0,
                progress_cb=prog_cb,
                cancel_flag=cancel_flag,
                log_cb=log_cb,
            )
        return _sftp_download_file(
            sftp, remote, local,
            total_size=size,
            progress_cb=prog_cb,
            cancel_flag=cancel_flag,
            log_cb=log_cb,
        )

    results: list = []
    try:
        try:
            remote_files = sftp.listdir(config.source)
        except Exception as exc:
            log_cb(f"[Fehler] Kann {config.source} auf {device.name} nicht auflisten: {exc}")
            return []

        mjpgs = {os.path.splitext(f)[0] for f in remote_files if f.lower().endswith(".mjpg")}
        wavs  = {os.path.splitext(f)[0] for f in remote_files if f.lower().endswith(".wav")}
        bases = sorted(mjpgs & wavs)
        log_cb(f"{device.name}: {len(bases)} vollstaendige Aufnahme(n) gefunden")

        # Bei selektiver Dateiliste nur gewünschte Dateien herunterladen
        if selective_bases:
            bases = [b for b in bases if b in selective_bases]
            log_cb(f"{device.name}: {len(bases)} Aufnahme(n) ausgewählt")

        for idx, base in enumerate(bases, 1):
            if cancel_flag and cancel_flag.is_set():
                log_cb("Abgebrochen.")
                break

            src_dir = config.source.rstrip("/")
            remote_mjpg = f"{src_dir}/{base}.mjpg"
            remote_wav  = f"{src_dir}/{base}.wav"
            local_mjpg  = local_dir / (base + ".mjpg")
            local_wav   = local_dir / (base + ".wav")

            # Remote-Dateigrössen bestimmen
            r_mjpg_size = _remote_size(sftp, remote_mjpg)
            r_wav_size  = _remote_size(sftp, remote_wav)

            total_recording = (r_mjpg_size or 0) + (r_wav_size or 0)
            log_cb(
                f"[{idx}/{len(bases)}] {base}  "
                f"({_fmt_size(total_recording)})"
            )

            # Bereits vollständig vorhanden? -> Grössenvergleich
            already_ok = (
                local_mjpg.exists() and local_wav.exists()
                and r_mjpg_size is not None and r_wav_size is not None
                and local_mjpg.stat().st_size == r_mjpg_size
                and local_wav.stat().st_size  == r_wav_size
            )
            if already_ok:
                log_cb(f"  Ueberspringe (bereits vorhanden, gleiche Groesse)")
                results.append((str(local_dir), base, str(local_mjpg)))
                if delete_after_download:
                    _sftp_delete_recording(sftp, config.source, base, log_cb)
                continue

            def _progress(transferred: int, total: int, _dev=device.name, _fn=""):
                if progress_cb and total > 0:
                    progress_cb(_dev, _fn, transferred, total)

            try:
                # .mjpg herunterladen (mit Resume-Unterstützung)
                log_cb(f"  .mjpg ({_fmt_size(r_mjpg_size or 0)}) ...")
                ok_mjpg = _download_file(
                    remote_mjpg, str(local_mjpg), r_mjpg_size,
                    lambda t, tot, _fn=base + ".mjpg": _progress(t, tot, _fn=_fn),
                )

                if cancel_flag and cancel_flag.is_set():
                    log_cb("  Abgebrochen (Datei bleibt fuer Fortsetzen erhalten).")
                    break

                # .wav herunterladen (mit Resume-Unterstützung)
                log_cb(f"  .wav ({_fmt_size(r_wav_size or 0)}) ...")
                ok_wav = _download_file(
                    remote_wav, str(local_wav), r_wav_size,
                    lambda t, tot, _fn=base + ".wav": _progress(t, tot, _fn=_fn),
                )

                if ok_mjpg and ok_wav:
                    results.append((str(local_dir), base, str(local_mjpg)))
                    log_cb(f"  -> {base} fertig")
                    if delete_after_download:
                        _sftp_delete_recording(sftp, config.source, base, log_cb)
                else:
                    log_cb(f"  [Warnung] {base}: Groessenvergleich fehlgeschlagen")

            except _CancelledError:
                # Partielle Dateien bewusst beibehalten für Resume
                log_cb("  Abgebrochen (Datei bleibt fuer Fortsetzen erhalten).")
                break
            except Exception as exc:
                log_cb(f"[Fehler] Download von {base}: {exc}")
                # Bei echten Fehlern partielle Dateien entfernen
                for p in (local_mjpg, local_wav):
                    try:
                        p.unlink(missing_ok=True)
                    except Exception:
                        pass

    finally:
        try:
            sftp.close()
            client.close()
        except Exception:
            pass

    log_cb(f"{device.name}: {len(results)} Aufnahme(n) heruntergeladen/vorhanden")
    return results
