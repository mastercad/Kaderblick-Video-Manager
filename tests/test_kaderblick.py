"""Tests für kaderblick.py:

Geprüft:
- KaderblickRegistry: already_posted / record / Persistenz
- _headers(): korrekte Authorization-Header je auth_mode
- _PostPreservingRedirectHandler: POST bleibt POST bei Redirect
- _ssl_ctx(): https → SSL-Kontext, http → None
- _refresh_jwt(): Payload und Token-Rückgabe (gemockt)
"""

import json
import tempfile
import urllib.request
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.kaderblick import (
    KaderblickRegistry,
    _headers,
    _PostPreservingRedirectHandler,
    _ssl_ctx,
)


# ─── KaderblickRegistry ───────────────────────────────────────────────────────

class TestKaderblickRegistry:
    def _reg(self, tmp: str) -> KaderblickRegistry:
        return KaderblickRegistry(path=Path(tmp) / "kb.json")

    def test_initially_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            reg = self._reg(tmp)
            assert reg.already_posted("yt-abc") is None

    def test_record_and_retrieve(self):
        with tempfile.TemporaryDirectory() as tmp:
            reg = self._reg(tmp)
            reg.record("yt-abc", kaderblick_id=7, game_id="42", name="Halbzeit 1")
            assert reg.already_posted("yt-abc") == 7

    def test_persisted_to_disk(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kb.json"
            reg = KaderblickRegistry(path=path)
            reg.record("yt-xyz", kaderblick_id=99, game_id="1", name="Test")
            # Neu laden
            reg2 = KaderblickRegistry(path=path)
            assert reg2.already_posted("yt-xyz") == 99

    def test_file_contains_posted_at(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kb.json"
            reg = KaderblickRegistry(path=path)
            reg.record("yt-ts", kaderblick_id=1, game_id="5", name="V")
            data = json.loads(path.read_text())
            assert "posted_at" in data["yt-ts"]

    def test_multiple_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            reg = self._reg(tmp)
            reg.record("yt-1", 1, "10", "A")
            reg.record("yt-2", 2, "10", "B")
            assert reg.already_posted("yt-1") == 1
            assert reg.already_posted("yt-2") == 2

    def test_unknown_id_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            reg = self._reg(tmp)
            reg.record("yt-known", 5, "1", "X")
            assert reg.already_posted("yt-unknown") is None

    def test_corrupt_file_handled_gracefully(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kb.json"
            path.write_text("nicht gültiges json{{{{")
            reg = KaderblickRegistry(path=path)  # darf nicht crashen
            assert reg.already_posted("anything") is None

    def test_creates_parent_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sub" / "dir" / "kb.json"
            reg = KaderblickRegistry(path=path)
            reg.record("yt-dir", 3, "7", "Test")
            assert path.exists()


# ─── _headers ────────────────────────────────────────────────────────────────

class _KB:
    """Minimales Kaderblick-Settings-Objekt für Tests."""
    def __init__(self, auth_mode="bearer", bearer="tok123", jwt="jwt456",
                 base_url="https://api.kaderblick.de"):
        self.auth_mode      = auth_mode
        self.bearer_token   = bearer
        self.jwt_token      = jwt
        self.base_url       = base_url
        self.jwt_refresh_token = ""


class TestHeaders:
    def test_bearer_mode_sets_authorization(self):
        kb = _KB(auth_mode="bearer", bearer="mytoken")
        h = _headers(kb)
        assert h["Authorization"] == "Bearer mytoken"
        assert "Cookie" not in h

    def test_jwt_mode_sets_cookie(self):
        kb = _KB(auth_mode="jwt", jwt="myjwt")
        h = _headers(kb)
        assert h["Cookie"] == "jwt_token=myjwt"
        assert "Authorization" not in h

    def test_content_type_always_json(self):
        for mode in ("bearer", "jwt"):
            h = _headers(_KB(auth_mode=mode))
            assert h["Content-Type"] == "application/json"

    def test_origin_header_present(self):
        h = _headers(_KB())
        assert "Origin" in h
        assert "kaderblick" in h["Origin"].lower()


# ─── _ssl_ctx ────────────────────────────────────────────────────────────────

class TestSslCtx:
    def test_https_returns_ssl_context(self):
        import ssl
        ctx = _ssl_ctx("https://api.kaderblick.de/api/something")
        assert isinstance(ctx, ssl.SSLContext)

    def test_http_returns_none(self):
        ctx = _ssl_ctx("http://local.example.com/api/something")
        assert ctx is None

    def test_case_insensitive(self):
        import ssl
        ctx = _ssl_ctx("HTTPS://api.kaderblick.de")
        assert isinstance(ctx, ssl.SSLContext)


# ─── _PostPreservingRedirectHandler ───────────────────────────────────────────

class TestPostPreservingRedirectHandler:
    def _make_request(self, url: str, method: str = "POST") -> urllib.request.Request:
        req = urllib.request.Request(
            url,
            data=b'{"key":"val"}',
            headers={"Content-Type": "application/json"},
            method=method,
        )
        return req

    def _handler(self) -> _PostPreservingRedirectHandler:
        return _PostPreservingRedirectHandler()

    def test_post_preserved_on_301(self):
        handler = self._handler()
        req = self._make_request("http://old.example.com/api", "POST")
        fp = MagicMock()
        new_req = handler.redirect_request(
            req, fp, 301, "Moved", {}, "https://new.example.com/api")
        assert new_req.get_method() == "POST"

    def test_body_preserved_on_redirect(self):
        handler = self._handler()
        req = self._make_request("http://old.example.com/api", "POST")
        fp = MagicMock()
        new_req = handler.redirect_request(
            req, fp, 302, "Found", {}, "https://new.example.com/api")
        assert new_req.data == b'{"key":"val"}'

    def test_new_url_used(self):
        handler = self._handler()
        req = self._make_request("http://old.example.com/api", "POST")
        fp = MagicMock()
        new_req = handler.redirect_request(
            req, fp, 307, "Temporary Redirect", {},
            "https://redirected.example.com/newpath")
        assert "redirected.example.com" in new_req.full_url

    def test_get_request_also_works(self):
        """Auch GET-Anfragen sollen korrekt weitergeleitet werden."""
        handler = self._handler()
        req = self._make_request("http://old.example.com/api", "GET")
        req.data = None
        fp = MagicMock()
        new_req = handler.redirect_request(
            req, fp, 301, "Moved", {}, "https://new.example.com/api")
        assert new_req.get_method() == "GET"


# ─── _refresh_jwt (gemockt) ───────────────────────────────────────────────────

class TestRefreshJwt:
    def test_no_refresh_token_returns_false(self):
        from src.kaderblick import _refresh_jwt
        kb = _KB(auth_mode="jwt", jwt="old")
        kb.jwt_refresh_token = ""
        assert _refresh_jwt(kb) is False

    def test_successful_refresh_updates_token(self):
        from src.kaderblick import _refresh_jwt

        kb = _KB(auth_mode="jwt", jwt="old_token")
        kb.jwt_refresh_token = "my_refresh"

        fake_response_data = json.dumps({"token": "new_token_abc"}).encode()
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = fake_response_data

        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = _refresh_jwt(kb)

        assert result is True
        assert kb.jwt_token == "new_token_abc"

    def test_failed_refresh_returns_false(self):
        from src.kaderblick import _refresh_jwt
        import urllib.error

        kb = _KB(auth_mode="jwt", jwt="old_token")
        kb.jwt_refresh_token = "bad_refresh"

        with patch("urllib.request.urlopen",
                   side_effect=urllib.error.URLError("connection refused")):
            result = _refresh_jwt(kb)

        assert result is False
        assert kb.jwt_token == "old_token"   # unverändert


# ─── post_to_kaderblick: Sort-Index-Berechnung ────────────────────────────────

class TestPostToKaderblickSortIndex:
    """Der Sort-Index wird live vom Server abgerufen, nicht vorkalkuliert.

    Szenario: Auf dem Server sind bereits N Videos für das Spiel → neues
    Video bekommt Sort-Index N+1, egal was als sort_index-Parameter übergeben wird.
    """

    def _make_settings(self, tmp: str) -> object:
        """Minimales AppSettings-Objekt für post_to_kaderblick."""
        from src.settings import AppSettings
        s = AppSettings()
        s.kaderblick.auth_mode    = "bearer"
        s.kaderblick.bearer_token = "test-token"
        s.kaderblick.base_url     = "http://localhost"
        # Verhindert echte Datei-I/O für die Registry
        import kaderblick as _kb_mod
        return s

    @staticmethod
    def _call(settings, game_videos: list[dict], expect_sort: int,
              tmp: str, existing_yt_id: str = ""):
        """Ruft post_to_kaderblick mit gemockten HTTP-Calls auf und prüft sort."""
        from src.kaderblick import post_to_kaderblick, KaderblickRegistry

        src = Path(tmp) / "video.mp4"
        src.touch()

        posted_payloads: list[dict] = []

        def fake_fetch_game_videos(kb, gid):
            return game_videos

        def fake_post_video(kb, gid, payload):
            posted_payloads.append(payload)
            return {"success": True, "video": {"id": 999}}

        reg_path = Path(tmp) / "reg.json"

        with patch("src.kaderblick.fetch_game_videos", side_effect=fake_fetch_game_videos), \
             patch("src.kaderblick.post_video", side_effect=fake_post_video), \
             patch("src.kaderblick._get_registry",
                   return_value=KaderblickRegistry(path=reg_path)):
            result = post_to_kaderblick(
                settings=settings,
                game_id="31",
                video_name="Test-Video",
                youtube_video_id="yt-new",
                youtube_url="https://youtu.be/yt-new",
                file_path=src,
                output_file_path=None,
                game_start_seconds=0,
                video_type_id=1,
                camera_id=1,
                sort_index=1,          # absichtlich zu klein / falsch
                log_callback=lambda m: None,
            )

        assert result is True
        assert posted_payloads, "post_video wurde nicht aufgerufen"
        actual_sort = int(posted_payloads[0]["sort"])
        assert actual_sort == expect_sort, (
            f"Erwartet Sort {expect_sort}, tatsächlich {actual_sort}")

    def test_no_existing_videos_sort_is_1(self):
        """Leere Server-Liste → Sort-Index 1."""
        from src.settings import AppSettings
        s = AppSettings()
        s.kaderblick.auth_mode    = "bearer"
        s.kaderblick.bearer_token = "tok"
        s.kaderblick.base_url     = "http://localhost"
        with tempfile.TemporaryDirectory() as tmp:
            self._call(s, game_videos=[], expect_sort=1, tmp=tmp)

    def test_one_existing_video_sort_is_2(self):
        """Ein Video mit Sort=1 bereits vorhanden → neues bekommt Sort=2."""
        from src.settings import AppSettings
        s = AppSettings()
        s.kaderblick.auth_mode    = "bearer"
        s.kaderblick.bearer_token = "tok"
        s.kaderblick.base_url     = "http://localhost"
        existing = [{"id": 5, "youtubeId": "yt-other", "sort": 1}]
        with tempfile.TemporaryDirectory() as tmp:
            self._call(s, game_videos=existing, expect_sort=2, tmp=tmp)

    def test_three_existing_videos_sort_is_4(self):
        """Drei Videos mit Sorts 1,2,3 → neues bekommt Sort=4."""
        from src.settings import AppSettings
        s = AppSettings()
        s.kaderblick.auth_mode    = "bearer"
        s.kaderblick.bearer_token = "tok"
        s.kaderblick.base_url     = "http://localhost"
        existing = [
            {"id": 1, "youtubeId": "yt-a", "sort": 1},
            {"id": 2, "youtubeId": "yt-b", "sort": 2},
            {"id": 3, "youtubeId": "yt-c", "sort": 3},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            self._call(s, game_videos=existing, expect_sort=4, tmp=tmp)

    def test_gaps_in_sort_uses_max_plus_1(self):
        """Lücken in der Sort-Reihenfolge: max(vorhandene) + 1 wird verwendet."""
        from src.settings import AppSettings
        s = AppSettings()
        s.kaderblick.auth_mode    = "bearer"
        s.kaderblick.bearer_token = "tok"
        s.kaderblick.base_url     = "http://localhost"
        # Sorts: 1 und 5 — max ist 5 → neues wird 6
        existing = [
            {"id": 10, "youtubeId": "yt-x", "sort": 1},
            {"id": 11, "youtubeId": "yt-y", "sort": 5},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            self._call(s, game_videos=existing, expect_sort=6, tmp=tmp)

    def test_duplicate_detection_skips_post(self):
        """Wenn youtubeId schon auf dem Server ist → kein POST."""
        from src.settings import AppSettings
        from src.kaderblick import post_to_kaderblick, KaderblickRegistry

        s = AppSettings()
        s.kaderblick.auth_mode    = "bearer"
        s.kaderblick.bearer_token = "tok"
        s.kaderblick.base_url     = "http://localhost"

        # Server kennt das Video bereits
        existing = [{"id": 77, "youtubeId": "yt-new", "sort": 1}]

        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "v.mp4"; src.touch()
            reg_path = Path(tmp) / "reg.json"

            with patch("src.kaderblick.fetch_game_videos", return_value=existing), \
                 patch("src.kaderblick.post_video") as mock_post, \
                 patch("src.kaderblick._get_registry",
                       return_value=KaderblickRegistry(path=reg_path)):
                result = post_to_kaderblick(
                    settings=s,
                    game_id="31",
                    video_name="Test",
                    youtube_video_id="yt-new",
                    youtube_url="https://youtu.be/yt-new",
                    file_path=src,
                    output_file_path=None,
                    game_start_seconds=0,
                    video_type_id=1,
                    camera_id=1,
                    sort_index=1,
                    log_callback=lambda m: None,
                )

        assert result is True
        mock_post.assert_not_called()

