"""Tests für youtube_title_editor.py (Generator-Funktionen).

Geprüft:
- _teams_str            (neu hinzugefügte Hilfsfunktion)
- build_playlist_title
- build_video_title     (Längen-Fallback-Kette)
- build_video_description
"""

import pytest

from src.youtube_title_editor import (
    MatchData,
    SegmentData,
    _teams_str,
    build_playlist_title,
    build_video_title,
    build_video_description,
)

MAX_LEN = 100


# ─── _teams_str ───────────────────────────────────────────────────────────────

class TestTeamsStr:
    def test_both_teams(self):
        m = MatchData(home_team="FC Bayern", away_team="Borussia")
        assert _teams_str(m) == "FC Bayern vs Borussia"

    def test_only_home(self):
        m = MatchData(home_team="FC Muster", away_team="")
        assert _teams_str(m) == "FC Muster"

    def test_only_away(self):
        m = MatchData(home_team="", away_team="SV Gegner")
        assert _teams_str(m) == "SV Gegner"

    def test_no_teams(self):
        m = MatchData(home_team="", away_team="")
        assert _teams_str(m) == ""

    def test_whitespace_teams_are_truthy(self):
        # Leerzeichen zählt als truthy → beide Seiten vorhanden
        m = MatchData(home_team=" ", away_team=" ")
        assert "vs" in _teams_str(m)


# ─── build_playlist_title ─────────────────────────────────────────────────────

class TestBuildPlaylistTitle:
    def _std_match(self) -> MatchData:
        return MatchData(
            date_iso="2026-03-19",
            competition="Sparkassenpokal",
            home_team="FC Heimat",
            away_team="SV Gast",
        )

    def test_full_format(self):
        title = build_playlist_title(self._std_match())
        assert title == "19.03.2026 | Sparkassenpokal | FC Heimat vs SV Gast"

    def test_date_formatted_correctly(self):
        m = MatchData(date_iso="2026-01-05")
        title = build_playlist_title(m)
        assert title.startswith("05.01.2026")

    def test_missing_competition(self):
        m = MatchData(date_iso="2026-03-19", home_team="A", away_team="B")
        title = build_playlist_title(m)
        assert "Sparkassenpokal" not in title
        assert "A vs B" in title

    def test_missing_teams(self):
        m = MatchData(date_iso="2026-03-19", competition="Liga")
        title = build_playlist_title(m)
        assert "Liga" in title
        assert "vs" not in title

    def test_empty_match(self):
        title = build_playlist_title(MatchData())
        assert title == ""

    def test_max_length_respected(self):
        m = MatchData(
            date_iso="2026-03-19",
            competition="A" * 80,
            home_team="B" * 80,
            away_team="C" * 80,
        )
        assert len(build_playlist_title(m)) <= MAX_LEN

    def test_invalid_date_used_as_is(self):
        m = MatchData(date_iso="not-a-date", competition="Liga")
        title = build_playlist_title(m)
        assert "not-a-date" in title


# ─── build_video_title ────────────────────────────────────────────────────────

class TestBuildVideoTitle:
    def _match(self) -> MatchData:
        return MatchData(
            date_iso="2026-03-19",
            competition="Verbandsliga",
            home_team="FC Heim",
            away_team="SV Gast",
        )

    def _seg(self, **kwargs) -> SegmentData:
        defaults = dict(
            camera="DJI Osmo Action5 Pro",
            side="Links",
            half=1,
            part=0,
            type_name="1. Halbzeit",
        )
        defaults.update(kwargs)
        return SegmentData(**defaults)

    def test_full_title_contains_date_and_teams(self):
        title = build_video_title(self._match(), self._seg())
        assert "2026-03-19" in title
        assert "FC Heim vs SV Gast" in title

    def test_title_always_within_max_len(self):
        m = MatchData(
            date_iso="2025-12-31",
            competition="Langer Wettbewerbsname e.V. Kreis IV",
            home_team="Langer Vereinsname Sportfreunde",
            away_team="Auch Langer Vereinsname Turnerschaft",
        )
        s = SegmentData(
            camera="DJI Osmo Action5 Pro",
            side="Links",
            half=2,
            part=3,
            type_name="2. Halbzeit",
        )
        assert len(build_video_title(m, s)) <= MAX_LEN

    def test_part_suffix_added(self):
        s = self._seg(part=2)
        title = build_video_title(self._match(), s)
        assert "Teil 2" in title

    def test_no_part_when_zero(self):
        s = self._seg(part=0)
        title = build_video_title(self._match(), s)
        assert "Teil" not in title

    def test_empty_match_uses_type(self):
        m = MatchData()
        s = SegmentData(type_name="Vorbereitung", half=1)
        title = build_video_title(m, s)
        assert "Vorbereitung" in title

    def test_fallback_removes_camera(self):
        """Wenn Titel mit Kamera zu lang → Kamera-Name muss wegfallen."""
        m = MatchData(
            date_iso="2026-03-19",
            home_team="VfB Sehr Langer Vereinsname SC",
            away_team="TSV Auch Sehr Langer Gegner FC",
        )
        s = SegmentData(
            camera="Kaderblick Links Tribüne Hauptkamera",
            side="Links",
            half=1,
            part=2,
            type_name="1. Halbzeit",
        )
        title = build_video_title(m, s)
        assert len(title) <= MAX_LEN
        # Kamera wurde im Fallback entfernt
        assert "Kaderblick Links Tribüne Hauptkamera" not in title

    def test_type_name_used_over_half(self):
        """type_name hat Vorrang vor automatisch generiertem 'N. Halbzeit'."""
        s = SegmentData(half=2, type_name="Nachspielzeit")
        title = build_video_title(self._match(), s)
        assert "Nachspielzeit" in title
        assert "2. Halbzeit" not in title


# ─── build_video_description ─────────────────────────────────────────────────

class TestBuildVideoDescription:
    def test_contains_date(self):
        m = MatchData(date_iso="2026-03-19")
        s = SegmentData()
        desc = build_video_description(m, s)
        assert "2026" in desc

    def test_contains_teams(self):
        m = MatchData(home_team="FC Heim", away_team="SV Gast")
        s = SegmentData()
        desc = build_video_description(m, s)
        assert "FC Heim" in desc
        assert "SV Gast" in desc

    def test_empty_match_no_crash(self):
        # Keine Exception bei leeren Pflichtfeldern erwartet
        desc = build_video_description(MatchData(), SegmentData())
        assert isinstance(desc, str)
