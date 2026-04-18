from __future__ import annotations

from ..integrations.youtube_title_editor import MatchData
from ..settings import AppSettings
from .model import WorkflowJob


def default_match_data(settings: AppSettings | None) -> MatchData:
    values = settings.default_match_values() if settings is not None else {}
    return MatchData(
        date_iso=str(values.get("date_iso") or "").strip(),
        competition=str(values.get("competition") or "").strip(),
        home_team=str(values.get("home_team") or "").strip(),
        away_team=str(values.get("away_team") or "").strip(),
        location=str(values.get("location") or "").strip(),
    )


def resolve_match_data(settings: AppSettings | None, overrides: dict | None) -> MatchData:
    defaults = default_match_data(settings)
    payload = overrides if isinstance(overrides, dict) else {}
    return MatchData(
        date_iso=str(payload.get("date_iso") or defaults.date_iso).strip(),
        competition=str(payload.get("competition") or defaults.competition).strip(),
        home_team=str(payload.get("home_team") or defaults.home_team).strip(),
        away_team=str(payload.get("away_team") or defaults.away_team).strip(),
        location=str(payload.get("location") or defaults.location).strip(),
    )


def titlecard_match_data(settings: AppSettings | None, job: WorkflowJob) -> MatchData:
    defaults = default_match_data(settings)
    return MatchData(
        date_iso=(job.title_card_date or "").strip() or defaults.date_iso,
        competition=defaults.competition,
        home_team=(job.title_card_home_team or "").strip() or defaults.home_team,
        away_team=(job.title_card_away_team or "").strip() or defaults.away_team,
        location=defaults.location,
    )


def resolve_kaderblick_game_id(settings: AppSettings | None, job: WorkflowJob, explicit: str = "") -> str:
    if (explicit or "").strip():
        return explicit.strip()
    if (job.default_kaderblick_game_id or "").strip():
        return job.default_kaderblick_game_id.strip()
    return (getattr(settings, "default_kaderblick_game_id", "") or "").strip()