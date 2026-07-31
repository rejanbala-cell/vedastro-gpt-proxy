from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any

import requests

from .config import settings
from .football_data import FootballDataError, client as football_client
from .text_utils import domain_from_url, normalize_text, team_supported


_ODDS = re.compile(r"(?<!\d)(1\.\d{1,2}|[2-9]\.\d{1,2})(?!\d)")
_DRAW_WORDS = ("draw", "tie", "x")
_HOME_WORDS = ("home win", "1", "hosts")
_AWAY_WORDS = ("away win", "2", "visitors")


def _tavily_search(query: str) -> dict[str, Any]:
    if not settings.tavily_api_key or not settings.tavily_enabled:
        return {
            "status": "unavailable",
            "results": [],
            "reason": "tavily_not_configured",
        }
    try:
        response = requests.post(
            f"{settings.tavily_base_url}/search",
            headers={
                "Authorization": f"Bearer {settings.tavily_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "query": query,
                "topic": "general",
                "search_depth": settings.tavily_search_depth,
                "max_results": settings.prediction_evidence_max_results,
                "include_answer": "basic",
                "include_images": False,
            },
            timeout=(5, settings.prediction_tavily_timeout_seconds),
        )
    except requests.RequestException as exc:
        return {
            "status": "transport_error",
            "results": [],
            "error_type": type(exc).__name__,
        }
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    if response.status_code != 200:
        return {
            "status": "http_error",
            "http_status": response.status_code,
            "results": [],
            "provider_message": (
                payload.get("detail")
                if isinstance(payload, dict)
                else None
            ),
        }
    return {
        "status": "ok",
        "answer": payload.get("answer"),
        "results": (
            payload.get("results")
            if isinstance(payload.get("results"), list)
            else []
        ),
        "request_id": payload.get("request_id"),
    }


def _find_team_odd(blob: str, team_name: str) -> list[float]:
    normalized = normalize_text(blob)
    tokens = [
        token for token in normalize_text(team_name).split()
        if len(token) >= 3
    ]
    if not tokens:
        return []
    matches: list[float] = []
    for match in _ODDS.finditer(blob):
        left = normalize_text(blob[max(0, match.start()-90):match.start()])
        right = normalize_text(blob[match.end():match.end()+90])
        neighbourhood = left + " " + right
        if any(token in neighbourhood for token in tokens):
            value = float(match.group(1))
            if 1.01 <= value <= 20:
                matches.append(value)
    return matches


def _find_draw_odds(blob: str) -> list[float]:
    values: list[float] = []
    lowered = normalize_text(blob)
    for match in _ODDS.finditer(blob):
        neighbourhood = normalize_text(
            blob[max(0, match.start()-70):match.end()+70]
        )
        if any(word in neighbourhood for word in _DRAW_WORDS):
            value = float(match.group(1))
            if 1.01 <= value <= 20:
                values.append(value)
    return values


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def market_consensus(fixture: dict[str, Any]) -> dict[str, Any]:
    home = fixture["home_team"]
    away = fixture["away_team"]
    kickoff = fixture["kickoff_utc"]
    date_text = (
        kickoff.date().isoformat()
        if isinstance(kickoff, datetime)
        else str(kickoff)[:10]
    )
    query = (
        f'"{home}" vs "{away}" {date_text} '
        "1X2 decimal odds bookmaker home draw away"
    )
    search = _tavily_search(query)
    home_by_domain: dict[str, list[float]] = {}
    away_by_domain: dict[str, list[float]] = {}
    draw_by_domain: dict[str, list[float]] = {}
    sources: list[dict[str, Any]] = []

    for row in search.get("results", []):
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "")
        content = str(row.get("content") or "")
        url = str(row.get("url") or "")
        blob = f"{title}\n{content}"
        if not (
            team_supported(home, blob)
            and team_supported(away, blob)
        ):
            continue
        domain = domain_from_url(url)
        if not domain:
            continue
        h = _find_team_odd(blob, home)
        a = _find_team_odd(blob, away)
        d = _find_draw_odds(blob)
        if h:
            home_by_domain.setdefault(domain, []).extend(h)
        if a:
            away_by_domain.setdefault(domain, []).extend(a)
        if d:
            draw_by_domain.setdefault(domain, []).extend(d)
        sources.append({
            "title": title[:250],
            "url": url[:1000],
            "domain": domain,
            "home_odds": h,
            "draw_odds": d,
            "away_odds": a,
        })

    home_values = [
        _median(values) for values in home_by_domain.values()
        if _median(values) is not None
    ]
    away_values = [
        _median(values) for values in away_by_domain.values()
        if _median(values) is not None
    ]
    draw_values = [
        _median(values) for values in draw_by_domain.values()
        if _median(values) is not None
    ]
    home_median = _median([v for v in home_values if v])
    away_median = _median([v for v in away_values if v])
    draw_median = _median([v for v in draw_values if v])

    distinct = len(
        set(home_by_domain) | set(away_by_domain) | set(draw_by_domain)
    )
    verified = bool(
        home_median
        and away_median
        and distinct >= settings.prediction_market_min_domains
    )

    favourite_side = None
    favourite_team = None
    near_pickem = False
    if verified:
        if abs(home_median - away_median) <= 0.08:
            near_pickem = True
        elif home_median < away_median:
            favourite_side = "home"
            favourite_team = home
        else:
            favourite_side = "away"
            favourite_team = away

    return {
        "status": "verified" if verified else "unverified",
        "query": query,
        "distinct_domains": distinct,
        "required_domains": settings.prediction_market_min_domains,
        "home_median_odds": home_median,
        "draw_median_odds": draw_median,
        "away_median_odds": away_median,
        "favourite_side": favourite_side,
        "favourite_team": favourite_team,
        "near_pickem": near_pickem,
        "sources": sources,
        "search_status": search.get("status"),
        "search_answer": search.get("answer"),
    }


def _competition_code(raw: dict[str, Any]) -> str:
    competition = raw.get("competition")
    if isinstance(competition, dict):
        return str(
            competition.get("code")
            or competition.get("id")
            or ""
        ).strip()
    return ""


def _team_id(raw: dict[str, Any], side: str) -> int | None:
    team = raw.get(f"{side}Team")
    if not isinstance(team, dict):
        return None
    value = team.get("id")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _standing_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    standings = payload.get("standings")
    if not isinstance(standings, list):
        return []
    preferred = None
    for item in standings:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "").upper() == "TOTAL":
            preferred = item
            break
    preferred = preferred or next(
        (item for item in standings if isinstance(item, dict)),
        {},
    )
    table = preferred.get("table") if isinstance(preferred, dict) else []
    return [row for row in table if isinstance(row, dict)] if isinstance(table, list) else []


def performance_snapshot(fixture: dict[str, Any]) -> dict[str, Any]:
    raw = fixture.get("raw_fixture_json")
    raw = raw if isinstance(raw, dict) else {}
    code = _competition_code(raw)
    home_id = _team_id(raw, "home")
    away_id = _team_id(raw, "away")
    standings_payload = {}
    provider_status = "unavailable"
    if code:
        try:
            standings_payload = football_client.standings(code)
            provider_status = "ok"
        except FootballDataError as exc:
            provider_status = f"error:{exc.status_code or 'transport'}"

    rows = _standing_rows(standings_payload)
    by_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        team = row.get("team")
        if not isinstance(team, dict):
            continue
        try:
            by_id[int(team.get("id"))] = row
        except (TypeError, ValueError):
            continue

    home_row = by_id.get(home_id or -1)
    away_row = by_id.get(away_id or -1)

    def metrics(row: dict[str, Any] | None) -> dict[str, float | int | None]:
        if not row:
            return {
                "position": None,
                "played": 0,
                "points": 0,
                "ppg": None,
                "goal_difference": 0,
                "draw_rate": None,
            }
        played = int(row.get("playedGames") or 0)
        points = int(row.get("points") or 0)
        draws = int(row.get("draw") or 0)
        return {
            "position": row.get("position"),
            "played": played,
            "points": points,
            "ppg": round(points / played, 4) if played else None,
            "goal_difference": int(row.get("goalDifference") or 0),
            "draw_rate": round(draws / played, 4) if played else None,
        }

    home = metrics(home_row)
    away = metrics(away_row)
    home_ppg = home["ppg"]
    away_ppg = away["ppg"]

    if home_ppg is None or away_ppg is None:
        # Search-based preview fallback. It is used as current evidence only,
        # not as a market assignment.
        kickoff = fixture["kickoff_utc"]
        date_text = (
            kickoff.date().isoformat()
            if isinstance(kickoff, datetime)
            else str(kickoff)[:10]
        )
        query = (
            f'"{fixture["home_team"]}" vs "{fixture["away_team"]}" '
            f"{date_text} form injuries preview prediction"
        )
        search = _tavily_search(query)
        answer = normalize_text(search.get("answer"))
        home_mentions = sum(
            answer.count(token)
            for token in normalize_text(fixture["home_team"]).split()
            if len(token) >= 4
        )
        away_mentions = sum(
            answer.count(token)
            for token in normalize_text(fixture["away_team"]).split()
            if len(token) >= 4
        )
        score_home = settings.prediction_home_advantage
        score_away = 0.0
        if home_mentions > away_mentions:
            score_home += 0.08
        elif away_mentions > home_mentions:
            score_away += 0.08
        draw_score = 0.12 if "draw" in answer else 0.0
        source = "tavily_preview_fallback"
        evidence_complete = bool(search.get("results"))
    else:
        score_home = float(home_ppg) + settings.prediction_home_advantage
        score_away = float(away_ppg)
        average_draw_rate = (
            float(home["draw_rate"] or 0)
            + float(away["draw_rate"] or 0)
        ) / 2
        draw_score = average_draw_rate
        source = "football-data.org_standings"
        evidence_complete = True

    margin = score_home - score_away
    close = abs(margin) <= settings.prediction_draw_margin
    high_draw = draw_score >= 0.28

    if close and high_draw:
        baseline = "draw"
    elif margin >= 0:
        baseline = "home"
    else:
        baseline = "away"

    return {
        "status": "complete" if evidence_complete else "limited",
        "source": source,
        "provider_status": provider_status,
        "competition_code": code or None,
        "home": home,
        "away": away,
        "home_score": round(score_home, 4),
        "away_score": round(score_away, 4),
        "draw_score": round(draw_score, 4),
        "score_margin": round(margin, 4),
        "close_match": close,
        "high_draw_evidence": high_draw,
        "baseline_outcome": baseline,
    }
