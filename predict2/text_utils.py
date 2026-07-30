from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import urlparse


TEAM_STOPWORDS = {
    "fc", "cf", "sc", "ac", "afc", "club", "football",
    "futebol", "de", "the", "women", "w", "u19", "u20", "u21",
    "u23", "reserves", "ii",
}

VENUE_WORDS = (
    "stadium", "arena", "stade", "stadion", "stadio",
    "estadio", "estádio", "campo", "parque", "ground",
    "sports centre", "sports center", "sport centre",
    "sport center", "complexo desportivo", "centro desportivo",
    "olympic centre", "olympic center",
)

AGGREGATOR_DOMAINS = {
    "forebet.com", "365scores.com", "soccerway.com",
    "flashscore.com", "livescore.com", "espn.com",
    "sportsmole.co.uk", "worldfootball.net",
}


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize(
        "NFKD", str(value or "")
    ).encode("ascii", "ignore").decode("ascii")
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def meaningful_tokens(value: Any) -> set[str]:
    return {
        token
        for token in normalize_text(value).split()
        if len(token) >= 2 and token not in TEAM_STOPWORDS
    }


def text_similarity(left: Any, right: Any) -> float:
    a = normalize_text(left)
    b = normalize_text(right)
    if not a or not b:
        return 0.0

    seq = SequenceMatcher(None, a, b).ratio()
    a_tokens = meaningful_tokens(a)
    b_tokens = meaningful_tokens(b)
    union = a_tokens | b_tokens
    overlap = (
        len(a_tokens & b_tokens) / len(union)
        if union else 0.0
    )
    containment = (
        min(
            len(a_tokens & b_tokens) / max(1, len(a_tokens)),
            len(a_tokens & b_tokens) / max(1, len(b_tokens)),
        )
        if a_tokens and b_tokens else 0.0
    )
    return max(seq, overlap, containment)


def team_supported(team_name: str, blob: str) -> bool:
    team_tokens = meaningful_tokens(team_name)
    blob_tokens = meaningful_tokens(blob)
    if not team_tokens:
        return False
    required = 1 if len(team_tokens) == 1 else max(
        1, len(team_tokens) - 1
    )
    return len(team_tokens & blob_tokens) >= required


def fixture_supported(
    home_team: str,
    away_team: str,
    blob: str,
) -> bool:
    return (
        team_supported(home_team, blob)
        and team_supported(away_team, blob)
    )


def date_support(
    kickoff: datetime,
    blob: str,
    *,
    tolerance_days: int = 1,
) -> dict[str, Any]:
    normalized_blob = normalize_text(blob)
    for delta in range(-tolerance_days, tolerance_days + 1):
        candidate = kickoff + timedelta(days=delta)
        values = {
            candidate.strftime("%Y-%m-%d"),
            candidate.strftime("%d/%m/%Y"),
            candidate.strftime("%d-%m-%Y"),
            candidate.strftime("%d %B %Y"),
            candidate.strftime("%B %d %Y"),
            candidate.strftime("%d %b %Y"),
        }
        if any(
            normalize_text(value) in normalized_blob
            for value in values
        ):
            return {
                "supported": True,
                "exact": delta == 0,
                "day_delta": delta,
            }
    return {
        "supported": False,
        "exact": False,
        "day_delta": None,
    }


def domain_from_url(url: Any) -> str:
    try:
        host = urlparse(str(url or "")).hostname or ""
    except ValueError:
        return ""
    host = host.lower()
    return host[4:] if host.startswith("www.") else host


def official_like(
    *,
    url: str,
    title: str,
    content: str,
    home_team: str,
    away_team: str,
) -> bool:
    domain = domain_from_url(url)
    if not domain or any(
        domain == item or domain.endswith("." + item)
        for item in AGGREGATOR_DOMAINS
    ):
        return False

    blob = normalize_text(f"{title}\n{content}")
    if "official" in blob:
        return True

    domain_text = normalize_text(domain)
    home_tokens = sorted(
        meaningful_tokens(home_team),
        key=len,
        reverse=True,
    )
    away_tokens = sorted(
        meaningful_tokens(away_team),
        key=len,
        reverse=True,
    )
    return any(
        len(token) >= 4 and token in domain_text
        for token in [*home_tokens[:2], *away_tokens[:2]]
    )


def _clean_venue(value: str) -> str:
    value = " ".join(value.split()).strip(" ,.;:|-–—")
    value = re.sub(
        r"\s+(?:capacity|attendance|referee|kickoff|weather)\b.*$",
        "",
        value,
        flags=re.IGNORECASE,
    )
    return value.strip(" ,.;:|-–—")


def extract_venue_phrases(blob: Any) -> list[str]:
    text = str(blob or "")
    patterns = [
        re.compile(
            r"(?:venue|stadium|ground|local)\s*[:\-–—]\s*"
            r"([A-ZÀ-ÖØ-Ý0-9][^\n|;]{4,110})",
            re.IGNORECASE,
        ),
        re.compile(
            r"\bat\s+([A-ZÀ-ÖØ-Ý0-9][A-Za-zÀ-ÖØ-öø-ÿ0-9'’.\- ]{3,100}"
            r"(?:Stadium|Arena|Ground|Stade|Stadion|Stadio|"
            r"Estádio|Estadio|Campo|Parque|Sports Centre|Sports Center|"
            r"Complexo Desportivo|Centro Desportivo))\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b((?:Stade|Stadion|Stadio|Estádio|Estadio|Campo|Parque|"
            r"Complexo Desportivo|Centro Desportivo)\s+"
            r"[A-ZÀ-ÖØ-Ý0-9][A-Za-zÀ-ÖØ-öø-ÿ0-9'’.\- ]{2,100})",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b([A-ZÀ-ÖØ-Ý0-9][A-Za-zÀ-ÖØ-öø-ÿ0-9'’.\- ]{2,90}\s+"
            r"(?:Stadium|Arena|Ground|Sports Centre|Sports Center|"
            r"Olympic Centre|Olympic Center))\b",
            re.IGNORECASE,
        ),
    ]

    blocked = (
        "venue pending", "venue unknown", "venue not",
        "stadium not", "to be confirmed", "tbd", "prediction",
        "odds", "h2h", "match preview",
    )
    output: list[str] = []
    seen: set[str] = set()

    for pattern in patterns:
        for match in pattern.finditer(text):
            value = _clean_venue(match.group(1))
            normalized = normalize_text(value)
            if (
                not normalized
                or normalized in seen
                or len(value) < 5
                or len(value) > 120
                or any(item in normalized for item in blocked)
            ):
                continue
            if not any(
                normalize_text(word) in normalized
                for word in VENUE_WORDS
            ):
                continue
            seen.add(normalized)
            output.append(value)
    return output[:20]


def cluster_venue_phrases(
    rows: list[dict[str, Any]],
    *,
    similarity_minimum: float,
) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []

    for row in rows:
        phrase = str(row.get("venue_name") or "").strip()
        domain = str(row.get("domain") or "").strip()
        if not phrase or not domain:
            continue

        selected = None
        for cluster in clusters:
            if text_similarity(
                phrase,
                cluster["venue_name"],
            ) >= similarity_minimum:
                selected = cluster
                break

        if selected is None:
            selected = {
                "venue_name": phrase,
                "aliases": [],
                "domains": set(),
                "official_domains": set(),
                "sources": [],
            }
            clusters.append(selected)

        if len(phrase) > len(selected["venue_name"]):
            selected["venue_name"] = phrase
        if phrase not in selected["aliases"]:
            selected["aliases"].append(phrase)
        selected["domains"].add(domain)
        if row.get("official_like"):
            selected["official_domains"].add(domain)
        selected["sources"].append(row)

    safe: list[dict[str, Any]] = []
    for cluster in clusters:
        safe.append({
            "venue_name": cluster["venue_name"],
            "aliases": cluster["aliases"],
            "distinct_domains": sorted(cluster["domains"]),
            "official_domains": sorted(
                cluster["official_domains"]
            ),
            "sources": cluster["sources"],
        })

    safe.sort(
        key=lambda item: (
            len(item["official_domains"]),
            len(item["distinct_domains"]),
            len(item["sources"]),
        ),
        reverse=True,
    )
    return safe
