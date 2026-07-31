from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Callable

import requests

from .config import settings
from .text_utils import (
    cluster_venue_phrases,
    date_support,
    domain_from_url,
    extract_venue_phrases,
    fixture_supported,
    official_like,
    text_similarity,
)


class TavilyError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        provider_message: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.provider_message = provider_message


class TavilyClient:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_request = 0.0

    def _headers(self) -> dict[str, str]:
        if not settings.tavily_api_key:
            raise TavilyError("TAVILY_API_KEY is not configured.")
        return {
            "Authorization": f"Bearer {settings.tavily_api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _wait(self) -> None:
        elapsed = time.monotonic() - self._last_request
        delay = settings.tavily_min_interval_seconds - elapsed
        if delay > 0:
            time.sleep(delay)

    def _post(
        self,
        endpoint: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        with self._lock:
            self._wait()
            try:
                response = requests.post(
                    f"{settings.tavily_base_url}/{endpoint}",
                    headers=self._headers(),
                    json=payload,
                    timeout=(
                        5,
                        settings.tavily_timeout_seconds,
                    ),
                )
            except requests.RequestException as exc:
                self._last_request = time.monotonic()
                raise TavilyError(
                    "Tavily network request failed.",
                    provider_message=type(exc).__name__,
                ) from exc
            self._last_request = time.monotonic()

        try:
            body = response.json()
        except ValueError as exc:
            raise TavilyError(
                "Tavily returned invalid JSON.",
                status_code=response.status_code,
            ) from exc

        if response.status_code != 200:
            message = None
            if isinstance(body, dict):
                message = str(
                    body.get("detail")
                    or body.get("message")
                    or body.get("error")
                    or ""
                ).strip() or None
            raise TavilyError(
                "Tavily rejected the request.",
                status_code=response.status_code,
                provider_message=message,
            )
        return body if isinstance(body, dict) else {}

    def search(self, query: str) -> dict[str, Any]:
        if not settings.tavily_enabled:
            raise TavilyError("Tavily search is disabled.")
        return self._post(
            "search",
            {
                "query": query,
                "topic": "general",
                "search_depth": settings.tavily_search_depth,
                "max_results": settings.tavily_max_results,
                "include_answer": False,
                "include_images": False,
            },
        )

    def extract(
        self,
        urls: list[str],
        *,
        query: str,
    ) -> dict[str, Any]:
        selected = [
            value for value in dict.fromkeys(urls)
            if value
        ][: settings.tavily_extract_max_urls]
        if not selected or not settings.tavily_extract_enabled:
            return {
                "results": [],
                "failed_results": [],
                "skipped": True,
            }
        return self._post(
            "extract",
            {
                "urls": selected,
                "query": query,
                "extract_depth": settings.tavily_extract_depth,
                "format": "markdown",
                "include_images": False,
            },
        )

    def resolve_fixture_venue(
        self,
        fixture: dict[str, Any],
        *,
        provider_venue: str | None = None,
        progress: Callable[[str, str], None] | None = None,
    ) -> dict[str, Any]:
        kickoff = fixture["kickoff_utc"]
        if not isinstance(kickoff, datetime):
            raise TavilyError("Fixture kickoff is not a datetime.")

        home = str(fixture.get("home_team") or "")
        away = str(fixture.get("away_team") or "")
        competition = str(
            fixture.get("competition_name") or ""
        )
        country = str(
            fixture.get("competition_country") or ""
        )
        query = (
            f'"{home}" "{away}" "{kickoff.date().isoformat()}" '
            f'"{competition}" {country} '
            "official venue stadium ground"
        )
        if progress:
            progress(
                "tavily_search",
                "Searching fixture-specific venue pages.",
            )
        search = self.search(query)
        rows = search.get("results")
        rows = rows if isinstance(rows, list) else []

        preliminary: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or "")
            content = str(row.get("content") or "")
            url = str(row.get("url") or "")
            blob = f"{title}\n{content}\n{url}"
            if not fixture_supported(home, away, blob):
                continue
            preliminary.append({
                "title": title[:300],
                "content": content[:5000],
                "url": url[:1200],
                "domain": domain_from_url(url),
                "score": row.get("score"),
            })

        snippet_phrases: list[dict[str, Any]] = []
        for row in preliminary:
            snippet_blob = (
                f"{row['title']}\n{row['content']}\n{row['url']}"
            )
            snippet_date = date_support(
                kickoff,
                snippet_blob,
                tolerance_days=1,
            )
            if not snippet_date["supported"]:
                continue
            snippet_official = official_like(
                url=row["url"],
                title=row["title"],
                content=snippet_blob,
                home_team=home,
                away_team=away,
            )
            for phrase in extract_venue_phrases(snippet_blob):
                snippet_phrases.append({
                    "venue_name": phrase,
                    "title": row["title"],
                    "url": row["url"],
                    "domain": row["domain"],
                    "official_like": snippet_official,
                    "date_exact": snippet_date["exact"],
                    "date_day_delta": snippet_date["day_delta"],
                })

        snippet_clusters = cluster_venue_phrases(
            snippet_phrases,
            similarity_minimum=settings.venue_similarity_minimum,
        )
        snippet_selected = None
        if provider_venue:
            snippet_matching = [
                cluster for cluster in snippet_clusters
                if text_similarity(
                    provider_venue,
                    cluster["venue_name"],
                ) >= settings.venue_similarity_minimum
            ]
            if snippet_matching:
                snippet_selected = {
                    **snippet_matching[0],
                    "venue_name": provider_venue,
                    "provider_venue_confirmed": True,
                }
        else:
            snippet_eligible = [
                cluster for cluster in snippet_clusters
                if (
                    len(cluster["distinct_domains"])
                    >= settings.tavily_min_distinct_domains
                    or (
                        settings.tavily_require_official_source
                        and len(cluster["official_domains"]) >= 1
                    )
                )
            ]
            if snippet_eligible:
                snippet_selected = snippet_eligible[0]

        if snippet_selected is not None:
            if progress:
                progress(
                    "tavily_search_verified",
                    "Venue was verified from search snippets; "
                    "full-page extraction was not required.",
                )
            return {
                "status": (
                    "provider_venue_confirmed"
                    if provider_venue
                    else "web_venue_consensus"
                ),
                "verified": True,
                "query": query,
                "search_request_id": search.get("request_id"),
                "extract_request_id": None,
                "extract_skipped": True,
                "preliminary_pages": preliminary,
                "fixture_pages": [],
                "venue_clusters": snippet_clusters,
                "selected": snippet_selected,
            }

        if progress:
            progress(
                "tavily_extract",
                "Reading the strongest fixture pages for venue evidence.",
            )
        extract = self.extract(
            [row["url"] for row in preliminary],
            query=(
                f"For {home} vs {away}, extract the exact "
                "match venue or stadium and city."
            ),
        )
        extracted = {
            str(row.get("url") or ""): str(
                row.get("raw_content") or ""
            )
            for row in (
                extract.get("results")
                if isinstance(extract.get("results"), list)
                else []
            )
            if isinstance(row, dict)
        }

        pages: list[dict[str, Any]] = []
        phrases: list[dict[str, Any]] = []
        for row in preliminary:
            raw = extracted.get(row["url"], "")
            blob = (
                f"{row['title']}\n{row['content']}\n"
                f"{raw}\n{row['url']}"
            )
            if not fixture_supported(home, away, blob):
                continue
            date_check = date_support(
                kickoff,
                blob,
                tolerance_days=1,
            )
            if not date_check["supported"]:
                continue

            is_official = official_like(
                url=row["url"],
                title=row["title"],
                content=blob,
                home_team=home,
                away_team=away,
            )
            venue_phrases = extract_venue_phrases(blob)
            page = {
                "title": row["title"],
                "url": row["url"],
                "domain": row["domain"],
                "official_like": is_official,
                "date_exact": date_check["exact"],
                "date_day_delta": date_check["day_delta"],
                "venue_phrases": venue_phrases,
            }
            pages.append(page)
            for phrase in venue_phrases:
                phrases.append({
                    "venue_name": phrase,
                    "title": row["title"],
                    "url": row["url"],
                    "domain": row["domain"],
                    "official_like": is_official,
                    "date_exact": date_check["exact"],
                    "date_day_delta": date_check["day_delta"],
                })

        clusters = cluster_venue_phrases(
            phrases,
            similarity_minimum=settings.venue_similarity_minimum,
        )

        selected = None
        status = "venue_not_published"

        if provider_venue:
            matching = [
                cluster for cluster in clusters
                if text_similarity(
                    provider_venue,
                    cluster["venue_name"],
                ) >= settings.venue_similarity_minimum
            ]
            if matching:
                selected = matching[0]
                selected = {
                    **selected,
                    "venue_name": provider_venue,
                    "provider_venue_confirmed": True,
                }
                status = "provider_venue_confirmed"
        else:
            eligible = [
                cluster for cluster in clusters
                if (
                    len(cluster["distinct_domains"])
                    >= settings.tavily_min_distinct_domains
                    or (
                        settings.tavily_require_official_source
                        and len(cluster["official_domains"]) >= 1
                    )
                )
            ]
            if eligible:
                selected = eligible[0]
                status = "web_venue_consensus"

        return {
            "status": status,
            "verified": selected is not None,
            "query": query,
            "search_request_id": search.get("request_id"),
            "extract_request_id": extract.get("request_id"),
            "preliminary_pages": preliminary,
            "fixture_pages": pages,
            "venue_clusters": clusters,
            "selected": selected,
        }


client = TavilyClient()
