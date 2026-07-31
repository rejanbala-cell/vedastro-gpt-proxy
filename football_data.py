from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None
    dict_row = None

from .config import settings


class DatabaseUnavailable(RuntimeError):
    pass


@contextmanager
def connect(*, dict_rows: bool = False) -> Iterator:
    if psycopg is None:
        raise DatabaseUnavailable("psycopg is not installed.")
    if not settings.database_url:
        raise DatabaseUnavailable("DATABASE_URL is not configured.")

    kwargs = {}
    if dict_rows:
        kwargs["row_factory"] = dict_row

    connection = psycopg.connect(
        settings.database_url,
        connect_timeout=10,
        **kwargs,
    )
    try:
        yield connection
    finally:
        connection.close()


def ensure_schema() -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS predict2_metadata (
            metadata_key TEXT PRIMARY KEY,
            metadata_value TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fixtures (
            id BIGSERIAL PRIMARY KEY,
            provider TEXT NOT NULL,
            provider_fixture_id TEXT NOT NULL,
            sport TEXT NOT NULL DEFAULT 'soccer',
            competition_name TEXT NOT NULL,
            competition_country TEXT,
            season TEXT,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            kickoff_utc TIMESTAMPTZ NOT NULL,
            venue_name TEXT,
            venue_city TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone_name TEXT,
            fixture_status TEXT NOT NULL DEFAULT 'scheduled',
            neutral_venue BOOLEAN NOT NULL DEFAULT FALSE,
            raw_fixture_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (provider, provider_fixture_id)
        )
        """,
        """
        ALTER TABLE fixtures
            ADD COLUMN IF NOT EXISTS location_source TEXT
        """,
        """
        ALTER TABLE fixtures
            ADD COLUMN IF NOT EXISTS location_confidence NUMERIC(6,3)
        """,
        """
        ALTER TABLE fixtures
            ADD COLUMN IF NOT EXISTS location_verified_at TIMESTAMPTZ
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fixtures_kickoff_utc
        ON fixtures (kickoff_utc)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fixtures_provider_kickoff
        ON fixtures (provider, kickoff_utc)
        """,
        """
        CREATE TABLE IF NOT EXISTS predict2_venue_attempts (
            id BIGSERIAL PRIMARY KEY,
            job_id TEXT NOT NULL,
            fixture_id BIGINT NOT NULL
                REFERENCES fixtures(id) ON DELETE CASCADE,
            status TEXT NOT NULL,
            stage TEXT NOT NULL,
            venue_name TEXT,
            venue_city TEXT,
            country TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone_name TEXT,
            confidence NUMERIC(6,3),
            source TEXT,
            audit_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ,
            UNIQUE (job_id, fixture_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_predict2_venue_attempts_fixture
        ON predict2_venue_attempts (fixture_id, completed_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS predict2_geocode_cache (
            cache_key TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            query_text TEXT NOT NULL,
            result_json JSONB NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS predict2_venue_jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            reason TEXT NOT NULL,
            fixture_id BIGINT
                REFERENCES fixtures(id) ON DELETE SET NULL,
            window_days INTEGER NOT NULL,
            limit_count INTEGER NOT NULL,
            fixtures_total INTEGER NOT NULL DEFAULT 0,
            fixtures_completed INTEGER NOT NULL DEFAULT 0,
            verified INTEGER NOT NULL DEFAULT 0,
            unresolved INTEGER NOT NULL DEFAULT 0,
            skipped INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0,
            current_fixture JSONB,
            current_stage TEXT,
            stage_started_at TIMESTAMPTZ,
            last_progress_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            message TEXT,
            error_type TEXT,
            traceback_json JSONB,
            results_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS
            idx_predict2_venue_jobs_one_active
        ON predict2_venue_jobs ((1))
        WHERE status IN ('queued', 'running')
        """,
        """
        CREATE INDEX IF NOT EXISTS
            idx_predict2_venue_jobs_created
        ON predict2_venue_jobs (created_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS predict2_prediction_runs (
            id BIGSERIAL PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE,
            fixture_id BIGINT NOT NULL
                REFERENCES fixtures(id) ON DELETE CASCADE,
            model_version TEXT NOT NULL,
            status TEXT NOT NULL,
            requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ,
            outcome TEXT,
            outcome_label TEXT,
            confidence TEXT,
            eligibility TEXT,
            method TEXT,
            favourite_side TEXT,
            favourite_team TEXT,
            underdog_team TEXT,
            market_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            performance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            venue_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            chart_json JSONB,
            decision_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            diagnostic_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            error_type TEXT,
            error_message TEXT,
            UNIQUE (fixture_id, model_version)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS
            idx_predict2_prediction_runs_fixture
        ON predict2_prediction_runs (fixture_id, requested_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS predict2_official_results (
            fixture_id BIGINT PRIMARY KEY
                REFERENCES fixtures(id) ON DELETE CASCADE,
            home_score INTEGER,
            away_score INTEGER,
            outcome TEXT,
            source TEXT NOT NULL,
            raw_result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS predict2_prediction_audits (
            id BIGSERIAL PRIMARY KEY,
            prediction_id BIGINT NOT NULL
                REFERENCES predict2_prediction_runs(id) ON DELETE CASCADE,
            fixture_id BIGINT NOT NULL
                REFERENCES fixtures(id) ON DELETE CASCADE,
            predicted_outcome TEXT NOT NULL,
            actual_outcome TEXT,
            correct BOOLEAN,
            market_baseline_outcome TEXT,
            market_baseline_correct BOOLEAN,
            audited_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (prediction_id)
        )
        """,
    ]

    with connect() as connection:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        connection.commit()
