from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

import pytest
from airflow.exceptions import AirflowSkipException

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import chicago_crimes_pipeline
import pipeline_utils


class RecordingHook:
    def __init__(self):
        self.statements: list[str] = []

    def run(self, sql: str):
        self.statements.append(" ".join(sql.split()))


def test_build_recent_where_clause_uses_expected_window():
    where_clause = pipeline_utils.build_recent_where_clause(
        days=7,
        now=datetime(2026, 3, 10, 12, 0, 0),
    )

    assert where_clause == "date > '2026-03-03T12:00:00'"


def test_collect_crimes_data_paginates_until_partial_batch():
    calls: list[tuple[int, int, str | None]] = []

    def fake_fetch(limit: int, offset: int = 0, where_clause: str | None = None) -> list[dict]:
        calls.append((limit, offset, where_clause))
        batches = {
            0: [{"id": "1"}, {"id": "2"}],
            2: [{"id": "3"}],
        }
        return batches.get(offset, [])

    rows = pipeline_utils.collect_crimes_data(
        batch_size=2,
        where_clause="date > '2026-03-03T12:00:00'",
        fetcher=fake_fetch,
    )

    assert rows == [{"id": "1"}, {"id": "2"}, {"id": "3"}]
    assert calls == [
        (2, 0, "date > '2026-03-03T12:00:00'"),
        (2, 2, "date > '2026-03-03T12:00:00'"),
    ]


def test_collect_crimes_data_returns_empty_when_first_batch_is_empty():
    def fake_fetch(limit: int, offset: int = 0, where_clause: str | None = None) -> list[dict]:
        return []

    rows = pipeline_utils.collect_crimes_data(
        batch_size=10,
        where_clause="date > '2026-03-03T12:00:00'",
        fetcher=fake_fetch,
    )

    assert rows == []


def test_build_upsert_sql_does_not_update_conflict_key():
    sql = pipeline_utils.build_upsert_sql(
        "raw_staging",
        "raw",
        ["id", "case_number", "date"],
    )

    assert "ON CONFLICT (id)" in sql
    assert "id = EXCLUDED.id" not in sql
    assert "case_number = EXCLUDED.case_number" in sql


def test_prepare_pipeline_tables_creates_prod_and_custom_staging_tables():
    hook = RecordingHook()

    pipeline_utils.prepare_pipeline_tables(
        hook,
        raw_staging_table="raw_bootstrap_staging",
        transformed_staging_table="transformed_bootstrap_staging",
    )

    assert len(hook.statements) == 4
    assert "CREATE TABLE IF NOT EXISTS chicago_crimes_raw" in hook.statements[0]
    assert "CREATE TABLE IF NOT EXISTS chicago_crimes_transformed" in hook.statements[1]
    assert "DROP TABLE IF EXISTS raw_bootstrap_staging" in hook.statements[2]
    assert "LIKE chicago_crimes_raw INCLUDING DEFAULTS INCLUDING CONSTRAINTS" in hook.statements[2]
    assert "DROP TABLE IF EXISTS transformed_bootstrap_staging" in hook.statements[3]
    assert "LIKE chicago_crimes_transformed INCLUDING DEFAULTS INCLUDING CONSTRAINTS" in hook.statements[3]


def test_upsert_staging_to_production_uses_given_staging_tables():
    hook = RecordingHook()

    pipeline_utils.upsert_staging_to_production(
        hook,
        raw_staging_table="raw_bootstrap_staging",
        transformed_staging_table="transformed_bootstrap_staging",
    )

    assert len(hook.statements) == 2
    assert "FROM raw_bootstrap_staging" in hook.statements[0]
    assert "INSERT INTO chicago_crimes_raw" in hook.statements[0]
    assert "FROM transformed_bootstrap_staging" in hook.statements[1]
    assert "INSERT INTO chicago_crimes_transformed" in hook.statements[1]


def test_daily_raw_quality_check_skips_when_ingestion_is_empty():
    dag = chicago_crimes_pipeline.chicago_crimes_daily()
    raw_check = dag.task_dict["soda_check_raw"]

    with pytest.raises(AirflowSkipException):
        raw_check.python_callable(0)
