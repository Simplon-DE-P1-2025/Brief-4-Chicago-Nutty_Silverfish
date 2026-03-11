from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import pipeline_utils


class RecordingHook:
    """Enregistre les requetes SQL sans les executer."""

    def __init__(self):
        self.statements: list[str] = []
        self._count = 42

    def run(self, sql: str):
        self.statements.append(" ".join(sql.split()))

    def get_first(self, sql: str):
        self.statements.append(" ".join(sql.split()))
        return (self._count,)


def test_transform_raw_to_table_sql_generates_geo_filter():
    hook = RecordingHook()
    hook._count = 100

    pipeline_utils.transform_raw_to_table_sql(
        hook,
        source_table="chicago_crimes_raw",
        target_table="chicago_crimes_transformed_v2",
        geo_filter=True,
        future_date_filter=False,
        truncate_target=False,
    )

    insert_sql = hook.statements[0]
    assert "BETWEEN 41.6 AND 42.1" in insert_sql
    assert "BETWEEN -87.95 AND -87.5" in insert_sql


def test_transform_raw_to_table_sql_generates_future_date_filter():
    hook = RecordingHook()
    hook._count = 100

    pipeline_utils.transform_raw_to_table_sql(
        hook,
        source_table="chicago_crimes_raw",
        target_table="chicago_crimes_transformed_v2",
        geo_filter=False,
        future_date_filter=True,
        truncate_target=False,
    )

    insert_sql = hook.statements[0]
    assert "<= NOW()" in insert_sql


def test_transform_raw_to_table_sql_without_geo_filter():
    hook = RecordingHook()
    hook._count = 100

    pipeline_utils.transform_raw_to_table_sql(
        hook,
        source_table="chicago_crimes_raw",
        target_table="chicago_crimes_transformed_v2",
        geo_filter=False,
        future_date_filter=False,
        truncate_target=False,
    )

    insert_sql = hook.statements[0]
    assert "BETWEEN 41.6 AND 42.1" not in insert_sql
    assert "BETWEEN -87.95 AND -87.5" not in insert_sql


def test_transform_raw_to_table_sql_truncates_target():
    hook = RecordingHook()
    hook._count = 100

    pipeline_utils.transform_raw_to_table_sql(
        hook,
        source_table="chicago_crimes_raw",
        target_table="chicago_crimes_transformed_v2",
        truncate_target=True,
    )

    assert "TRUNCATE TABLE chicago_crimes_transformed_v2" in hook.statements[0]


def test_create_sandbox_table_uses_transformed_schema():
    hook = RecordingHook()

    pipeline_utils.create_sandbox_table(hook, "chicago_crimes_transformed_v2")

    assert len(hook.statements) == 1
    assert "CREATE TABLE IF NOT EXISTS chicago_crimes_transformed_v2" in hook.statements[0]
    assert "crime_date TIMESTAMP" in hook.statements[0]
    assert "latitude DOUBLE PRECISION" in hook.statements[0]
