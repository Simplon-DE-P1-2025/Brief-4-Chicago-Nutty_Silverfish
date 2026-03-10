"""DAG sandbox Chicago Crimes -- re-transformation amelioree des donnees brutes existantes.

Lit directement depuis chicago_crimes_raw (production) et ecrit dans
chicago_crimes_transformed_v2. Permet de comparer les resultats avec
la table transformee actuelle avant de basculer.
"""

from __future__ import annotations

from datetime import datetime
import logging
import os
import sys

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.dirname(__file__))

from pipeline_utils import (
    POSTGRES_CONN_ID,
    RAW_TABLE,
    TRANSFORMED_TABLE,
    SANDBOX_TABLE,
    create_sandbox_table,
    run_soda_scan,
    transform_raw_to_table_sql,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="chicago_crimes_sandbox",
    description="Sandbox: re-transformation amelioree depuis chicago_crimes_raw",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dataops", "chicago", "soda", "sandbox"],
    default_args={"owner": "dataops-team", "retries": 1},
)
def chicago_crimes_sandbox():

    @task()
    def setup_sandbox_table():
        """Cree la table sandbox si elle n'existe pas."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        create_sandbox_table(hook)

    @task()
    def transform_raw_to_sandbox():
        """Transforme chicago_crimes_raw vers la table sandbox en SQL pur."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        count = transform_raw_to_table_sql(
            hook,
            source_table=RAW_TABLE,
            target_table=SANDBOX_TABLE,
            geo_filter=True,
            future_date_filter=True,
            truncate_target=True,
        )
        logger.info("Sandbox: %s lignes transformees dans %s", count, SANDBOX_TABLE)
        return count

    @task()
    def soda_check_sandbox(row_count: int):
        """Controle qualite sur la table sandbox."""
        logger.info("Controle Soda sandbox sur %s lignes", row_count)
        return run_soda_scan("sandbox_transformed_checks.yml", "sandbox_quality_check")

    @task()
    def compare_results():
        """Compare la table sandbox avec la table transformee actuelle."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        comparison_sql = f"""
            SELECT 'original' AS source, COUNT(*) AS total FROM {TRANSFORMED_TABLE}
            UNION ALL
            SELECT 'sandbox' AS source, COUNT(*) AS total FROM {SANDBOX_TABLE};
        """
        results = hook.get_records(comparison_sql)
        for source, total in results:
            logger.info("Comparaison: %s = %s lignes", source, total)

        geo_outliers_sql = f"""
            SELECT COUNT(*) FROM {TRANSFORMED_TABLE}
            WHERE latitude NOT BETWEEN 41.6 AND 42.1
               OR longitude NOT BETWEEN -87.95 AND -87.5;
        """
        outliers = hook.get_first(geo_outliers_sql)[0]
        logger.info("Lignes hors Chicago dans l'original: %s", outliers)

        future_sql = f"""
            SELECT COUNT(*) FROM {TRANSFORMED_TABLE}
            WHERE crime_date > NOW();
        """
        future_count = hook.get_first(future_sql)[0]
        logger.info("Lignes avec date future dans l'original: %s", future_count)

        return {
            "original_vs_sandbox": {r[0]: r[1] for r in results},
            "geo_outliers_in_original": outliers,
            "future_dates_in_original": future_count,
        }

    t_setup = setup_sandbox_table()
    t_transform = transform_raw_to_sandbox()
    t_soda = soda_check_sandbox(t_transform)
    t_compare = compare_results()

    t_setup >> t_transform >> t_soda >> t_compare


chicago_crimes_sandbox()
