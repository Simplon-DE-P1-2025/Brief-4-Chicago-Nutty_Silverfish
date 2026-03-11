"""Pipeline Chicago Crimes quotidien — recupere les 14 derniers jours, transforme, et UPSERT en production."""

from __future__ import annotations

from datetime import datetime
import logging
import os
import sys

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

from pipeline_utils import (
    API_BATCH_SIZE,
    POSTGRES_CONN_ID,
    RECENT_WINDOW_DAYS,
    build_recent_where_clause,
    collect_crimes_data,
    fetch_crimes_data,
    load_raw_to_staging,
    prepare_pipeline_tables,
    run_soda_scan,
    transform_and_load,
    upsert_staging_to_production,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="chicago_crimes_daily",
    description="Pipeline quotidien Chicago Crimes — 14 derniers jours + UPSERT",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dataops", "chicago", "soda", "daily"],
    default_args={"owner": "dataops-team", "retries": 2},
)
def chicago_crimes_daily():

    @task()
    def create_tables():
        """Cree les tables production et staging."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        prepare_pipeline_tables(hook)

    @task()
    def ingest_data():
        """Recupere les 14 derniers jours depuis l'API avec pagination et charge en staging."""
        where_clause = build_recent_where_clause(days=RECENT_WINDOW_DAYS)
        logger.info(
            "Ingestion quotidienne avec batch_size=%s et filtre=%s",
            API_BATCH_SIZE,
            where_clause,
        )

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()
        all_data = collect_crimes_data(
            API_BATCH_SIZE,
            where_clause=where_clause,
            fetcher=fetch_crimes_data,
        )

        if not all_data:
            logger.warning(
                "Aucune donnee recuperee depuis l'API Chicago pour la fenetre %s. "
                "Le reste du DAG sera ignore.",
                where_clause,
            )
            return 0

        df = pd.DataFrame(all_data)
        count = load_raw_to_staging(df, hook, engine)
        logger.info("Ingestion quotidienne: %s lignes chargees en staging", count)
        return count

    @task()
    def soda_check_raw(row_count: int):
        """Controle qualite sur la table brute de staging."""
        if row_count == 0:
            raise AirflowSkipException("Aucune donnee brute recente a valider")
        logger.info("Controle Soda sur %s lignes brutes", row_count)
        return run_soda_scan("raw_data_checks.yml", "raw_quality_check")

    @task()
    def transform_data():
        """Transforme les donnees brutes et charge la table transformee en staging."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()
        return transform_and_load(hook, engine)

    @task()
    def soda_check_transformed(transformed_count: int):
        """Controle qualite sur la table transformee de staging."""
        logger.info("Controle Soda sur %s lignes transformees", transformed_count)
        return run_soda_scan("transformed_data_checks.yml", "transformed_quality_check")

    @task()
    def upsert_production():
        """UPSERT des donnees staging vers les tables production."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        upsert_staging_to_production(hook)

    tables = create_tables()
    rows = ingest_data()
    raw_ok = soda_check_raw(rows)
    transformed = transform_data()
    transformed_ok = soda_check_transformed(transformed)
    upsert = upsert_production()

    tables >> rows >> raw_ok >> transformed >> transformed_ok >> upsert


chicago_crimes_daily()
