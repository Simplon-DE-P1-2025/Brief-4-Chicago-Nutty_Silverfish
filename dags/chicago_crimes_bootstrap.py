"""DAG bootstrap Chicago Crimes — chargement initial complet par batch de 50k.

Utilise le Dynamic Task Mapping pour que chaque batch soit une tache
visible dans l'UI Airflow. Si un batch echoue, seul celui-la est retry.
"""

from __future__ import annotations

from datetime import datetime
import logging
import os
import sys

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(__file__))

from pipeline_utils import (
    CHICAGO_API_URL,
    API_BATCH_SIZE,
    POSTGRES_CONN_ID,
    RAW_TABLE,
    TRANSFORMED_TABLE,
    fetch_crimes_data,
    load_raw_to_staging,
    prepare_pipeline_tables,
    run_soda_scan,
    staging_table_name,
    transform_and_load,
    upsert_staging_to_production,
)

logger = logging.getLogger(__name__)
BOOTSTRAP_RAW_STAGING = staging_table_name(f"{RAW_TABLE}_bootstrap")
BOOTSTRAP_TRANSFORMED_STAGING = staging_table_name(f"{TRANSFORMED_TABLE}_bootstrap")


@dag(
    dag_id="chicago_crimes_bootstrap",
    description="Chargement initial complet Chicago Crimes — trigger manuel",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dataops", "chicago", "soda", "bootstrap"],
    default_args={"owner": "dataops-team", "retries": 2},
)
def chicago_crimes_bootstrap():

    @task()
    def create_tables():
        """Cree les tables production et staging."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        prepare_pipeline_tables(
            hook,
            raw_staging_table=BOOTSTRAP_RAW_STAGING,
            transformed_staging_table=BOOTSTRAP_TRANSFORMED_STAGING,
        )

    @task()
    def generate_offsets() -> list[int]:
        """Interroge l'API pour connaitre le nombre total de lignes et genere la liste des offsets."""
        response = requests.get(
            CHICAGO_API_URL,
            params={"$select": "count(*) as total"},
            timeout=120,
        )
        response.raise_for_status()
        total = int(response.json()[0]["total"])
        offsets = list(range(0, total, API_BATCH_SIZE))
        logger.info("Total API: %s lignes -> %s batchs de %s", total, len(offsets), API_BATCH_SIZE)
        return offsets

    @task(max_active_tis_per_dagrun=1)
    def process_batch(offset: int):
        """Traite un batch : ingestion -> soda raw -> transfo -> soda transformed -> UPSERT."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        data = fetch_crimes_data(API_BATCH_SIZE, offset=offset)
        if not data:
            logger.info("Batch offset=%s vide, rien a faire", offset)
            return {"offset": offset, "raw": 0, "transformed": 0}

        df = pd.DataFrame(data)

        # 1. Staging raw + Soda check
        count = load_raw_to_staging(
            df,
            hook,
            engine,
            staging_table=BOOTSTRAP_RAW_STAGING,
        )
        run_soda_scan("bootstrap_raw_data_checks.yml", f"bootstrap_raw_offset_{offset}")

        # 2. Transformation + Soda check
        transformed_count = transform_and_load(
            hook,
            engine,
            raw_staging_table=BOOTSTRAP_RAW_STAGING,
            transformed_staging_table=BOOTSTRAP_TRANSFORMED_STAGING,
        )
        run_soda_scan(
            "bootstrap_transformed_data_checks.yml",
            f"bootstrap_transformed_offset_{offset}",
        )

        # 3. UPSERT en production
        upsert_staging_to_production(
            hook,
            raw_staging_table=BOOTSTRAP_RAW_STAGING,
            transformed_staging_table=BOOTSTRAP_TRANSFORMED_STAGING,
        )

        logger.info(
            "Batch offset=%s termine: %s raw, %s transformed -> UPSERT OK",
            offset, count, transformed_count,
        )
        return {"offset": offset, "raw": count, "transformed": transformed_count}

    tables = create_tables()
    offsets = generate_offsets()
    batches = process_batch.expand(offset=offsets)

    tables >> offsets >> batches


chicago_crimes_bootstrap()
