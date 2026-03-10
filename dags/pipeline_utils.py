"""Fonctions utilitaires partagees par les pipelines Chicago Crimes."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
import logging
import os

from airflow.sdk.bases.hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests

logger = logging.getLogger(__name__)

STAGING_SUFFIX = "_staging"

# --- Constantes partagees ---
CHICAGO_API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
API_BATCH_SIZE = 50000
RECENT_WINDOW_DAYS = 14
POSTGRES_CONN_ID = "chicago_crimes_db"
SODA_CONFIG_PATH = "/usr/local/airflow/include/soda/configuration.yml"
SODA_CHECKS_DIR = "/usr/local/airflow/include/soda/checks"
RAW_TABLE = "chicago_crimes_raw"
TRANSFORMED_TABLE = "chicago_crimes_transformed"

RAW_COLUMNS = [
    "id", "case_number", "date", "block", "iucr", "primary_type",
    "description", "location_description", "arrest", "domestic",
    "beat", "district", "ward", "community_area", "fbi_code",
    "x_coordinate", "y_coordinate", "year", "updated_on",
    "latitude", "longitude",
]

TRANSFORMED_COLUMNS = [
    "id", "case_number", "crime_date", "crime_year", "crime_month",
    "crime_day_of_week", "block", "primary_type", "description",
    "location_description", "arrest", "domestic", "district", "ward",
    "community_area", "latitude", "longitude",
]

RAW_TABLE_SCHEMA = """
    id VARCHAR(50) PRIMARY KEY,
    case_number VARCHAR(20),
    date VARCHAR(50),
    block VARCHAR(100),
    iucr VARCHAR(10),
    primary_type VARCHAR(100),
    description VARCHAR(255),
    location_description VARCHAR(100),
    arrest VARCHAR(10),
    domestic VARCHAR(10),
    beat VARCHAR(10),
    district VARCHAR(10),
    ward VARCHAR(10),
    community_area VARCHAR(10),
    fbi_code VARCHAR(10),
    x_coordinate VARCHAR(20),
    y_coordinate VARCHAR(20),
    year VARCHAR(10),
    updated_on VARCHAR(50),
    latitude VARCHAR(30),
    longitude VARCHAR(30)
""".strip()

TRANSFORMED_TABLE_SCHEMA = """
    id VARCHAR(50) PRIMARY KEY,
    case_number VARCHAR(20),
    crime_date TIMESTAMP,
    crime_year INTEGER,
    crime_month INTEGER,
    crime_day_of_week INTEGER,
    block VARCHAR(100),
    primary_type VARCHAR(100),
    description VARCHAR(255),
    location_description VARCHAR(100),
    arrest BOOLEAN,
    domestic BOOLEAN,
    district VARCHAR(10),
    ward VARCHAR(10),
    community_area VARCHAR(10),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
""".strip()


def staging_table_name(table_name: str) -> str:
    """Ajoute le suffixe _staging a un nom de table."""
    return f"{table_name}{STAGING_SUFFIX}"


RAW_STAGING = staging_table_name(RAW_TABLE)
TRANSFORMED_STAGING = staging_table_name(TRANSFORMED_TABLE)


# --- Fonctions SQL ---

def build_soda_environment(connection) -> dict[str, str]:
    """Convertit une connexion Airflow en variables d'environnement pour Soda."""
    return {
        "POSTGRES_HOST": connection.host or "",
        "POSTGRES_PORT": str(connection.port or 5432),
        "POSTGRES_USER": connection.login or "",
        "POSTGRES_PASSWORD": connection.password or "",
        "POSTGRES_DB": connection.schema or "",
    }


def normalize_boolean_field(value, field_name: str) -> bool:
    """Convertit une valeur true/false en booleen Python."""
    if isinstance(value, bool):
        return value
    if value is None:
        raise ValueError(f"{field_name}: valeur manquante")
    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{field_name}: valeur invalide {value!r}")


def build_promote_sql(table_pairs: Sequence[tuple[str, str]]) -> str:
    """Genere le SQL de promotion staging -> production dans une transaction."""
    statements = ["BEGIN;"]
    for target, staging in table_pairs:
        statements.append(f"TRUNCATE TABLE {target};")
        statements.append(f"INSERT INTO {target} SELECT * FROM {staging};")
    statements.append("COMMIT;")
    return "\n".join(statements)


def build_upsert_sql(
    staging: str, target: str, columns: list[str], conflict_key: str = "id"
) -> str:
    """Genere le SQL d'UPSERT staging -> production (INSERT ... ON CONFLICT DO UPDATE)."""
    cols = ", ".join(columns)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != conflict_key)
    return (
        f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging} "
        f"ON CONFLICT ({conflict_key}) DO UPDATE SET {updates};"
    )


def create_table_if_missing(hook: PostgresHook, table_name: str, schema_sql: str) -> None:
    """Cree une table PostgreSQL si elle n'existe pas deja."""
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema_sql}
        );
        """
    )


def recreate_staging_table(hook: PostgresHook, target_table: str, staging_table: str) -> None:
    """Recree une table de staging a partir de la table cible."""
    hook.run(
        f"""
        DROP TABLE IF EXISTS {staging_table};
        CREATE TABLE {staging_table}
        (LIKE {target_table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
        """
    )


def prepare_pipeline_tables(
    hook: PostgresHook,
    raw_staging_table: str = RAW_STAGING,
    transformed_staging_table: str = TRANSFORMED_STAGING,
) -> None:
    """Cree les tables de prod puis recree les tables de staging du pipeline."""
    create_table_if_missing(hook, RAW_TABLE, RAW_TABLE_SCHEMA)
    create_table_if_missing(hook, TRANSFORMED_TABLE, TRANSFORMED_TABLE_SCHEMA)
    recreate_staging_table(hook, RAW_TABLE, raw_staging_table)
    recreate_staging_table(hook, TRANSFORMED_TABLE, transformed_staging_table)
    logger.info(
        "Tables preparees: %s, %s, %s, %s",
        RAW_TABLE,
        TRANSFORMED_TABLE,
        raw_staging_table,
        transformed_staging_table,
    )


def upsert_staging_to_production(
    hook: PostgresHook,
    raw_staging_table: str = RAW_STAGING,
    transformed_staging_table: str = TRANSFORMED_STAGING,
) -> None:
    """UPSERT les tables de staging vers les tables finales."""
    hook.run(build_upsert_sql(raw_staging_table, RAW_TABLE, RAW_COLUMNS))
    hook.run(build_upsert_sql(transformed_staging_table, TRANSFORMED_TABLE, TRANSFORMED_COLUMNS))
    logger.info(
        "UPSERT production termine pour %s et %s depuis %s et %s",
        RAW_TABLE,
        TRANSFORMED_TABLE,
        raw_staging_table,
        transformed_staging_table,
    )


# --- Fonctions d'ingestion et transformation ---

def build_recent_where_clause(days: int = RECENT_WINDOW_DAYS, now: datetime | None = None) -> str:
    """Construit la clause SoQL pour ne garder qu'une fenetre recente."""
    reference = now or datetime.now()
    window_start = (reference - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    return f"date > '{window_start}'"


def collect_crimes_data(
    batch_size: int,
    where_clause: str | None = None,
    fetcher: Callable[..., list[dict]] | None = None,
) -> list[dict]:
    """Recupere tous les batchs disponibles via pagination."""
    fetch = fetcher or fetch_crimes_data
    all_data: list[dict] = []
    offset = 0

    while True:
        data = fetch(batch_size, offset=offset, where_clause=where_clause)
        logger.info(
            "API Chicago batch recu: offset=%s, lignes=%s, filtre=%s",
            offset,
            len(data),
            where_clause or "<none>",
        )
        if not data:
            break
        all_data.extend(data)
        if len(data) < batch_size:
            break
        offset += batch_size

    return all_data


def fetch_crimes_data(limit: int, offset: int = 0, where_clause: str | None = None) -> list[dict]:
    """Recupere les crimes depuis l'API Chicago avec pagination et filtre optionnel."""
    params: dict[str, str | int] = {
        "$limit": limit,
        "$offset": offset,
        "$order": ":id",
    }
    if where_clause:
        params["$where"] = where_clause
    response = requests.get(CHICAGO_API_URL, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


def run_soda_scan(check_file: str, scan_name: str):
    """Execute un scan Soda sur la base cible."""
    from soda.scan import Scan

    connection = BaseHook.get_connection(POSTGRES_CONN_ID)
    os.environ.update(build_soda_environment(connection))

    scan = Scan()
    scan.set_scan_definition_name(scan_name)
    scan.set_data_source_name("chicago_crimes")
    scan.add_configuration_yaml_file(SODA_CONFIG_PATH)
    scan.add_sodacl_yaml_file(os.path.join(SODA_CHECKS_DIR, check_file))
    scan.execute()

    results = scan.get_scan_results()
    logger.info("Soda scan %s termine: %s", scan_name, results)
    if scan.has_check_fails():
        raise ValueError(f"Soda scan {scan_name} echoue: {scan.get_checks_fail()}")
    return results


def load_raw_to_staging(
    df: pd.DataFrame,
    hook: PostgresHook,
    engine,
    staging_table: str = RAW_STAGING,
):
    """Charge un DataFrame brut dans la table de staging raw."""
    df = df[[col for col in RAW_COLUMNS if col in df.columns]]
    hook.run(f"TRUNCATE TABLE {staging_table};")
    with engine.begin() as conn:
        df.to_sql(
            staging_table, conn, if_exists="append", index=False,
            method="multi", chunksize=1000,
        )
    logger.info("%s lignes inserees dans %s", len(df), staging_table)
    return len(df)


def transform_and_load(
    hook: PostgresHook,
    engine,
    raw_staging_table: str = RAW_STAGING,
    transformed_staging_table: str = TRANSFORMED_STAGING,
) -> int:
    """Transforme les donnees brutes et charge la table transformee en staging."""
    df = pd.read_sql(f"SELECT * FROM {raw_staging_table}", engine)
    if df.empty:
        raise ValueError("Aucune donnee brute disponible pour la transformation")

    df = df.dropna(subset=["latitude", "longitude", "primary_type"])
    df["crime_date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce")
    df = df.dropna(subset=["crime_date"])
    df["crime_year"] = df["crime_date"].dt.year
    df["crime_month"] = df["crime_date"].dt.month
    df["crime_day_of_week"] = df["crime_date"].dt.dayofweek

    for field in ["arrest", "domestic"]:
        df[field] = df[field].map(lambda value, f=field: normalize_boolean_field(value, f))

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])

    transformed_df = df[TRANSFORMED_COLUMNS].copy()
    if transformed_df.empty:
        raise ValueError("Aucune donnee exploitable apres transformation")

    hook.run(f"TRUNCATE TABLE {transformed_staging_table};")
    with engine.begin() as conn:
        transformed_df.to_sql(
            transformed_staging_table, conn, if_exists="append", index=False,
            method="multi", chunksize=1000,
        )
    logger.info("%s lignes transformees chargees dans %s", len(transformed_df), transformed_staging_table)
    return len(transformed_df)


# --- Sandbox : transformation SQL pure (pas de pandas) ---

SANDBOX_TABLE = "chicago_crimes_transformed_v2"


def create_sandbox_table(hook: PostgresHook, table_name: str = SANDBOX_TABLE) -> None:
    """Cree la table sandbox si elle n'existe pas."""
    create_table_if_missing(hook, table_name, TRANSFORMED_TABLE_SCHEMA)


def transform_raw_to_table_sql(
    hook: PostgresHook,
    source_table: str = RAW_TABLE,
    target_table: str = SANDBOX_TABLE,
    geo_filter: bool = True,
    future_date_filter: bool = True,
    truncate_target: bool = True,
) -> int:
    """Transforme les donnees brutes vers une table cible entierement en SQL.

    Contrairement a transform_and_load qui utilise pandas, cette fonction
    effectue toute la transformation cote PostgreSQL. Elle gere des millions
    de lignes sans consommation memoire Python.
    """
    cols = ", ".join(TRANSFORMED_COLUMNS)

    where_clauses = [
        "latitude IS NOT NULL",
        "longitude IS NOT NULL",
        "primary_type IS NOT NULL",
        "date IS NOT NULL",
        "LOWER(TRIM(COALESCE(arrest, ''))) IN ('true', 'false')",
        "LOWER(TRIM(COALESCE(domestic, ''))) IN ('true', 'false')",
    ]

    if geo_filter:
        where_clauses.append("latitude::DOUBLE PRECISION BETWEEN 41.6 AND 42.1")
        where_clauses.append("longitude::DOUBLE PRECISION BETWEEN -87.95 AND -87.5")

    if future_date_filter:
        where_clauses.append("date::TIMESTAMP <= NOW()")

    where_sql = " AND ".join(where_clauses)

    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in TRANSFORMED_COLUMNS if c != "id"
    )

    sql = f"""
        INSERT INTO {target_table} ({cols})
        SELECT
            id,
            case_number,
            date::TIMESTAMP AS crime_date,
            EXTRACT(YEAR FROM date::TIMESTAMP)::INTEGER AS crime_year,
            EXTRACT(MONTH FROM date::TIMESTAMP)::INTEGER AS crime_month,
            EXTRACT(DOW FROM date::TIMESTAMP)::INTEGER AS crime_day_of_week,
            block,
            primary_type,
            description,
            location_description,
            CASE LOWER(TRIM(arrest)) WHEN 'true' THEN TRUE WHEN 'false' THEN FALSE END AS arrest,
            CASE LOWER(TRIM(domestic)) WHEN 'true' THEN TRUE WHEN 'false' THEN FALSE END AS domestic,
            district,
            ward,
            community_area,
            latitude::DOUBLE PRECISION,
            longitude::DOUBLE PRECISION
        FROM {source_table}
        WHERE {where_sql}
        ON CONFLICT (id) DO UPDATE SET {updates};
    """

    if truncate_target:
        hook.run(f"TRUNCATE TABLE {target_table};")

    hook.run(sql)

    count = hook.get_first(f"SELECT COUNT(*) FROM {target_table}")[0]
    logger.info(
        "Transformation SQL terminee: %s lignes dans %s depuis %s",
        count, target_table, source_table,
    )
    return count
