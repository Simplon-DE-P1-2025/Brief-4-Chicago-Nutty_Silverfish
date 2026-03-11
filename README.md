# Pipeline Chicago Crimes

Pipeline Airflow pour ingest, controler, transformer et charger les donnees Chicago Crimes dans PostgreSQL.

## DAGs disponibles

- `chicago_crimes_daily`
  - recupere les 14 derniers jours via l'API Chicago
  - charge `chicago_crimes_raw_staging`
  - lance les checks Soda sur le brut
  - transforme vers `chicago_crimes_transformed_staging`
  - lance les checks Soda sur le transforme
  - fait un UPSERT vers les tables finales `chicago_crimes_raw` et `chicago_crimes_transformed`
  - si l'API ne renvoie aucune ligne sur la fenetre recente, le DAG passe en no-op et skip l'aval

- `chicago_crimes_bootstrap`
  - chargement historique manuel par batch de `50000`
  - utilise des tables de staging dediees:
    - `chicago_crimes_raw_bootstrap_staging`
    - `chicago_crimes_transformed_bootstrap_staging`
  - checks Soda batch par batch
  - UPSERT batch par batch vers les tables finales
  - valide sur plusieurs offsets manuels (`0`, `50000`, `100000`)

Les deux DAGs sont limites a `max_active_runs=1`.

## Tables principales

- tables finales:
  - `chicago_crimes_raw`
  - `chicago_crimes_transformed`
- staging quotidien:
  - `chicago_crimes_raw_staging`
  - `chicago_crimes_transformed_staging`
- staging bootstrap:
  - `chicago_crimes_raw_bootstrap_staging`
  - `chicago_crimes_transformed_bootstrap_staging`

## Demarrage local

```bash
astro dev start

astro dev run connections add chicago_crimes_db \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema postgres \
  --conn-login postgres \
  --conn-password postgres \
  --conn-port 5432
```

UI Airflow:

```bash
http://127.0.0.1:8080
```

Si le navigateur ne voit rien sur `localhost:8080`, verifier que le navigateur est bien sur la meme machine que celle qui execute Astro/Docker.

## Repartir proprement

```bash
astro dev kill
astro dev start

astro dev run connections add chicago_crimes_db \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema postgres \
  --conn-login postgres \
  --conn-password postgres \
  --conn-port 5432
```

Le `kill` supprime les donnees des conteneurs locaux. Il faut donc recreer la connexion Airflow apres reset.

## Lancer les DAGs

Bootstrap historique:

```bash
astro dev run dags trigger chicago_crimes_bootstrap
```

Quotidien:

```bash
astro dev run dags trigger chicago_crimes_daily
```

Ne pas lancer `chicago_crimes_daily` en parallele du `chicago_crimes_bootstrap`.

## Commandes utiles

Verifier les DAGs:

```bash
astro dev parse
astro dev run dags list
```

Tester une tache:

```bash
astro dev run tasks test chicago_crimes_bootstrap create_tables 2026-03-10
astro dev run tasks test chicago_crimes_daily ingest_data 2026-03-10
```

Verifier la base finale:

```bash
SELECT 'chicago_crimes_raw' AS table_name, COUNT(*) FROM chicago_crimes_raw
UNION ALL
SELECT 'chicago_crimes_transformed' AS table_name, COUNT(*) FROM chicago_crimes_transformed;
```
