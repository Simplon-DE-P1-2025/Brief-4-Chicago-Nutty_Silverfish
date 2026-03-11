# Pipeline Chicago Crimes

Pipeline Airflow pour ingerer, controler, transformer et charger les donnees Chicago Crimes dans PostgreSQL. Les donnees proviennent de l'API ouverte de la ville de Chicago (dataset `ijzp-q8t2`).

## Stack technique

- Apache Airflow (via Astro CLI)
- PostgreSQL
- Soda Core (controles qualite)
- Pandas (transformation Python)
- Docker (environnement local)

## Structure du projet

```
dags/
  chicago_crimes_pipeline.py   # DAG quotidien (daily)
  chicago_crimes_bootstrap.py  # DAG chargement historique
  chicago_crimes_sandbox.py    # DAG sandbox (re-transformation SQL)
  pipeline_utils.py            # Fonctions partagees
include/soda/
  configuration.yml            # Connexion Soda vers PostgreSQL
  checks/                      # Fichiers de checks par table
tests/
  test_chicago_crimes_pipeline.py
  test_sandbox.py
```

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

- `chicago_crimes_sandbox`
  - re-transformation des donnees brutes existantes (`chicago_crimes_raw`) en SQL pur (sans Pandas)
  - ecrit dans `chicago_crimes_transformed_v2`
  - applique les filtres geo et dates futures
  - lance les checks Soda sur la table sandbox
  - compare les resultats avec la table transformee actuelle (comptage, outliers geo, dates futures)
  - declenchement manuel uniquement

Les trois DAGs sont limites a `max_active_runs=1`.

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
- sandbox:
  - `chicago_crimes_transformed_v2`

## Checks de qualite (Soda)

Chaque etape du pipeline lance un scan Soda. Les validations principales :

- **Donnees brutes** : `row_count > 0`, pas d'ID manquant, pas de doublons sur `id`, pas de `date` ou `primary_type` manquant, au moins une latitude/longitude presente, pas de date dans le futur
- **Donnees transformees** : `row_count > 0`, pas d'ID manquant ni de doublons, pas de `crime_date` / `latitude` / `longitude` manquant, bornes geographiques Chicago (lat `41.6-42.1`, lon `-87.95 a -87.5`), pas de date dans le futur
- **Bootstrap** : memes checks adaptes aux tables de staging bootstrap
- **Sandbox** : `row_count >= 1500`, memes checks de bornes geo et dates futures

Les fichiers de checks se trouvent dans `include/soda/checks/`.

## Transformation des donnees

Le pipeline transforme les donnees brutes (raw) en donnees exploitables :

1. Suppression des lignes sans `latitude`, `longitude` ou `primary_type`
2. Conversion de `date` (texte) en `crime_date` (timestamp) + extraction de `crime_year`, `crime_month`, `crime_day_of_week`
3. Conversion de `arrest` et `domestic` (texte `"true"/"false"`) en booleens
4. Conversion de `latitude` et `longitude` en `DOUBLE PRECISION`
5. Filtrage des coordonnees hors Chicago (bornes geo) et des dates futures (DAG sandbox)

Le DAG daily/bootstrap utilise Pandas pour la transformation. Le DAG sandbox effectue la meme transformation entierement en SQL cote PostgreSQL.

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

Sandbox:

```bash
astro dev run dags trigger chicago_crimes_sandbox
```

Ne pas lancer `chicago_crimes_daily` en parallele du `chicago_crimes_bootstrap`.

## Tests

Lancer les tests unitaires :

```bash
astro dev pytest
```

Les tests couvrent :
- la construction de la clause `WHERE` temporelle
- la pagination de l'API
- la generation du SQL d'UPSERT
- la preparation des tables de staging
- le comportement no-op du DAG daily quand l'ingestion est vide
- la transformation SQL sandbox (filtres geo, dates futures, truncate)

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
