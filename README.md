# ACME Lakehouse

A customer-style [OpenLakeForge](https://github.com/OpenLakeForge/openlakeforge)
project. ACME is a fictitious company. The project loads two public external
sources incrementally, from Bronze through Silver to Gold, the way a team would
build its own lakehouse on a published OpenLakeForge release
([#110](https://github.com/OpenLakeForge/openlakeforge/issues/110)).

The platform's own CSV reference project stays in the platform repository as
its conformance fixture. This repository holds the realistic workloads.

## What it loads

| Source | Resource | Load mode | Cursor | Key | Silver | Gold |
| --- | --- | --- | --- | --- | --- | --- |
| Open Food Facts API | `products` | incremental | `last_modified_t` | `code` | `food.products`, merged | `mart_product_scores` (incremental) |
| Open Food Facts API | `nutrition_grades` | full | | | `food.nutrition_grades`, overwritten | |
| RNAcentral PostgreSQL | `sequences` | incremental | `id` | `urs` | `rna.sequences`, merged | `mart_sequence_index` (incremental) |
| RNAcentral PostgreSQL | `expert_databases` | full | | | `rna.expert_databases`, overwritten | `mart_database_coverage` (table) |

`mart_nutriscore_distribution` is also a full rebuild. Each model that stays
a full rebuild says why in its SQL.

- **Open Food Facts.** The search API cannot filter on modification time, but
  it can sort on it. The connector reads one country's products newest first
  and stops at the cursor. If its page budget runs out first, the run fails
  and the cursor does not move, so no product is skipped.
- **RNAcentral.** The connector reads `rna` in `id` order, at most
  `max_rows_per_run` rows a run, so the first runs backfill the table one slice
  at a time. A killed run is retried from the last loaded id.

How each layer's mode is declared, what a full refresh rebuilds, and the known
limits are documented once, in the platform's
[load modes reference](https://github.com/OpenLakeForge/openlakeforge/blob/main/docs/reference/descriptors.md#load-modes).

## Layout

| Path | Contains |
| --- | --- |
| `openlakeforge.yaml` | Deployment Profile: local provider, Slim preset, DEV and PROD stages |
| `lakehouse_code/governance.yml` | Domains and the products they own |
| `lakehouse_code/bronze/<source>/` | `source.yaml` (load modes, runtime keys) and the dlt connector |
| `lakehouse_code/silver/<domain>/contracts/floe/` | Floe contracts: `incremental_mode: file` with `merge_scd1`, or `overwrite` |
| `lakehouse_code/gold/<product>/dbt/` | dbt projects with incremental `merge` models |
| `lakehouse_code/pipelines/dagster/` | One Dagster product job per Gold product |
| `tests/` | Offline extractor tests against a controlled HTTP server and a temporary database |

## Stage bindings

Endpoints and credentials are not part of the code: each stage binds them in
its own namespace, and the same revision runs in DEV and PROD unchanged. DEV
reads the Open Food Facts staging server, PROD the production one.

```bash
kubectl -n olf-dev create configmap olf-source-open-food-facts \
  --from-literal=base_url=https://world.openfoodfacts.net \
  --from-literal=user_agent="acme-lakehouse/0.1 (+https://github.com/OpenLakeForge/acme-lakehouse)" \
  --from-literal=country=belgium
```

```bash
kubectl -n olf-prod create configmap olf-source-open-food-facts \
  --from-literal=base_url=https://world.openfoodfacts.org \
  --from-literal=user_agent="acme-lakehouse/0.1 (+https://github.com/OpenLakeForge/acme-lakehouse)" \
  --from-literal=country=belgium
```

RNAcentral publishes one read-only identity for its public database
(<https://rnacentral.org/help/public-database>). It still goes through a Secret
per stage, like any credential. Repeat these two commands for `olf-prod`:

```bash
kubectl -n olf-dev create configmap olf-source-rnacentral \
  --from-literal=host=hh-pgsql-public.ebi.ac.uk --from-literal=port=5432 --from-literal=database=pfmegrnargs
```

```bash
kubectl -n olf-dev create secret generic olf-source-rnacentral-credentials \
  --from-literal=username=<user from the RNAcentral page> --from-literal=password=<password from the RNAcentral page>
```

Activation refuses a revision in a stage that has not bound every key
`source.yaml` lists.

## Run

```bash
uv run olf product run food_quality --stage dev
```

A run is incremental. To reload Bronze from scratch and rebuild Gold for one
run:

```bash
uv run olf product run food_quality --stage dev --full-refresh bronze
```

## Develop

```bash
uv sync
```

```bash
uv run pytest
```

```bash
uv run olf project validate --project .
```

```bash
uv run olf check contracts --repo-root .
```

The tests run each connector through the platform's own Bronze loader
(`libs.bronze_dlt`, from the pinned OpenLakeForge distribution) against
controlled servers. They need no network. They cover the first load, a run
with no change, changed rows, a killed run, a full refresh, the page budget,
and DEV/PROD isolation.

`openlakeforge` is pinned to an exact platform commit in `pyproject.toml`
until the load modes ship in a published release.

## Data terms

This repository contains code only, no extracted data.

- Open Food Facts data is under the Open Database License (ODbL). A lakehouse
  built from it is a derived database: publishing it requires attribution and
  the same licence.
- RNAcentral's public database is a shared service. `max_rows_per_run` bounds
  each extraction, so read its terms before raising it.

## Not yet verified

- A live run on the local kind stack: Dagster and dbt incremental runs
  against the real services. The Open Food Facts extraction and the Floe
  `merge_scd1` runs, catch-up, no-change, incremental and full refresh, were
  checked once against Polaris and SeaweedFS outside kind, with Floe 0.8.6.
- Scan statistics showing that an incremental Gold run reads only new Silver
  data ([#344](https://github.com/OpenLakeForge/openlakeforge/issues/344)).
