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
  and the cursor does not move, so no product is skipped. On its first run,
  with no cursor yet, it backfills the newest products
  the page budget allows, so a quiet source still loads.
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
| `lakehouse_code/silver/floe.yml`, `lakehouse_code/silver/sources.yml` | The Floe project and the Bronze sources it reads |
| `lakehouse_code/silver/<domain>/contracts/` | `domain.yml` and one Floe contract per entity: `incremental_mode: file` with `merge_scd1`, or `overwrite` |
| `lakehouse_code/gold/` | The Gold dbt project: shared `dbt_project.yml`, `packages.yml` and `macros/` |
| `lakehouse_code/gold/models/_sources/<domain>.yml` | Each domain's Silver tables, as a dbt source named after the domain |
| `lakehouse_code/gold/models/<product>/` | A product's models, in schema `<product>_gold`, with incremental `merge` models |
| `lakehouse_code/dashboards/superset/<dashboard>/` | Superset report bundle |
| `lakehouse_code/pipelines/dagster/` | One Dagster product job per Gold product |
| `tests/` | Offline extractor tests against a controlled HTTP server and a temporary database |

## Stage bindings

Endpoints and credentials are not part of the code: each stage binds them in
its own namespace, and the same revision runs in DEV and PROD unchanged. DEV
reads the Open Food Facts staging server, PROD the production one.

`olf` writes each stage's bindings from the keys `runtime` declares in each
`source.yaml`, and refuses an unknown or missing key. For DEV:

```bash
uv run olf infra source bind open_food_facts --stage dev -f openlakeforge.yaml \
  --set base_url=https://world.openfoodfacts.net \
  --set user_agent="acme-lakehouse/0.1 (+https://github.com/OpenLakeForge/acme-lakehouse)" \
  --set country=belgium
```

For PROD:

```bash
uv run olf infra source bind open_food_facts --stage prod -f openlakeforge.yaml \
  --set base_url=https://world.openfoodfacts.org \
  --set user_agent="acme-lakehouse/0.1 (+https://github.com/OpenLakeForge/acme-lakehouse)" \
  --set country=belgium
```

RNAcentral publishes one read-only identity for its public database
(<https://rnacentral.org/help/public-database>). It still goes through a Secret
per stage, like any credential, so it is read from the environment, never
passed on the command line. Export `RNACENTRAL_USERNAME` and
`RNACENTRAL_PASSWORD` with the values from that page, then bind each stage:

```bash
for stage in dev prod; do
  uv run olf infra source bind rnacentral --stage "$stage" -f openlakeforge.yaml \
    --set host=hh-pgsql-public.ebi.ac.uk --set port=5432 --set database=pfmegrnargs \
    --secret-from-env username=RNACENTRAL_USERNAME \
    --secret-from-env password=RNACENTRAL_PASSWORD
done
```

Check what each stage has bound. The output names keys only, never values,
and the command exits 1 while a key is missing:

```bash
uv run olf infra source bindings --stage dev -f openlakeforge.yaml
uv run olf infra source bindings --stage prod -f openlakeforge.yaml
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

```bash
uv run olf check dbt --repo-root .
```

```bash
uv run olf check project-code --repo-root .
```

The tests run each connector through the platform's own Bronze loader
(`openlakeforge_runtime.bronze_dlt`, from the pinned `openlakeforge-runtime`
package) against
controlled servers. They need no network. They cover the first load, a run
with no change, changed rows, a killed run, the page budget,
and DEV/PROD isolation.

`openlakeforge`, `openlakeforge-core` and `openlakeforge-runtime` are pinned
to one OpenLakeForge release from PyPI in `pyproject.toml` (currently
`0.6.0a2`, tag `v0.6.0-alpha.2`); keep the three versions equal. After moving
the pin, run `uv run olf project migrate` to bring the descriptors to the
formats that release reads, and apply the layout moves the release's
[CHANGELOG](https://github.com/OpenLakeForge/openlakeforge/blob/main/CHANGELOG.md)
lists under Migration; `olf check` names any file still in the old place.

Python packages the project code needs beyond the platform's own go in
`lakehouse_code/requirements.txt`. The project-code image installs them held to
the platform's pins, and `olf check project-code` fails on a conflict.

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
