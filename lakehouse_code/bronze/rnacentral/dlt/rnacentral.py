"""RNAcentral sequences and expert databases, read from its public PostgreSQL.

`sequences` reads the `rna` table in `id` order, at most `max_rows_per_run`
rows a run, so the first runs backfill one slice at a time. A killed run is
retried from the last loaded id: the cursor moves only when dlt finishes
loading a batch.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

import sqlalchemy as sa

import dlt
from libs.bronze_csv import BronzeLoadResult
from libs.bronze_dlt import load_dlt_source

_SEQUENCE_COLUMNS = {
    "id": {"data_type": "bigint", "nullable": False},
    "urs": {"data_type": "text", "nullable": False},
    "length": {"data_type": "bigint"},
    "md5": {"data_type": "text"},
    "created_at": {"data_type": "timestamp"},
}

_DATABASE_COLUMNS = {
    "id": {"data_type": "bigint", "nullable": False},
    "name": {"data_type": "text", "nullable": False},
    "display_name": {"data_type": "text"},
    "alive": {"data_type": "text"},
    "num_sequences": {"data_type": "bigint"},
    "num_organisms": {"data_type": "bigint"},
    "last_import_date": {"data_type": "timestamp"},
}


def _engine(*, host: str, port: int, database: str, username: str, password: str) -> sa.Engine:
    url = sa.URL.create(
        "postgresql+psycopg2", username=username, password=password, host=host, port=port, database=database
    )
    return sa.create_engine(url, connect_args={"connect_timeout": 30})


@dlt.source(name="rnacentral")
def rnacentral(
    host: str = dlt.config.value,
    port: int = dlt.config.value,
    database: str = dlt.config.value,
    username: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    batch_size: int = 10_000,
    max_rows_per_run: int = 100_000,
) -> Any:
    engine = _engine(host=host, port=port, database=database, username=username, password=password)

    @dlt.resource(name="sequences", primary_key="urs", columns=_SEQUENCE_COLUMNS)
    def sequences(
        # dlt reads the cursor from this default argument; it is dlt's own idiom.
        id: Any = dlt.sources.incremental("id", initial_value=0),  # noqa: A002, B008
    ) -> Iterator[list[dict[str, Any]]]:
        last, loaded = id.start_value, 0
        query = sa.text(
            "select id, urs, len as length, md5, timestamp as created_at "
            "from rna where id > :last order by id limit :limit"
        )
        with engine.connect() as connection:
            while loaded < max_rows_per_run:
                limit = min(batch_size, max_rows_per_run - loaded)
                rows = [dict(row) for row in connection.execute(query, {"last": last, "limit": limit}).mappings()]
                if not rows:
                    return
                yield rows
                last, loaded = rows[-1]["id"], loaded + len(rows)

    @dlt.resource(name="expert_databases", primary_key="id", columns=_DATABASE_COLUMNS)
    def expert_databases() -> Iterator[list[dict[str, Any]]]:
        query = sa.text(
            "select id, descr as name, display_name, alive, num_sequences, num_organisms, last_import_date "
            "from rnc_database order by id"
        )
        with engine.connect() as connection:
            yield [dict(row) for row in connection.execute(query).mappings()]

    return sequences, expert_databases


def load_rnacentral_entities_to_bronze(
    resources: Sequence[str], *, full_refresh: bool = False
) -> dict[str, BronzeLoadResult]:
    return load_dlt_source("rnacentral", rnacentral(), resources, full_refresh=full_refresh)
