from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
import sqlalchemy as sa

from conftest import LAKEHOUSE_CODE, bronze_files, bronze_rows
from lakehouse_code.bronze.rnacentral.dlt import rnacentral as rnacentral_module
from lakehouse_code.bronze.rnacentral.dlt.rnacentral import rnacentral
from libs.bronze_dlt import load_dlt_source


@pytest.fixture
def connections(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """What the source asked to connect to, with the database swapped for SQLite."""
    seen: list[dict] = []

    def engine(**binding: object) -> sa.Engine:
        seen.append(binding)
        return sa.create_engine(f"sqlite:///{binding['database']}")

    monkeypatch.setattr(rnacentral_module, "_engine", engine)
    return seen


@pytest.fixture
def database(tmp_path: Path, stage_env, connections) -> Path:
    """A temporary database with RNAcentral's table and column names."""
    path = tmp_path / "rnacentral.db"
    with sqlite3.connect(path) as connection:
        connection.execute("create table rna (id integer, urs text, len integer, md5 text, timestamp text)")
        connection.execute(
            "create table rnc_database (id integer, descr text, display_name text, alive text, "
            "num_sequences integer, num_organisms integer, last_import_date text)"
        )
        connection.executemany(
            "insert into rna values (?, ?, ?, ?, ?)",
            [(i, f"URS{i:010d}", 40 * i, f"md5-{i}", "2026-01-01 00:00:00") for i in range(1, 8)],
        )
        connection.execute("insert into rnc_database values (1, 'ENA', 'ENA', 'Y', 7, 3, '2026-04-21 09:56:18')")
    stage_env(
        "dev",
        {
            "SOURCES__RNACENTRAL__HOST": "rnacentral.dev.example",
            "SOURCES__RNACENTRAL__PORT": "5432",
            "SOURCES__RNACENTRAL__DATABASE": str(path),
            "SOURCES__RNACENTRAL__USERNAME": "reader",
            "SOURCES__RNACENTRAL__PASSWORD": "not-a-real-password",
            "SOURCES__RNACENTRAL__BATCH_SIZE": "2",
            "SOURCES__RNACENTRAL__MAX_ROWS_PER_RUN": "4",
        },
    )
    return path


def _load(bucket: Path, source=None, *, full_refresh: bool = False, resources=("sequences",)) -> dict[str, int]:
    results = load_dlt_source(
        "rnacentral",
        source or rnacentral(),
        resources,
        full_refresh=full_refresh,
        bucket_url=bucket.as_uri(),
        lakehouse_root=LAKEHOUSE_CODE,
    )
    return {name: result.rows for name, result in results.items()}


def test_sequences_backfill_a_bounded_slice_per_run(database: Path, tmp_path: Path) -> None:
    bucket = tmp_path / "bronze"

    assert [_load(bucket)["sequences"] for _ in range(3)] == [4, 3, 0]
    ids = sorted(row["id"] for row in bronze_rows(bucket, "rnacentral", "sequences"))
    assert ids == list(range(1, 8))


def test_a_killed_run_is_retried_without_duplicates(database: Path, tmp_path: Path) -> None:
    bucket = tmp_path / "bronze"
    seen: list[int] = []

    def die_on_the_second_batch(row: dict) -> dict:
        seen.append(row["id"])
        if len(seen) > 2:
            raise RuntimeError("killed")
        return row

    killed = rnacentral()
    killed.resources["sequences"].add_map(die_on_the_second_batch)
    with pytest.raises(Exception, match="killed"):
        _load(bucket, killed)

    assert _load(bucket)["sequences"] == 4
    assert _load(bucket)["sequences"] == 3
    ids = [row["id"] for row in bronze_rows(bucket, "rnacentral", "sequences")]
    assert sorted(ids) == list(range(1, 8))


def test_new_sequences_land_in_a_newer_batch(database: Path, tmp_path: Path) -> None:
    bucket = tmp_path / "bronze"
    _load(bucket), _load(bucket)
    with sqlite3.connect(database) as connection:
        connection.execute("insert into rna values (8, 'URS0000000008', 99, 'md5-8', '2026-02-01 00:00:00')")

    assert _load(bucket)["sequences"] == 1
    newest = bronze_files(bucket, "rnacentral", "sequences")[0]
    import pyarrow.parquet as pq

    assert pq.read_table(bucket / "rnacentral" / "sequences" / newest).column("urs").to_pylist() == ["URS0000000008"]


def test_a_full_refresh_restarts_the_backfill(database: Path, tmp_path: Path) -> None:
    bucket = tmp_path / "bronze"
    _load(bucket), _load(bucket)

    assert _load(bucket, full_refresh=True)["sequences"] == 4
    assert sorted(row["id"] for row in bronze_rows(bucket, "rnacentral", "sequences")) == [1, 2, 3, 4]


def test_expert_databases_replace_their_files_on_every_run(database: Path, tmp_path: Path) -> None:
    bucket = tmp_path / "bronze"
    _load(bucket, resources=("expert_databases",))
    with sqlite3.connect(database) as connection:
        connection.execute("update rnc_database set num_sequences = 8")

    assert _load(bucket, resources=("expert_databases",)) == {"expert_databases": 1}
    (row,) = bronze_rows(bucket, "rnacentral", "expert_databases")
    assert (row["name"], row["num_sequences"]) == ("ENA", 8)


def test_the_stage_bindings_reach_the_connection(database: Path, connections: list[dict], tmp_path: Path) -> None:
    _load(tmp_path / "bronze", resources=("expert_databases",))

    (binding,) = connections
    assert binding == {
        "host": "rnacentral.dev.example",
        "port": 5432,
        "database": str(database),
        "username": "reader",
        "password": "not-a-real-password",
    }
