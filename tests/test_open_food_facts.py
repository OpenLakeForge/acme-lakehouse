from __future__ import annotations

import time
from pathlib import Path

import pytest

from conftest import LAKEHOUSE_CODE, FakeOpenFoodFacts, bronze_files, bronze_rows
from lakehouse_code.bronze.open_food_facts.dlt.open_food_facts import open_food_facts
from libs.bronze_dlt import load_dlt_source

NOW = int(time.time())


def _bindings(api: FakeOpenFoodFacts, *, page_size: int = 2, max_pages: int = 10) -> dict[str, str]:
    return {
        "SOURCES__OPEN_FOOD_FACTS__BASE_URL": api.url,
        "SOURCES__OPEN_FOOD_FACTS__USER_AGENT": "acme-lakehouse-tests/1.0",
        "SOURCES__OPEN_FOOD_FACTS__COUNTRY": "belgium",
        "SOURCES__OPEN_FOOD_FACTS__PAGE_SIZE": str(page_size),
        "SOURCES__OPEN_FOOD_FACTS__MAX_PAGES": str(max_pages),
        "SOURCES__OPEN_FOOD_FACTS__REQUEST_INTERVAL_SECONDS": "0",
    }


def _load(bucket: Path, *, full_refresh: bool = False, resources=("products", "nutrition_grades")) -> dict[str, int]:
    results = load_dlt_source(
        "open_food_facts",
        open_food_facts(),
        resources,
        full_refresh=full_refresh,
        bucket_url=bucket.as_uri(),
        lakehouse_root=LAKEHOUSE_CODE,
    )
    return {name: result.rows for name, result in results.items()}


@pytest.fixture
def dev(off_servers, stage_env) -> FakeOpenFoodFacts:
    api = off_servers["dev"]
    stage_env("dev", _bindings(api))
    for index in range(5):
        api.upsert(f"p{index}", NOW - 3600 * (index + 1), nutriscore_grade="b")
    # Older than the first run's lookback: never extracted incrementally.
    api.upsert("ancient", NOW - 30 * 86400)
    return api


def test_the_first_run_extracts_the_lookback_window(dev: FakeOpenFoodFacts, tmp_path: Path) -> None:
    assert _load(tmp_path) == {"products": 5, "nutrition_grades": 2}
    assert {row["code"] for row in bronze_rows(tmp_path, "open_food_facts", "products")} == {f"p{i}" for i in range(5)}


def test_a_run_with_no_change_writes_nothing_and_reads_one_page(dev: FakeOpenFoodFacts, tmp_path: Path) -> None:
    _load(tmp_path)
    before = bronze_files(tmp_path, "open_food_facts", "products")
    dev.requests.clear()

    assert _load(tmp_path, resources=("products",)) == {"products": 0}
    assert bronze_files(tmp_path, "open_food_facts", "products") == before
    assert [request["path"] for request in dev.requests] == ["/api/v2/search"]


def test_changed_and_new_products_land_once_in_a_newer_batch(dev: FakeOpenFoodFacts, tmp_path: Path) -> None:
    _load(tmp_path)
    dev.upsert("p3", NOW - 60, nutriscore_grade="a")
    dev.upsert("new", NOW - 30)

    assert _load(tmp_path, resources=("products",)) == {"products": 2}
    _older, newest = bronze_files(tmp_path, "open_food_facts", "products")
    import pyarrow.parquet as pq

    batch = pq.read_table(tmp_path / "open_food_facts" / "products" / newest).to_pylist()
    assert {row["code"] for row in batch} == {"p3", "new"}
    assert next(row for row in batch if row["code"] == "p3")["nutriscore_grade"] == "a"


def test_running_out_of_pages_fails_without_moving_the_cursor(dev: FakeOpenFoodFacts, stage_env, tmp_path) -> None:
    _load(tmp_path)
    for index in range(6):
        dev.upsert(f"burst{index}", NOW - index - 1)
    stage_env("dev", _bindings(dev, max_pages=2))

    with pytest.raises(Exception, match="did not reach the cursor"):
        _load(tmp_path, resources=("products",))

    stage_env("dev", _bindings(dev))
    assert _load(tmp_path, resources=("products",)) == {"products": 6}


def test_dev_and_prod_read_their_own_server_and_keep_their_own_cursor(off_servers, stage_env, tmp_path) -> None:
    dev, prod = off_servers["dev"], off_servers["prod"]
    dev.upsert("staging_only", NOW - 60)
    prod.upsert("production_only", NOW - 60)
    buckets = {"dev": tmp_path / "dev", "prod": tmp_path / "prod"}

    for stage, api in (("dev", dev), ("prod", prod)):
        stage_env(stage, _bindings(api))
        _load(buckets[stage], resources=("products",))
    dev.upsert("staging_second", NOW - 10)
    stage_env("prod", _bindings(prod))

    assert _load(buckets["prod"], resources=("products",)) == {"products": 0}
    assert [row["code"] for row in bronze_rows(buckets["prod"], "open_food_facts", "products")] == ["production_only"]
    assert [row["code"] for row in bronze_rows(buckets["dev"], "open_food_facts", "products")] == ["staging_only"]


def test_a_full_refresh_reloads_products_from_the_lookback_window(dev: FakeOpenFoodFacts, tmp_path: Path) -> None:
    _load(tmp_path)
    dev.upsert("p0", NOW - 10, nutriscore_grade="e")
    _load(tmp_path, resources=("products",))

    assert _load(tmp_path, full_refresh=True, resources=("products",)) == {"products": 5}
    rows = bronze_rows(tmp_path, "open_food_facts", "products")
    assert len(bronze_files(tmp_path, "open_food_facts", "products")) == 1
    assert sorted(row["code"] for row in rows) == [f"p{i}" for i in range(5)]


def test_nutrition_grades_replace_their_files_on_every_run(dev: FakeOpenFoodFacts, tmp_path: Path) -> None:
    _load(tmp_path)
    dev.grades = {"a": 11, "b": 3, "e": 4}

    assert _load(tmp_path, resources=("nutrition_grades",)) == {"nutrition_grades": 3}
    rows = bronze_rows(tmp_path, "open_food_facts", "nutrition_grades")
    assert sorted((row["grade"], row["products"]) for row in rows) == [("a", 11), ("b", 3), ("e", 4)]


def test_requests_identify_the_client_and_filter_the_country(dev: FakeOpenFoodFacts, tmp_path: Path) -> None:
    _load(tmp_path, resources=("products",))

    search = dev.requests[0]
    assert search["user_agent"] == "acme-lakehouse-tests/1.0"
    assert search["query"]["countries_tags_en"] == ["belgium"]
    assert search["query"]["sort_by"] == ["last_modified_t"]
