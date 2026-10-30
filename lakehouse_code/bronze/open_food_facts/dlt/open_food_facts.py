"""Open Food Facts products and Nutri-Score grades, read from the public API.

The search API cannot filter on modification time, but it can sort on it. So
`products` reads newest first and stops at the first product at or before the
cursor. The stage binds `base_url`: DEV reads the staging server
(world.openfoodfacts.net), PROD the production one (world.openfoodfacts.org).
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import dlt
from dlt.sources.helpers import requests
from libs.bronze_csv import BronzeLoadResult
from libs.bronze_dlt import load_dlt_source

_PRODUCT_FIELDS = (
    "code",
    "product_name",
    "brands",
    "categories_tags",
    "nutriscore_grade",
    "nova_group",
    "nutriments",
    "last_modified_t",
)

# Declared rather than inferred: a column that is null throughout a page would
# otherwise get no type, and a later page would land it as a variant column
# Floe does not read.
_PRODUCT_COLUMNS = {
    "code": {"data_type": "text", "nullable": False},
    "product_name": {"data_type": "text"},
    "brands": {"data_type": "text"},
    "categories": {"data_type": "text"},
    "nutriscore_grade": {"data_type": "text"},
    "nova_group": {"data_type": "bigint"},
    "energy_kcal_100g": {"data_type": "double"},
    "sugars_100g": {"data_type": "double"},
    "salt_100g": {"data_type": "double"},
    "last_modified_t": {"data_type": "bigint", "nullable": False},
    "last_modified_at": {"data_type": "timestamp", "nullable": False},
}


class CursorNotReached(RuntimeError):
    """The page budget ran out before the products reached the cursor."""


@dlt.source(name="open_food_facts")
def open_food_facts(
    base_url: str = dlt.config.value,
    user_agent: str = dlt.config.value,
    country: str = dlt.config.value,
    initial_lookback_days: int = 3,
    page_size: int = 100,
    max_pages: int = 30,
    # The API guide allows 10 search requests a minute per client.
    request_interval_seconds: float = 6.0,
) -> Any:
    base = base_url.rstrip("/")
    headers = {"User-Agent": user_agent}
    initial = int((datetime.now(UTC) - timedelta(days=initial_lookback_days)).timestamp())

    @dlt.resource(name="products", primary_key="code", columns=_PRODUCT_COLUMNS)
    def products(
        # dlt reads the cursor from this default argument; it is dlt's own idiom.
        last_modified_t: Any = dlt.sources.incremental(  # noqa: B008
            "last_modified_t", initial_value=initial, row_order="desc"
        ),
    ) -> Iterator[list[dict[str, Any]]]:
        seen: set[str] = set()
        for page in range(1, max_pages + 1):
            if page > 1:
                time.sleep(request_interval_seconds)
            response = requests.get(
                f"{base}/api/v2/search",
                params={
                    "countries_tags_en": country,
                    "sort_by": "last_modified_t",
                    "fields": ",".join(_PRODUCT_FIELDS),
                    "page_size": page_size,
                    "page": page,
                },
                headers=headers,
            )
            response.raise_for_status()
            found = response.json().get("products") or []
            # A product edited while we page moves to page 1 and shifts the
            # rest down one, so it can be read twice; the first read is newer.
            rows = [_product_row(product) for product in found if product.get("code") not in seen]
            seen.update(row["code"] for row in rows)
            if rows:
                yield rows
            if len(found) < page_size or found[-1].get("last_modified_t", 0) <= last_modified_t.start_value:
                return
        # Stopping here would move the cursor past products not yet read, and
        # no later run would read them. Fail instead, so the cursor stays put.
        raise CursorNotReached(
            f"{max_pages} pages of {page_size} products did not reach the cursor "
            f"{last_modified_t.start_value}; raise max_pages or narrow the country"
        )

    @dlt.resource(name="nutrition_grades", primary_key="grade")
    def nutrition_grades() -> Iterator[list[dict[str, Any]]]:
        response = requests.get(f"{base}/facets/nutrition-grades.json", headers=headers)
        response.raise_for_status()
        yield [{"grade": tag["id"], "products": int(tag["products"])} for tag in response.json().get("tags", [])]

    return products, nutrition_grades


def _number(nutriments: dict[str, Any], key: str) -> float | None:
    value = nutriments.get(key)
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _product_row(product: dict[str, Any]) -> dict[str, Any]:
    nutriments = product.get("nutriments") or {}
    nova = product.get("nova_group")
    modified = int(product["last_modified_t"])
    return {
        "code": str(product["code"]),
        "product_name": product.get("product_name") or None,
        "brands": product.get("brands") or None,
        "categories": ",".join(product.get("categories_tags") or []) or None,
        "nutriscore_grade": product.get("nutriscore_grade") or None,
        "nova_group": int(nova) if isinstance(nova, int | float) or str(nova).isdigit() else None,
        "energy_kcal_100g": _number(nutriments, "energy-kcal_100g"),
        "sugars_100g": _number(nutriments, "sugars_100g"),
        "salt_100g": _number(nutriments, "salt_100g"),
        "last_modified_t": modified,
        "last_modified_at": datetime.fromtimestamp(modified, UTC),
    }


def load_open_food_facts_entities_to_bronze(
    resources: Sequence[str], *, full_refresh: bool = False
) -> dict[str, BronzeLoadResult]:
    return load_dlt_source("open_food_facts", open_food_facts(), resources, full_refresh=full_refresh)
