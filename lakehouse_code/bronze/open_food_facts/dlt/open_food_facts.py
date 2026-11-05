"""Open Food Facts products and Nutri-Score grades, read from the public API.

The search API cannot filter on modification time, but it can sort on it. So
`products` reads newest first and stops at the first product at or before the
cursor. The stage binds `base_url`: DEV reads the staging server
(world.openfoodfacts.net), PROD the production one (world.openfoodfacts.org).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from typing import Any

from openlakeforge_runtime.bronze_csv import BronzeLoadResult
from openlakeforge_runtime.bronze_dlt import load_dlt_source
from openlakeforge_runtime.bronze_incremental import newest_first_pages

import dlt
from dlt.sources.helpers import requests

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


@dlt.source(name="open_food_facts")
def open_food_facts(
    base_url: str = dlt.config.value,
    user_agent: str = dlt.config.value,
    country: str = dlt.config.value,
    page_size: int = 100,
    # Both servers refuse anonymous search pages past the tenth (401 on
    # staging, 503 on production), so a budget above 10 cannot be spent.
    max_pages: int = 10,
    # The API guide allows 10 search requests a minute per client.
    request_interval_seconds: float = 6.0,
) -> Any:
    base = base_url.rstrip("/")
    headers = {"User-Agent": user_agent}

    @dlt.resource(name="products", primary_key="code", columns=_PRODUCT_COLUMNS)
    def products(
        # dlt reads the cursor from this default argument; it is dlt's own idiom.
        last_modified_t: Any = dlt.sources.incremental(  # noqa: B008
            "last_modified_t", initial_value=0, row_order="desc"
        ),
    ) -> Iterator[list[dict[str, Any]]]:
        def page(number: int) -> list[dict[str, Any]]:
            response = requests.get(
                f"{base}/api/v2/search",
                params={
                    "countries_tags_en": country,
                    "sort_by": "last_modified_t",
                    "fields": ",".join(_PRODUCT_FIELDS),
                    "page_size": page_size,
                    "page": number,
                },
                headers=headers,
            )
            response.raise_for_status()
            return response.json().get("products") or []

        for found in newest_first_pages(
            page,
            last_modified_t,
            cursor_field="last_modified_t",
            key="code",
            page_size=page_size,
            max_pages=max_pages,
            interval_seconds=request_interval_seconds,
        ):
            yield [_product_row(product) for product in found]

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


def load_open_food_facts_entities_to_bronze(resources: Sequence[str]) -> dict[str, BronzeLoadResult]:
    return load_dlt_source("open_food_facts", open_food_facts(), resources)
