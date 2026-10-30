from __future__ import annotations

from libs.product_dagster import ProductDefinitionSpec, build_product_definitions

FOOD_QUALITY_SILVER_INPUTS = ("products", "nutrition_grades")

FOOD_QUALITY_GOLD_ASSETS = ("mart_product_scores", "mart_nutriscore_distribution")

defs = build_product_definitions(
    ProductDefinitionSpec(
        domain="food",
        product="food_quality",
        silver_inputs=FOOD_QUALITY_SILVER_INPUTS,
        bronze_inputs=tuple(("open_food_facts", resource) for resource in FOOD_QUALITY_SILVER_INPUTS),
        gold_assets=FOOD_QUALITY_GOLD_ASSETS,
    )
)
