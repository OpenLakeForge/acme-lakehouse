from __future__ import annotations

from libs.product_dagster import ProductDefinitionSpec, build_product_definitions

RNA_CATALOG_SILVER_INPUTS = ("sequences", "expert_databases")

RNA_CATALOG_GOLD_ASSETS = ("mart_sequence_index", "mart_database_coverage")

defs = build_product_definitions(
    ProductDefinitionSpec(
        domain="rna",
        product="rna_catalog",
        silver_inputs=RNA_CATALOG_SILVER_INPUTS,
        bronze_inputs=tuple(("rnacentral", resource) for resource in RNA_CATALOG_SILVER_INPUTS),
        gold_assets=RNA_CATALOG_GOLD_ASSETS,
    )
)
