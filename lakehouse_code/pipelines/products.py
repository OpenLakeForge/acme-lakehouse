from __future__ import annotations

from openlakeforge_runtime.code_location import ProductOverrides

# A product's job is built from governance.yml, and its inputs default to what the dbt and Floe projects declare. These
# entries pin the inputs explicitly, as the product modules did before the code location was derived from descriptors.
PRODUCTS: dict[str, ProductOverrides] = {
    "food_quality": ProductOverrides(
        silver_inputs=("products", "nutrition_grades"),
        bronze_inputs=(("open_food_facts", "products"), ("open_food_facts", "nutrition_grades")),
        gold_assets=("mart_product_scores", "mart_nutriscore_distribution"),
    ),
    "rna_catalog": ProductOverrides(
        silver_inputs=("sequences", "expert_databases"),
        bronze_inputs=(("rnacentral", "sequences"), ("rnacentral", "expert_databases")),
        gold_assets=("mart_sequence_index", "mart_database_coverage"),
    ),
}
