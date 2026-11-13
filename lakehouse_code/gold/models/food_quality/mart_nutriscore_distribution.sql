-- A full rebuild on purpose: a grade's share depends on every product, so
-- merging only the changed products would leave the other grades' shares stale.
select
    nutriscore_grade,
    count(*) as products,
    round(100.0 * count(*) / sum(count(*)) over (), 2) as share_pct,
    avg(sugars_100g) as avg_sugars_100g
from {{ ref('mart_product_scores') }}
group by nutriscore_grade
