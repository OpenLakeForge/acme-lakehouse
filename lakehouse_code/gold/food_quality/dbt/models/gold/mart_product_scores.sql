-- Incremental: each run merges only the products Silver changed since this
-- table's newest row. `olf product run --full-refresh` rebuilds it.
{{ config(materialized='incremental', unique_key='code', incremental_strategy='merge') }}

select
    code,
    product_name,
    brands,
    coalesce(nutriscore_grade, 'unknown') as nutriscore_grade,
    nova_group,
    energy_kcal_100g,
    sugars_100g,
    salt_100g,
    case
        when sugars_100g is null then 'unknown'
        when sugars_100g > 22.5 then 'high'
        when sugars_100g > 5 then 'medium'
        else 'low'
    end as sugar_level,
    last_modified_t,
    last_modified_at
from {{ source('silver', 'products') }}
{% if is_incremental() %}
where last_modified_t > (select coalesce(max(last_modified_t), 0) from {{ this }})
{% endif %}
