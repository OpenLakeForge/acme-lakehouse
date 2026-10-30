-- A full rebuild on purpose: the source table is small and replaced whole on
-- every run, so there is no delta to merge.
select
    id,
    coalesce(display_name, name) as database_name,
    alive = 'Y' as is_active,
    num_sequences,
    num_organisms,
    last_import_date
from {{ source('silver', 'expert_databases') }}
