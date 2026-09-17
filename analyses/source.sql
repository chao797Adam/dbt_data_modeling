select * from {{ source('external_source', 'orders') }}
