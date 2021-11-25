{{
    config (
        engine='MergeTree()',
        order_by=['LO_ORDERDATE', 'LO_ORDERKEY'],
        partition_by='toYYYYMM(LO_ORDERDATE)'
    )
}}

SELECT
    {{ dbt_utils.star(source('dbgen', 'lineorder')) }}
    , {{ dbt_utils.star(source('dbgen', 'customer')) }}
    , {{ dbt_utils.star(source('dbgen', 'supplier')) }}
    , {{ dbt_utils.star(source('dbgen', 'part')) }}
FROM
    {{ source('dbgen', 'lineorder') }} AS l
    INNER JOIN {{ source('dbgen', 'customer') }} AS c
        ON c.C_CUSTKEY = l.LO_CUSTKEY
    INNER JOIN {{ source('dbgen', 'supplier') }} AS s
        ON s.S_SUPPKEY = l.LO_SUPPKEY
    INNER JOIN {{ source('dbgen', 'part') }} AS p
        ON p.P_PARTKEY = l.LO_PARTKEY
