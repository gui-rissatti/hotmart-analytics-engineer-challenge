CREATE EXTERNAL TABLE IF NOT EXISTS fact_historic_gmv (
    purchase_id STRING,
    transaction_date DATE,
    product_id STRING,
    subsidiaria STRING,
    valor_gmv DOUBLE
)
PARTITIONED BY (transaction_date)
STORED AS PARQUET
LOCATION 's3://bucket/path/fact_historic_gmv/';

/* Exemplo de query para gerar GMV com time travel */

SELECT transaction_date, subsidiaria, SUM(valor_gmv) AS gmv_dia
FROM fact_historic_gmv
WHERE transaction_date = CURRENT_DATE
GROUP BY transaction_date, subsidiaria;