--QUAIS SÃO OS 2 PRODUTOS QUE MAIS FATURAM ($) DE CADA PRODUTOR?

WITH vendas AS (
    SELECT
        a.producer_id,
        b.prod_item_id,
        sum(b.purchase_value) as faturamento,
        count(distinct a.purchase_id) as qtd_vendas
    FROM purchase a
        LEFT JOIN product_item b
            ON a.prod_item_id = b.prod_item_id 
    WHERE 1=1
        AND order_date BETWEEN date('2021-01-01') AND date('2021-12-31')
          -- alternativa: EXTRACT(YEAR FROM order_date) = 2021 - desempate por performance (usar BETWEEN pode aproveitar índices B-Tree)
        AND release_date is not null
          -- considera apenas compras pagas (com release_date preenchido)
    GROUP BY 1, 2
)
, rank as (
    SELECT
        *,
        row_number() OVER (PARTITION BY producer_id ORDER BY faturamento DESC) as rank_produto
          -- cria uma numeração para os produtos de cada produtor, ordenada pelo faturamento
    FROM vendas
)
SELECT
    producer_id,
    prod_item_id,
    faturamento,
    qtd_vendas,
    rank_produto
FROM rank
    WHERE 1=1
        AND rank_produto < 3
            -- Filtro para pegar os 2 produtos que mais faturam por produtor
    ORDER BY producer_id, faturamento DESC;