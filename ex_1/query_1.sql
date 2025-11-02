--QUAIS SÃO OS 50 MAIORES PRODUTORES EM FATURAMENTO ($) DE 2021?

SELECT
    a.producer_id,
    sum(b.purchase_value) AS faturamento
FROM purchase a
    LEFT JOIN product_item b
        ON a.prod_item_id = b.prod_item_id
WHERE 1=1
    AND order_date BETWEEN date('2021-01-01') AND date('2021-12-31')
      -- alternativa: EXTRACT(YEAR FROM order_date) = 2021 - desempate por performance (usar BETWEEN pode aproveitar índices B-Tree)
    AND release_date is not null
        -- considera apenas compras pagas (com release_date preenchido)
GROUP BY 1
    -- agrupa por produtor (producer_id)
ORDER BY 2 DESC
    -- ordena do maior para o menor faturamento
LIMIT 50;
    -- garante que apenas os 50 maiores produtores sejam retornados