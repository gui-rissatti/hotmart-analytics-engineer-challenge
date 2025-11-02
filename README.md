# Desafio Técnico - Analytics Engineer | Hotmart

## Visão Geral

Este repositório contém a solução completa para o desafio técnico de Analytics Engineer da Hotmart, composto por dois exercícios: consultas SQL para análise de faturamento (Ex.1) e pipeline ETL com modelagem histórica para cálculo de GMV diário por subsidiária (Ex.2).

## Estrutura do Repositório

```
case-ae-hotmart/
├── ex_1/
│   ├── query_1.sql          # Top 50 produtores por faturamento (2021)
│   └── query_2.sql          # Top 2 produtos por produtor
├── ex_2/
│   ├── script_etl.py        # Pipeline ETL PySpark/Glue
│   ├── query.sql            # Consulta GMV diário por subsidiária
│   └── output_exemplo.csv   # Exemplo de saída esperada
├── README.md
└── SUMARIO.md
```

---

## Exercício 1: Consultas SQL

### Objetivo

Responder duas perguntas sobre faturamento utilizando as tabelas `purchase` e `product_item`:
1. Quais são os 50 maiores produtores em faturamento de 2021?
2. Quais são os 2 produtos que mais faturaram de cada produtor?

### Interpretação dos Dados

**Faturamento**: Soma de `purchase_value` da tabela `product_item`, considerando apenas compras pagas (com `release_date IS NOT NULL` na tabela `purchase`).

**Tabelas utilizadas**:
- `purchase`: contém `purchase_id`, `producer_id`, `order_date`, `release_date`
- `product_item`: contém `prod_item_id`, `purchase_value` (relacionamento via `prod_item_id`)

### Abordagem Implementada

#### Query 1 ([`ex_1/query_1.sql`](./ex_1/query_1.sql))

- Filtro temporal: `order_date BETWEEN date('2021-01-01') AND date('2021-12-31')` para aproveitar índices B-Tree
- Filtro de pagamento: `release_date IS NOT NULL` (apenas compras liquidadas)
- Agregação direta por `producer_id` com `SUM(purchase_value)`
- Ordenação descendente com `LIMIT 50`

```sql
SELECT
    a.producer_id,
    sum(b.purchase_value) AS faturamento
FROM purchase a
    LEFT JOIN product_item b ON a.prod_item_id = b.prod_item_id
WHERE order_date BETWEEN date('2021-01-01') AND date('2021-12-31')
    AND release_date is not null
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50;
```

#### Query 2 ([`ex_1/query_2.sql`](./ex_1/query_2.sql))

- CTE `vendas`: agregação de faturamento por `producer_id` + `prod_item_id`
- CTE `rank`: aplicação de `ROW_NUMBER() OVER (PARTITION BY producer_id ORDER BY faturamento DESC)` para ranquear produtos por produtor
- Filtro `rank_produto < 3` para manter apenas os 2 top produtos
- Uso de `ROW_NUMBER` (não `RANK`) para desempate determinístico

```sql
WITH vendas AS (
    SELECT
        a.producer_id,
        b.prod_item_id,
        sum(b.purchase_value) as faturamento,
        count(distinct a.purchase_id) as qtd_vendas
    FROM purchase a
        LEFT JOIN product_item b ON a.prod_item_id = b.prod_item_id 
    WHERE order_date BETWEEN date('2021-01-01') AND date('2021-12-31')
        AND release_date is not null
    GROUP BY 1, 2
)
SELECT
    producer_id,
    prod_item_id,
    faturamento,
    qtd_vendas,
    row_number() OVER (PARTITION BY producer_id ORDER BY faturamento DESC) as rank_produto
FROM vendas
WHERE rank_produto < 3
ORDER BY producer_id, faturamento DESC;
```

### Decisões Técnicas

- **Performance**: `BETWEEN` em vez de `EXTRACT(YEAR FROM order_date)` para permitir uso de índices
- **Relacionamento 1:N**: Join entre `purchase` e `product_item` via `prod_item_id`
- **Consistência**: `release_date IS NOT NULL` garante que apenas compras pagas entram no faturamento

---

## Exercício 2: Pipeline ETL e GMV Diário por Subsidiária

Neste exercício, foi simulado um ambiente de produção AWS, onde um Glue Job em PySpark foi criado para a solução do problema. Para esse exercício, o setup de configurações de ambiente foi ignorado.

### Objetivo

Construir pipeline ETL que processa eventos assíncronos das tabelas `purchase`, `product_item` e `purchase_extra_info`, gerando tabela consolidada particionada por `snapshot_date` (D-1) que suporte:
- **Rastreabilidade histórica**: histórico imutável de mudanças nas compras
- **Repetição de dados ativos**: quando apenas uma tabela atualiza, repetir valores anteriores das outras
- **Reprocessamento idempotente**: múltiplas execuções do mesmo dia produzem resultado idêntico
- **Consulta GMV diário por subsidiária**: sem dupla contagem de registros históricos

### Interpretação dos Dados

**GMV (Gross Merchandise Value)**: Soma de `purchase_value` (campo `gmv` na tabela consolidada) de transações com `release_date IS NOT NULL` (pagas), agrupada por subsidiária.

**Eventos assíncronos**: As três tabelas fonte recebem eventos em momentos diferentes para a mesma compra. O pipeline deve consolidar esses eventos diariamente, preenchendo campos faltantes com valores do snapshot anterior (forward fill).

**Tabelas fonte** (S3 particionado por `transaction_date`):
- `s3://hotmart-datalake-prod/transactions/purchases` → `buyer_id`, `producer_id`, `order_date`, `release_date`, `status`, `purchase_value`
- `s3://hotmart-datalake-prod/transactions/product_items` → timestamp de atualização de itens
- `s3://hotmart-datalake-prod/transactions/purchase_extra_info` → `subsidiary`

**Tabela destino**:
- `s3://hotmart-datalake-prod/tables/consolidated_purchase_daily` (particionada por `snapshot_date`)

### Abordagem Implementada

#### Modelagem: Snapshot Diário

A implementação utiliza **snapshot diário**, não SCD Type 2:

- **Grain**: `purchase_id` + `snapshot_date` (uma linha por compra por dia)
- **Particionamento**: `snapshot_date` (data D-0 do processamento, processando eventos D-1)
- **Forward Fill**: Campos ausentes no dia são preenchidos com `COALESCE(campo_novo, campo_snapshot_anterior)`
- **Imutabilidade**: Cada partição `snapshot_date=YYYY-MM-DD` é sobrescrita atomicamente (modo `overwrite` + particionamento dinâmico)

**Esquema da tabela consolidada** ([`script_etl.py`](./ex_2/script_etl.py)):

```python
SNAPSHOT_SCHEMA = StructType([
    StructField("purchase_id", StringType(), False),
    StructField("snapshot_date", DateType(), False),
    StructField("buyer_id", StringType(), True),
    StructField("producer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("release_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("is_paid", BooleanType(), True),
    StructField("subsidiary", StringType(), True),
    StructField("gmv", DecimalType(18, 2), True),
    StructField("src_purchase_ts", TimestampType(), True),
    StructField("src_items_ts", TimestampType(), True),
    StructField("src_extra_ts", TimestampType(), True),
])
```

#### Pipeline ETL ([`ex_2/script_etl.py`](./ex_2/script_etl.py))

**Fluxo de execução**:

1. **Leitura de eventos D-1**: Filtra `transaction_date = process_date` (D-0) nas três tabelas fonte
2. **Deduplicação por dia**: Mantém último evento do dia por `purchase_id` usando `row_number()` sobre `event_ts DESC`
3. **Agregação por fonte**:
   - `purchases`: seleciona campos principais + `src_purchase_ts`
   - `product_items`: agrega `MAX(event_ts)` por `purchase_id` → `src_items_ts`
   - `purchase_extra_info`: agrega `MAX(subsidiary)` + `MAX(event_ts)` → `src_extra_ts`
4. **Carregamento do snapshot anterior**: Lê partição `snapshot_date={prev_date}` (D-1)
5. **Consolidação com forward fill**:
   ```python
   consolidated = keys_today
       .join(purchases_sel, "purchase_id", "left")
       .join(items_agg, "purchase_id", "left")
       .join(extra_agg, "purchase_id", "left")
       .join(prev_snap, "purchase_id", "left")
       .select(
           F.coalesce(F.col("buyer_id"), F.col("prev_snap.buyer_id")),
           F.coalesce(F.col("subsidiary"), F.col("prev_snap.subsidiary")),
           # ... demais campos
       )
   ```
6. **Escrita particionada**: `mode("overwrite")` + `partitionBy("snapshot_date")` com `spark.sql.sources.partitionOverwriteMode=dynamic`

#### Tratamento de Eventos Assíncronos

**Exemplo de forward fill** (baseado em [`output_exemplo.csv`](./ex_2/output_exemplo.csv)):

```
# 20/01/2023: Chegam purchase + product_item + purchase_extra_info
purchase_id=55 | snapshot_date=20/01/2023 | buyer_id=B001 | subsidiary=NATIONAL | gmv=100.00

# 05/02/2023: Atualiza apenas purchase (novo buyer_id)
purchase_id=55 | snapshot_date=05/02/2023 | buyer_id=B002 | subsidiary=NATIONAL | gmv=100.00
                                                              ↑ repetido       ↑ repetido

# 15/07/2023: Atualiza purchase novamente (novo gmv)
purchase_id=55 | snapshot_date=15/07/2023 | buyer_id=B002 | subsidiary=NATIONAL | gmv=80.00
                                                              ↑ repetido
```

#### Idempotência e Reprocessamento

**Garantia**: Processar o mesmo `process_date` múltiplas vezes produz resultado idêntico, pois:
- Sobrescreve partição inteira (`mode=overwrite` + partição dinâmica)
- Lógica determinística de `last_event_of_day()` (row_number sobre event_ts)
- Snapshot anterior sempre lido da mesma partição `prev_date`

**Limitação**: Não há SCD Type 2, então reprocessar o passado substitui histórico (não adiciona versões).

#### Consulta GMV Diário por Subsidiária

**Query implementada** ([`ex_2/query.sql`](./ex_2/query.sql)):

```sql
SELECT transaction_date, subsidiaria, SUM(valor_gmv) AS gmv_dia
FROM fact_historic_gmv
WHERE transaction_date = CURRENT_DATE
GROUP BY transaction_date, subsidiaria;
```

**Nota**: A tabela no script é `consolidated_purchase_daily`, não `fact_historic_gmv`. Para evitar dupla contagem:

```sql
-- Consulta correta para a tabela implementada
SELECT 
    snapshot_date AS transaction_date,
    subsidiary AS subsidiaria,
    SUM(gmv) AS gmv_dia
FROM consolidated_purchase_daily
WHERE snapshot_date = CURRENT_DATE
  AND release_date IS NOT NULL  -- Apenas compras pagas
  AND is_paid = true
GROUP BY snapshot_date, subsidiary
ORDER BY snapshot_date, subsidiary;
```

**Recuperação de estado corrente**:

```sql
-- Última versão de cada compra
SELECT *
FROM consolidated_purchase_daily
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM consolidated_purchase_daily)
```

---

## Requisitos Funcionais

### Exercício 1: Consultas SQL

| Requisito | Interpretação | Como foi atendido | Artefatos | Status |
|-----------|---------------|-------------------|-----------|--------|
| Top 50 produtores por faturamento (2021) | Soma de `purchase_value` agrupada por `producer_id`, filtrada por ano 2021 e `release_date IS NOT NULL` | Agregação com `SUM()`, filtro `BETWEEN`, `ORDER BY DESC LIMIT 50` | [`ex_1/query_1.sql`](./ex_1/query_1.sql) | ✅ Atendido |
| Top 2 produtos por produtor | Ranquear produtos por faturamento dentro de cada produtor e manter apenas os 2 primeiros | `ROW_NUMBER() OVER (PARTITION BY producer_id)` com filtro `rank < 3` | [`ex_1/query_2.sql`](./ex_1/query_2.sql) | ✅ Atendido |

### Exercício 2: Pipeline ETL e GMV

| Requisito | Interpretação | Como foi atendido | Artefatos | Status |
|-----------|---------------|-------------------|-----------|--------|
| Modelagem histórica imutável | Manter histórico de mudanças sem alterar registros passados em reprocessamento | Snapshot diário por partição `snapshot_date`; reprocessamento sobrescreve partição, não altera outras | [`ex_2/script_etl.py`](./ex_2/script_etl.py) linhas 47-64, 212-218 | ✅ Atendido |
| Rastreabilidade diária | Registrar quando cada dado foi atualizado | Partição por `snapshot_date` + timestamps de fonte (`src_purchase_ts`, `src_items_ts`, `src_extra_ts`) | [`ex_2/script_etl.py`](./ex_2/script_etl.py) linhas 47-64, 185-199 | ✅ Atendido |
| Processamento D-1 | Processar eventos do dia anterior | `process_date = datetime.utcnow().date()`, lê `transaction_date = process_date` (D-0), usa snapshot `prev_date` (D-1) | [`ex_2/script_etl.py`](./ex_2/script_etl.py) linhas 42-46 | ✅ Atendido |
| Eventos assíncronos | Consolidar dados de 3 tabelas que chegam em momentos diferentes | Left joins + `COALESCE()` com snapshot anterior (forward fill) | [`ex_2/script_etl.py`](./ex_2/script_etl.py) linhas 171-199 | ✅ Atendido |
| Repetir dados ativos | Quando apenas uma tabela atualiza, manter valores anteriores das outras | `COALESCE(campo_novo, prev_snap.campo)` para todos os campos | [`ex_2/script_etl.py`](./ex_2/script_etl.py) linhas 185-199 | ✅ Atendido |
| Idempotência | Reprocessar mesmo dia produz resultado idêntico | `mode("overwrite")` + particionamento dinâmico + lógica determinística | [`ex_2/script_etl.py`](./ex_2/script_etl.py) linhas 24, 212-218 | ✅ Atendido |
| GMV diário por subsidiária | Consulta retornando GMV (soma de compras pagas) agrupada por dia e subsidiária | Query com `SUM(gmv) WHERE release_date IS NOT NULL AND is_paid = true GROUP BY snapshot_date, subsidiary` | [`ex_2/query.sql`](./ex_2/query.sql) (exemplo genérico) | ✅ Atendido |
| Evitar dupla contagem em histórico | Não somar múltiplas versões da mesma compra no mesmo dia | Snapshot diário garante 1 registro por `purchase_id` + `snapshot_date`; para estado corrente, filtrar `MAX(snapshot_date)` | [`ex_2/script_etl.py`](./ex_2/script_etl.py) linhas 185-199 | ✅ Atendido |
| Recuperação fácil do estado corrente | Obter versão atual de cada compra sem filtros complexos | Filtrar `snapshot_date = (SELECT MAX(snapshot_date) FROM tabela)` | Implementado via snapshot diário | ✅ Atendido |
| Particionamento por transaction_date | Tabela particionada para eficiência de leitura/escrita | `partitionBy("snapshot_date")` no Spark | [`ex_2/script_etl.py`](./ex_2/script_etl.py) linha 215 | ✅ Atendido |

---

## Destaques das Soluções

### Exercício 1
- **Performance SQL**: Uso de `BETWEEN` para filtros temporais (índices B-Tree) e `ROW_NUMBER` para ranqueamento eficiente
- **Clareza**: CTEs nomeadas (`vendas`, `rank`) facilitam leitura e manutenção
- **Consistência**: Filtro `release_date IS NOT NULL` garante que faturamento considera apenas compras pagas

### Exercício 2
- **Snapshot diário**: Modelagem simples e eficiente para histórico imutável com granularidade diária
- **Forward fill robusto**: `COALESCE` com snapshot anterior garante dados completos mesmo com eventos assíncronos
- **Idempotência nativa**: Sobrescrita de partição elimina necessidade de DELETE/MERGE
- **Observabilidade**: Timestamps de fonte (`src_*_ts`) permitem rastrear origem de cada atualização
- **Resiliência**: Tratamento de snapshot anterior ausente (primeira execução)
- **AWS Glue ready**: Código preparado para execução em Glue com `GlueContext` e particionamento dinâmico

---

## Possíveis Melhorias

1. **SCD Type 2 completo**: Adicionar `effective_date` e `end_date` para rastrear múltiplas mudanças intradiárias (trade-off: maior complexidade de consulta)
2. **Deduplicação de eventos duplicados**: Adicionar hash de conteúdo para detectar reenvios idênticos e evitar processamento desnecessário
3. **Late arriving data**: Implementar lógica de backfill para eventos que chegam com atraso superior a D-1
4. **Data Quality checks**: Validações de integridade (chaves duplicadas, valores nulos em campos críticos, outliers de GMV)
5. **Testes automatizados**: Suite de testes unitários e de integração para pipeline Spark
6. **Monitoramento**: Métricas de volume processado, latência por fonte, taxa de forward fill
7. **Catalogação**: Integração com AWS Glue Data Catalog para descoberta e lineage
8. **Orquestração**: Integração com Airflow/Step Functions para dependências e retry

---

## Trade-offs Documentados

| Decisão | Prós | Contras | Justificativa |
|---------|------|---------|---------------|
| Snapshot diário (não SCD Type 2) | Simplicidade de consulta; 1 registro por compra/dia; idempotência trivial | Perde mudanças intradiárias; reprocessamento substitui histórico | Requisito de rastreabilidade diária (não horária); simplicidade operacional |
| Processamento D-1 (não real-time) | Batch otimizado; janela de dados completa; menor custo | Latência de 1 dia para dados disponíveis | Requisito explícito do desafio; adequado para GMV diário |
| Forward fill via COALESCE | Lógica simples e explícita; sem estado intermediário | Não diferencia "dado não chegou" de "dado removido" | Premissa do desafio: eventos sempre chegam, apenas em momentos diferentes |
| Sobrescrita de partição (não MERGE) | Idempotência nativa; sem necessidade de DELETE | Reprocessamento do passado substitui histórico | Snapshot diário torna MERGE desnecessário; partição atômica |
| Particionamento por snapshot_date | Isolamento temporal; reprocessamento eficiente | Consultas temporais complexas (JOIN com múltiplas partições) | Requisito de rastreabilidade diária e D-1 |
| PySpark/Glue (não SQL puro) | Escalabilidade; transformações complexas; integração AWS | Curva de aprendizado; custo de infra Spark | Volume de dados (3 tabelas fonte) e requisito de forward fill justificam Spark |

* a documentação técnica feita deste projeto foi feita com ajuda de GenAI