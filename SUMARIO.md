# Sumário - Desafio Técnico Analytics Engineer | Hotmart

## Índice Geral

1. [Visão Geral](#visão-geral)
2. [Exercício 1: Consultas SQL](#exercício-1-consultas-sql)
3. [Exercício 2: Pipeline ETL e GMV](#exercício-2-pipeline-etl-e-gmv)
4. [Requisitos Funcionais](#requisitos-funcionais)
5. [Melhorias e Trade-offs](#melhorias-e-trade-offs)

---

## Estrutura do Repositório

### Diretório `ex_1/` - Consultas SQL

| Arquivo | Descrição | Link |
|---------|-----------|------|
| `query_1.sql` | Top 50 produtores por faturamento em 2021 | [Acessar](./ex_1/query_1.sql) |
| `query_2.sql` | Top 2 produtos que mais faturaram por produtor | [Acessar](./ex_1/query_2.sql) |

### Diretório `ex_2/` - Pipeline ETL

| Arquivo | Descrição | Link |
|---------|-----------|------|
| `script_etl.py` | Pipeline PySpark/Glue para consolidação diária de compras | [Acessar](./ex_2/script_etl.py) |
| `query.sql` | Consulta GMV diário por subsidiária | [Acessar](./ex_2/query.sql) |
| `output_exemplo.csv` | Exemplo de saída consolidada (snapshot diário) | [Acessar](./ex_2/output_exemplo.csv) |

---

## Navegação por Componente

### Exercício 1: Análise de Faturamento

#### Query 1: Top 50 Produtores (2021)
- **Objetivo**: Identificar os 50 maiores produtores por faturamento no ano de 2021
- **Tabelas**: `purchase`, `product_item`
- **Métrica**: Soma de `purchase_value` para compras pagas (`release_date IS NOT NULL`)
- **Arquivo**: [`ex_1/query_1.sql`](./ex_1/query_1.sql)

**Decisões técnicas**:
- Filtro temporal com `BETWEEN` para performance (índices B-Tree)
- Agregação direta sem CTEs desnecessárias
- `ORDER BY DESC LIMIT 50` para resultado otimizado

#### Query 2: Top 2 Produtos por Produtor
- **Objetivo**: Ranquear os 2 produtos que mais faturaram dentro de cada produtor
- **Tabelas**: `purchase`, `product_item`
- **Técnica**: `ROW_NUMBER() OVER (PARTITION BY producer_id ORDER BY faturamento DESC)`
- **Arquivo**: [`ex_1/query_2.sql`](./ex_1/query_2.sql)

**Decisões técnicas**:
- CTEs nomeadas (`vendas`, `rank`) para clareza
- `ROW_NUMBER` (não `RANK`) para desempate determinístico
- Filtro `rank_produto < 3` aplicado após window function

---

### Exercício 2: Pipeline ETL e GMV Diário

#### Script ETL Principal
- **Objetivo**: Consolidar eventos assíncronos de 3 tabelas em snapshot diário particionado
- **Tecnologia**: PySpark + AWS Glue
- **Arquivo**: [`ex_2/script_etl.py`](./ex_2/script_etl.py)

**Componentes principais**:

1. **Leitura de eventos D-1** (linhas 121-127)
   - Filtro por `transaction_date = process_date` (D-0)
   - Fontes: `purchases`, `product_items`, `purchase_extra_info`

2. **Deduplicação intradiária** (linhas 94-100, 129-131)
   - `last_event_of_day()`: mantém último evento por `purchase_id` via `row_number()`

3. **Agregação por fonte** (linhas 137-158)
   - `purchases`: campos principais + timestamp
   - `items`: timestamp agregado por compra
   - `extra`: subsidiária + timestamp

4. **Forward fill** (linhas 171-199)
   - Carrega snapshot anterior (D-1)
   - `COALESCE(campo_novo, prev_snap.campo)` para repetir dados ativos

5. **Escrita particionada** (linhas 212-218)
   - `mode("overwrite")` + `partitionBy("snapshot_date")`
   - Particionamento dinâmico para idempotência

#### Consulta GMV Diário por Subsidiária
- **Objetivo**: Calcular GMV (compras pagas) agrupado por dia e subsidiária
- **Arquivo**: [`ex_2/query.sql`](./ex_2/query.sql)

**Query correta para a tabela implementada**:
```sql
SELECT 
    snapshot_date AS transaction_date,
    subsidiary AS subsidiaria,
    SUM(gmv) AS gmv_dia
FROM consolidated_purchase_daily
WHERE snapshot_date = CURRENT_DATE
  AND release_date IS NOT NULL
  AND is_paid = true
GROUP BY snapshot_date, subsidiary
ORDER BY snapshot_date, subsidiary;
```

#### Exemplo de Saída
- **Arquivo**: [`ex_2/output_exemplo.csv`](./ex_2/output_exemplo.csv)
- **Demonstração**: Forward fill de `subsidiary` e `gmv` entre snapshots de compra 55 e 56

---

## Requisitos Funcionais - Resumo

### Exercício 1

| Requisito | Status | Artefato |
|-----------|--------|----------|
| Top 50 produtores por faturamento (2021) | ✅ Atendido | [`query_1.sql`](./ex_1/query_1.sql) |
| Top 2 produtos por produtor | ✅ Atendido | [`query_2.sql`](./ex_1/query_2.sql) |

### Exercício 2

| Requisito | Status | Artefato |
|-----------|--------|----------|
| Modelagem histórica imutável | ✅ Atendido | [`script_etl.py`](./ex_2/script_etl.py) (snapshot diário) |
| Rastreabilidade diária | ✅ Atendido | Partição `snapshot_date` + timestamps de fonte |
| Processamento D-1 | ✅ Atendido | Linhas 42-46 (calcula D-1 a partir de D-0) |
| Eventos assíncronos | ✅ Atendido | Left joins + forward fill (linhas 171-199) |
| Repetir dados ativos | ✅ Atendido | `COALESCE` com snapshot anterior |
| Idempotência | ✅ Atendido | Sobrescrita de partição + lógica determinística |
| GMV diário por subsidiária | ✅ Atendido | [`query.sql`](./ex_2/query.sql) |
| Evitar dupla contagem | ✅ Atendido | Grain `purchase_id` + `snapshot_date` (1 registro/dia) |
| Estado corrente fácil | ✅ Atendido | Filtro `MAX(snapshot_date)` |
| Particionamento | ✅ Atendido | `partitionBy("snapshot_date")` |

---

## Melhorias e Trade-offs

### Melhorias Possíveis
- [SCD Type 2 completo](#possíveis-melhorias) → Rastrear mudanças intradiárias
- [Deduplicação de eventos](#possíveis-melhorias) → Hash de conteúdo para detectar reenvios
- [Late arriving data](#possíveis-melhorias) → Backfill para atrasos > D-1
- [Data Quality checks](#possíveis-melhorias) → Validações de integridade
- [Testes automatizados](#possíveis-melhorias) → Suite unittest + pytest
- [Monitoramento](#possíveis-melhorias) → Métricas de volume e latência
- [Catalogação](#possíveis-melhorias) → AWS Glue Data Catalog
- [Orquestração](#possíveis-melhorias) → Airflow/Step Functions

### Trade-offs Documentados
- **Snapshot diário vs SCD Type 2**: Simplicidade de consulta vs granularidade intradiária
- **D-1 vs real-time**: Custo/otimização de batch vs latência
- **Forward fill vs NULL**: Lógica simples vs diferenciação "não chegou" / "removido"
- **Overwrite vs MERGE**: Idempotência nativa vs histórico append-only
- **Particionamento snapshot_date**: Isolamento temporal vs complexidade de time travel
- **PySpark vs SQL**: Escalabilidade/AWS vs simplicidade

---

## Links Úteis

- [README Principal](./README.md)
- [Exercício 1 - Consultas SQL](./ex_1/)
- [Exercício 2 - Pipeline ETL](./ex_2/)

---

**Autor**: Guilherme Rissatti Malheiros  
**Data**: Novembro de 2025  
**Contexto**: Desafio Técnico - Analytics Engineer | Hotmart
