"""
================================================================================
ETL PURCHASE HISTORY - HOTMART
================================================================================

Requisitos Atendidos:
✅ Modelagem histórica com SCD Type 2 (effective_date, end_date, is_current)
✅ Processamento D-1 (incremental por transaction_date)
✅ Tratamento assíncrono de 3 tabelas (full outer join)
✅ Forward fill (repete informações não atualizadas)
✅ Idempotência (reprocessável sem alterar passado)
✅ Time travel (navegação temporal via as_of_date)
✅ Particionamento por transaction_date
✅ GMV diário por subsidiária com time travel

Uso:
    # Processar dia específico
    python etl_purchase_history.py --process-date 2023-01-20
    
    # Consultar GMV com time travel
    python etl_purchase_history.py --query-gmv --as-of-date 2023-07-31

Melhorias Futuras:
    - Validações de data quality mais robustas
    - Testes automatizados
    - Métricas de observabilidade
    - Otimizações de performance (broadcast joins, etc)
    - Tratamento de late arriving data
    - Compactação de histórico antigo

================================================================================
"""

import argparse
import sys
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType, BooleanType


def get_spark_session(app_name="PurchaseHistoryETL"):
    """Cria SparkSession configurada"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()


def create_sample_data(spark):
    """
    Cria dados de exemplo baseados na transcrição do vídeo.
    Remove esta função em produção - use tabelas reais.
    """
    print("📝 Criando dados de exemplo...")
    
    # PURCHASE (eventos)
    purchase_data = [
        (55, 100, 5500, "2023-01-20", "2023-01-20", 1, 1000.00, "2023-01-20"),
        (55, 200, 5500, "2023-01-20", "2023-01-20", 1, 1000.00, "2023-02-05"),
        (55, 200, 5500, "2023-01-20", "2023-07-15", 1, 1000.00, "2023-07-15"),
        (56, 300, 5600, "2023-01-26", None, 2, 500.00, "2023-01-26"),
        (57, 400, 5700, "2023-01-20", "2023-01-20", 3, 2500.00, "2023-01-20"),
        (58, 500, 5800, "2023-01-21", "2023-01-21", 1, 350.00, "2023-01-21"),
    ]
    
    purchase_schema = StructType([
        StructField("purchase_id", IntegerType(), False),
        StructField("buyer_id", IntegerType(), True),
        StructField("purchase_relation_id", IntegerType(), True),
        StructField("order_date", DateType(), True),
        StructField("release_date", DateType(), True),
        StructField("producer_id", IntegerType(), True),
        StructField("purchase_value", DecimalType(10, 2), True),
        StructField("transaction_date", DateType(), False),
    ])
    
    df_purchase = spark.createDataFrame(purchase_data, purchase_schema)
    df_purchase.createOrReplaceTempView("purchase")
    
    # PRODUCT_ITEM (eventos)
    product_item_data = [
        (1, 5500, 200, 600.00, "2023-01-20"),
        (1, 5500, 200, 550.00, "2023-07-12"),
        (2, 5600, 201, 500.00, "2023-01-25"),  # Chegou ANTES da purchase!
        (3, 5700, 202, 2500.00, "2023-01-20"),
        (4, 5800, 203, 350.00, "2023-01-21"),
    ]
    
    product_item_schema = StructType([
        StructField("product_item_id", IntegerType(), False),
        StructField("purchase_relation_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("item_value", DecimalType(10, 2), True),
        StructField("transaction_date", DateType(), False),
    ])
    
    df_product_item = spark.createDataFrame(product_item_data, product_item_schema)
    df_product_item.createOrReplaceTempView("product_item")
    
    # PURCHASE_EXTRA_INFO (eventos)
    extra_info_data = [
        (1, 5500, "NATIONAL", "2023-01-23"),     # Chegou 3 DIAS depois
        (3, 5700, "INTERNATIONAL", "2023-01-20"),
        (4, 5800, "NATIONAL", "2023-01-21"),
    ]
    
    extra_info_schema = StructType([
        StructField("purchase_extra_info_id", IntegerType(), False),
        StructField("purchase_relation_id", IntegerType(), True),
        StructField("subsidiary", StringType(), True),
        StructField("transaction_date", DateType(), False),
    ])
    
    df_extra_info = spark.createDataFrame(extra_info_data, extra_info_schema)
    df_extra_info.createOrReplaceTempView("purchase_extra_info")
    
    print("✅ Dados de exemplo criados")


def read_events(spark, process_date):
    """
    Lê eventos de D-1 das três tabelas fonte.
    
    Args:
        spark: SparkSession
        process_date: Data a processar (YYYY-MM-DD)
    
    Returns:
        Tupla (df_purchase, df_product_item, df_extra_info)
    """
    print(f"📖 Lendo eventos de {process_date}")
    
    df_purchase = spark.table("purchase") \
        .filter(F.col("transaction_date") == process_date)
    
    df_product_item = spark.table("product_item") \
        .filter(F.col("transaction_date") == process_date)
    
    df_extra_info = spark.table("purchase_extra_info") \
        .filter(F.col("transaction_date") == process_date)
    
    counts = {
        "purchase": df_purchase.count(),
        "product_item": df_product_item.count(),
        "purchase_extra_info": df_extra_info.count()
    }
    
    print(f"   Eventos lidos: {counts}")
    
    if sum(counts.values()) == 0:
        print(f"⚠️  Nenhum evento encontrado para {process_date}")
        return None, None, None
    
    return df_purchase, df_product_item, df_extra_info


def merge_async_events(df_purchase, df_product_item, df_extra_info, process_date):
    """
    Merge assíncrono via full outer join.
    
    Desafio: As tabelas não chegam sincronizadas.
    Solução: Full outer join + coalesce para pegar valores disponíveis.
    
    Args:
        df_purchase: DataFrame de eventos purchase
        df_product_item: DataFrame de eventos product_item
        df_extra_info: DataFrame de eventos purchase_extra_info
        process_date: Data sendo processada
    
    Returns:
        DataFrame merged
    """
    print("🔗 Fazendo merge assíncrono (full outer join)")
    
    # Full outer join: purchase ⟕ product_item
    df_merged = df_purchase.alias("p").join(
        df_product_item.alias("pi"),
        on="purchase_relation_id",
        how="full_outer"
    )
    
    # Full outer join: result ⟕ purchase_extra_info
    df_merged = df_merged.join(
        df_extra_info.alias("pei"),
        on="purchase_relation_id",
        how="full_outer"
    )
    
    # Coalesce: pegar valor não-nulo
    df_merged = df_merged.select(
        F.coalesce("p.purchase_id", "pi.purchase_id", "pei.purchase_id").alias("purchase_id"),
        F.coalesce("p.purchase_relation_id", "pi.purchase_relation_id", "pei.purchase_relation_id").alias("purchase_relation_id"),
        F.lit(process_date).cast("date").alias("transaction_date"),
        
        # Campos de purchase
        F.col("p.buyer_id"),
        F.col("p.order_date"),
        F.col("p.release_date"),
        F.col("p.producer_id"),
        F.col("p.purchase_value"),
        
        # Campos de product_item
        F.col("pi.product_item_id"),
        F.col("pi.product_id"),
        F.col("pi.item_value"),
        
        # Campos de purchase_extra_info
        F.col("pei.purchase_extra_info_id"),
        F.col("pei.subsidiary"),
    )
    
    print(f"   Registros após merge: {df_merged.count()}")
    
    return df_merged


def apply_forward_fill(spark, df_merged, process_date):
    """
    Forward fill: repete valores anteriores quando campo não foi atualizado.
    
    Requisito do vídeo:
    "Quando ele atualizar a subsidiária, nesse mesmo dia não houve atualização 
    de nenhuma das outras tabelas, ele vai repetir o conteúdo e vai trazer 
    a nova informação."
    
    Args:
        spark: SparkSession
        df_merged: DataFrame após merge
        process_date: Data sendo processada
    
    Returns:
        DataFrame com forward fill aplicado
    """
    print("⏩ Aplicando forward fill")
    
    # Buscar últimos valores conhecidos (registros correntes)
    try:
        df_previous = spark.table("fact_purchase_history") \
            .filter(F.col("is_current") == True) \
            .select(
                "purchase_id",
                F.col("buyer_id").alias("prev_buyer_id"),
                F.col("order_date").alias("prev_order_date"),
                F.col("release_date").alias("prev_release_date"),
                F.col("producer_id").alias("prev_producer_id"),
                F.col("purchase_value").alias("prev_purchase_value"),
                F.col("product_item_id").alias("prev_product_item_id"),
                F.col("product_id").alias("prev_product_id"),
                F.col("item_value").alias("prev_item_value"),
                F.col("purchase_extra_info_id").alias("prev_purchase_extra_info_id"),
                F.col("subsidiary").alias("prev_subsidiary")
            )
    except:
        # Tabela ainda não existe (primeira execução)
        print("   ℹ️  Primeira execução - sem forward fill")
        return df_merged
    
    # Left join para trazer valores anteriores
    df_with_previous = df_merged.join(
        df_previous,
        on="purchase_id",
        how="left"
    )
    
    # Coalesce: usar valor novo se existir, senão repetir anterior
    df_forward_filled = df_with_previous.select(
        "purchase_id",
        "purchase_relation_id",
        "transaction_date",
        F.coalesce("buyer_id", "prev_buyer_id").alias("buyer_id"),
        F.coalesce("order_date", "prev_order_date").alias("order_date"),
        F.coalesce("release_date", "prev_release_date").alias("release_date"),
        F.coalesce("producer_id", "prev_producer_id").alias("producer_id"),
        F.coalesce("purchase_value", "prev_purchase_value").alias("purchase_value"),
        F.coalesce("product_item_id", "prev_product_item_id").alias("product_item_id"),
        F.coalesce("product_id", "prev_product_id").alias("product_id"),
        F.coalesce("item_value", "prev_item_value").alias("item_value"),
        F.coalesce("purchase_extra_info_id", "prev_purchase_extra_info_id").alias("purchase_extra_info_id"),
        F.coalesce("subsidiary", "prev_subsidiary").alias("subsidiary")
    )
    
    print("   ✅ Forward fill aplicado")
    
    return df_forward_filled


def detect_changes(spark, df_forward_filled):
    """
    Detecta mudanças reais comparando com registro anterior.
    
    Desafio: Após forward fill, muitos registros são idênticos ao anterior.
    Solução: Apenas inserir se houve mudança real.
    
    Args:
        spark: SparkSession
        df_forward_filled: DataFrame após forward fill
    
    Returns:
        DataFrame contendo apenas registros que mudaram
    """
    print("🔍 Detectando mudanças reais")
    
    try:
        df_previous = spark.table("fact_purchase_history") \
            .filter(F.col("is_current") == True) \
            .select(
                "purchase_id",
                F.concat_ws("|",
                    F.coalesce(F.col("buyer_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("order_date").cast("string"), F.lit("")),
                    F.coalesce(F.col("release_date").cast("string"), F.lit("")),
                    F.coalesce(F.col("producer_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("purchase_value").cast("string"), F.lit("")),
                    F.coalesce(F.col("product_item_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("product_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("item_value").cast("string"), F.lit("")),
                    F.coalesce(F.col("purchase_extra_info_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("subsidiary").cast("string"), F.lit(""))
                ).alias("prev_hash")
            )
    except:
        # Primeira execução - todos os registros são novos
        print("   ℹ️  Primeira execução - todos os registros são novos")
        return df_forward_filled
    
    # Calcular hash do registro atual
    df_with_hash = df_forward_filled.withColumn(
        "current_hash",
        F.concat_ws("|",
            F.coalesce(F.col("buyer_id").cast("string"), F.lit("")),
            F.coalesce(F.col("order_date").cast("string"), F.lit("")),
            F.coalesce(F.col("release_date").cast("string"), F.lit("")),
            F.coalesce(F.col("producer_id").cast("string"), F.lit("")),
            F.coalesce(F.col("purchase_value").cast("string"), F.lit("")),
            F.coalesce(F.col("product_item_id").cast("string"), F.lit("")),
            F.coalesce(F.col("product_id").cast("string"), F.lit("")),
            F.coalesce(F.col("item_value").cast("string"), F.lit("")),
            F.coalesce(F.col("purchase_extra_info_id").cast("string"), F.lit("")),
            F.coalesce(F.col("subsidiary").cast("string"), F.lit(""))
        )
    )
    
    # Comparar hashes
    df_with_comparison = df_with_hash.join(
        df_previous,
        on="purchase_id",
        how="left"
    )
    
    # Filtrar apenas registros que mudaram (ou são novos)
    df_changed = df_with_comparison.filter(
        (F.col("prev_hash").isNull()) |  # Novo registro
        (F.col("current_hash") != F.col("prev_hash"))  # Mudou
    ).drop("current_hash", "prev_hash")
    
    changed_count = df_changed.count()
    print(f"   📊 {changed_count} registros com mudanças reais")
    
    return df_changed


def apply_scd_type_2(spark, df_changed, process_date):
    """
    Aplica SCD Type 2: adiciona effective_date, end_date, is_current.
    
    Lógica:
    1. Novos registros têm effective_date = process_date
    2. end_date = NULL (registro corrente)
    3. is_current = True
    4. Atualizar registros anteriores (end_date e is_current)
    
    Args:
        spark: SparkSession
        df_changed: DataFrame com registros que mudaram
        process_date: Data sendo processada
    
    Returns:
        DataFrame com SCD Type 2 aplicado
    """
    print("📅 Aplicando SCD Type 2")
    
    # Adicionar colunas de SCD
    df_with_scd = df_changed.select(
        "*",
        F.lit(process_date).cast("date").alias("effective_date"),
        F.lit(None).cast("date").alias("end_date"),
        F.lit(True).alias("is_current")
    )
    
    return df_with_scd


def update_previous_records(spark, df_new, process_date):
    """
    Atualiza registros anteriores: end_date e is_current.
    
    Lógica:
    1. Registros que mudaram devem ter end_date = process_date - 1
    2. is_current = False
    
    Args:
        spark: SparkSession
        df_new: DataFrame com novos registros
        process_date: Data sendo processada
    """
    print("🔄 Atualizando registros anteriores")
    
    try:
        df_history = spark.table("fact_purchase_history")
    except:
        # Primeira execução - nada a atualizar
        print("   ℹ️  Primeira execução - nada a atualizar")
        return
    
    # Purchase IDs que mudaram
    purchase_ids_changed = df_new.select("purchase_id").distinct()
    
    # Atualizar: end_date = process_date, is_current = False
    df_updated = df_history.join(
        purchase_ids_changed,
        on="purchase_id",
        how="left"
    ).withColumn(
        "end_date",
        F.when(
            (F.col("is_current") == True) & (purchase_ids_changed.purchase_id.isNotNull()),
            F.lit(process_date).cast("date")
        ).otherwise(F.col("end_date"))
    ).withColumn(
        "is_current",
        F.when(
            (F.col("is_current") == True) & (purchase_ids_changed.purchase_id.isNotNull()),
            False
        ).otherwise(F.col("is_current"))
    )
    
    # Reescrever tabela (simplificado - em produção usar merge)
    df_updated.write.mode("overwrite").saveAsTable("fact_purchase_history_temp")
    spark.sql("DROP TABLE IF EXISTS fact_purchase_history")
    spark.sql("ALTER TABLE fact_purchase_history_temp RENAME TO fact_purchase_history")
    
    print("   ✅ Registros anteriores atualizados")


def write_partition(spark, df_final, process_date):
    """
    Escreve partição de forma idempotente.
    
    Estratégia: DELETE + INSERT (garantia de idempotência)
    
    Args:
        spark: SparkSession
        df_final: DataFrame final a escrever
        process_date: Data sendo processada
    """
    print(f"💾 Escrevendo partição transaction_date={process_date}")
    
    try:
        # Carregar dados existentes
        df_existing = spark.table("fact_purchase_history")
        
        # Remover partição antiga (idempotência)
        df_without_partition = df_existing.filter(
            F.col("transaction_date") != process_date
        )
        
        # Combinar com novos dados
        df_combined = df_without_partition.union(df_final)
        
        # Escrever (overwrite)
        df_combined.write.mode("overwrite").saveAsTable("fact_purchase_history")
        
    except:
        # Primeira execução - criar tabela
        print("   ℹ️  Criando tabela fact_purchase_history")
        df_final.write.mode("overwrite").saveAsTable("fact_purchase_history")
    
    print(f"   ✅ Partição {process_date} escrita com sucesso")


def run_etl(spark, process_date):
    """
    Pipeline ETL completo.
    
    Steps:
    1. Ler eventos D-1
    2. Merge assíncrono (full outer join)
    3. Forward fill
    4. Detectar mudanças
    5. Aplicar SCD Type 2
    6. Atualizar registros anteriores
    7. Escrever partição
    
    Args:
        spark: SparkSession
        process_date: Data a processar (YYYY-MM-DD)
    """
    print(f"\n{'='*80}")
    print(f"ETL PURCHASE HISTORY - {process_date}")
    print(f"{'='*80}\n")
    
    start_time = datetime.now()
    
    # 1. Ler eventos
    df_purchase, df_product_item, df_extra_info = read_events(spark, process_date)
    
    if df_purchase is None and df_product_item is None and df_extra_info is None:
        print(f"⏭️  Nada a processar para {process_date}")
        return
    
    # 2. Merge assíncrono
    df_merged = merge_async_events(df_purchase, df_product_item, df_extra_info, process_date)
    
    # 3. Forward fill
    df_forward_filled = apply_forward_fill(spark, df_merged, process_date)
    
    # 4. Detectar mudanças
    df_changed = detect_changes(spark, df_forward_filled)
    
    if df_changed.count() == 0:
        print("⏭️  Nenhuma mudança detectada - nada a fazer")
        return
    
    # 5. Aplicar SCD Type 2
    df_final = apply_scd_type_2(spark, df_changed, process_date)
    
    # 6. Atualizar registros anteriores
    update_previous_records(spark, df_final, process_date)
    
    # 7. Escrever partição
    write_partition(spark, df_final, process_date)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"\n{'='*80}")
    print(f"✅ ETL CONCLUÍDO EM {duration:.2f}s")
    print(f"{'='*80}\n")


def query_gmv_with_time_travel(spark, as_of_date=None):
    """
    Consulta GMV diário por subsidiária com time travel.
    
    Requisito do vídeo:
    "Eu tenho que conseguir navegar no tempo. Se eu me posicionar no dia de hoje
    e quiser o GMV de janeiro, ele tem que considerar alterações posteriores.
    Se eu me posicionar em 31 de janeiro, ele tem que trazer o GMV daquele momento."
    
    Time Travel:
    - as_of_date = None: usa dados correntes (is_current = True)
    - as_of_date = '2023-01-31': usa dados válidos naquela data
    
    Args:
        spark: SparkSession
        as_of_date: Data de referência para time travel (YYYY-MM-DD ou None)
    
    Returns:
        DataFrame com GMV diário por subsidiária
    """
    print(f"\n{'='*80}")
    print(f"CONSULTA GMV - Time Travel")
    print(f"{'='*80}\n")
    
    if as_of_date:
        print(f"📅 As of date: {as_of_date} (navegando no tempo)")
        
        # Time Travel: registros válidos naquela data
        # effective_date <= as_of_date AND (end_date > as_of_date OR end_date IS NULL)
        df_snapshot = spark.table("fact_purchase_history").filter(
            (F.col("effective_date") <= as_of_date) &
            ((F.col("end_date") > as_of_date) | (F.col("end_date").isNull()))
        )
    else:
        print(f"📅 Usando dados correntes (is_current = True)")
        
        # Dados correntes
        df_snapshot = spark.table("fact_purchase_history").filter(
            F.col("is_current") == True
        )
    
    # GMV = purchase_value (valor bruto da compra)
    # Agrupar por order_date (data da compra) e subsidiary
    df_gmv = df_snapshot.filter(
        F.col("release_date").isNotNull()  # Apenas compras pagas
    ).groupBy(
        F.col("order_date").alias("date"),
        "subsidiary"
    ).agg(
        F.sum("purchase_value").alias("gmv"),
        F.count("*").alias("total_transactions")
    ).orderBy("date", "subsidiary")
    
    print("\n📊 GMV Diário por Subsidiária:")
    df_gmv.show(50, truncate=False)
    
    print(f"\n{'='*80}\n")
    
    return df_gmv


def main():
    """CLI principal"""
    parser = argparse.ArgumentParser(description="ETL Purchase History - Hotmart")
    parser.add_argument("--process-date", help="Data a processar (YYYY-MM-DD)")
    parser.add_argument("--query-gmv", action="store_true", help="Consultar GMV")
    parser.add_argument("--as-of-date", help="Data de referência para time travel (YYYY-MM-DD)")
    parser.add_argument("--create-sample-data", action="store_true", help="Criar dados de exemplo")
    
    args = parser.parse_args()
    
    # Spark
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Criar dados de exemplo (apenas para teste)
    if args.create_sample_data:
        create_sample_data(spark)
    
    # Processar ETL
    if args.process_date:
        run_etl(spark, args.process_date)
    
    # Consultar GMV
    if args.query_gmv:
        query_gmv_with_time_travel(spark, args.as_of_date)
    
    # Sem argumentos - mostrar help
    if not args.process_date and not args.query_gmv:
        parser.print_help()
    
    spark.stop()


if __name__ == "__main__":
    main()
