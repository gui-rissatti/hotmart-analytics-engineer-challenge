import sys
import logging
from datetime import datetime, timedelta, date

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (StructType, StructField, StringType, DateType, DecimalType, BooleanType, TimestampType)

# =========================
# Parâmetros do Job
# =========================
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Overwrite dinâmico só na partição
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

logger = logging.getLogger('consolidation')
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

logger.info("Início do job de consolidação diária D-1")

# =========================
# Constantes de I/O
# =========================
S3_SOURCE_PURCHASES = "s3://hotmart-datalake-prod/transactions/purchases"
S3_SOURCE_ITEMS = "s3://hotmart-datalake-prod/transactions/product_items"
S3_SOURCE_EXTRA = "s3://hotmart-datalake-prod/transactions/purchase_extra_info"
S3_TARGET_CONSOLIDATED = "s3://hotmart-datalake-prod/tables/consolidated_purchase_daily"

# =========================
# Datas de processamento
# =========================
# D-1
process_date = datetime.utcnow().date()
prev_date = process_date - timedelta(days=1)
logger.info(f"process_date={process_date} (HOJE/D-0) | prev_date={prev_date} (ONTEM/D-1)")

# =========================
# Esquema do snapshot final
# =========================
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

def empty_snapshot_df():
    return spark.createDataFrame([], SNAPSHOT_SCHEMA)

# =========================
# Funções auxiliares
# =========================
def read_all_and_filter_date(path: str, dt: date):
    """
    Leitura resiliente: lê o caminho e filtra transaction_date == process_date.
    Isso evita pressupor layout de partições na camada raw.
    """
    df = spark.read.parquet(path)
    return df.filter(F.to_date(F.col("transaction_date")) == F.lit(dt.isoformat()))

def with_event_ts(df, ts_cols=("event_ts", "transaction_ts", "updated_at", "ingestion_ts")):
    """
    Gera/normaliza uma coluna 'event_ts' para ordenar eventos do dia.
    """
    for c in ts_cols:
        if c in df.columns:
            return df.withColumn("event_ts", F.col(c).cast("timestamp"))
    if "transaction_date" in df.columns:
        return df.withColumn(
            "event_ts",
            F.to_timestamp(F.concat_ws(" ", F.col("transaction_date").cast("string"), F.lit("23:59:59")))
        )
    return df.withColumn("event_ts", F.current_timestamp())

def last_event_of_day(df, key_col="purchase_id"):
    """
    Mantém somente o último evento do dia por chave usando row_number decrescente de event_ts.
    """
    w = Window.partitionBy(key_col).orderBy(F.col("event_ts").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

def safe_bool_paid(col_status):
    """
    Deriva indicador de pagamento a partir do status textual.
    Ajuste conforme enum real quando houver.
    """
    return (F.upper(col_status).isin("PAID", "PAGO", "APPROVED", "APROVADO", "SETTLED", "LIQUIDADO")).alias("is_paid")

def load_prev_snapshot(dt_prev: date):
    """
    Lê snapshot do dia anterior; se não existir, retorna DF vazio.
    """
    prev_path = f"{S3_TARGET_CONSOLIDATED}/snapshot_date={dt_prev.isoformat()}"
    try:
        return spark.read.schema(SNAPSHOT_SCHEMA).parquet(prev_path)
    except Exception:
        logger.warning(f"Snapshot anterior ausente: {prev_path}")
        return empty_snapshot_df()

# =========================
# Leitura D-1 das 3 fontes
# =========================
purchases_raw = read_all_and_filter_date(S3_SOURCE_PURCHASES, process_date)
items_raw = read_all_and_filter_date(S3_SOURCE_ITEMS, process_date)
extra_raw = read_all_and_filter_date(S3_SOURCE_EXTRA, process_date)

# Normaliza event_ts e mantém só o último evento do dia por purchase_id
purchases_day = last_event_of_day(with_event_ts(purchases_raw))
items_day = with_event_ts(items_raw)  # manteremos apenas carimbo por compra
extra_day = last_event_of_day(with_event_ts(extra_raw))

logger.info(f"Eventos do dia: purchases={purchases_day.count()} | items={items_day.count()} | extra={extra_day.count()}")

# =========================
# Seleções por fonte
# =========================
# Purchases: campos principais esperados
p_cols = [c for c in ["purchase_id", "buyer_id", "producer_id", "order_date", "release_date", "status", "purchase_value"] if c in purchases_day.columns]
purchases_sel = purchases_day.select(*p_cols, "event_ts").withColumnRenamed("event_ts", "src_purchase_ts")

# Items: apenas timestamp agregado por purchase_id para controle
items_agg = (
    items_day.groupBy("purchase_id")
    .agg(F.max("event_ts").alias("src_items_ts"))
)

# Extra: subsidiária por purchase_id
subs_col = "subsidiary" if "subsidiary" in extra_day.columns else ("subsidiaria" if "subsidiaria" in extra_day.columns else None)
if subs_col:
    extra_sel = extra_day.select("purchase_id", subs_col, "event_ts").withColumnRenamed(subs_col, "subsidiary")
else:
    extra_sel = extra_day.select("purchase_id", F.lit(None).cast(StringType()).alias("subsidiary"), "event_ts")
extra_agg = extra_sel.groupBy("purchase_id").agg(
    F.max("subsidiary").alias("subsidiary"),
    F.max("event_ts").alias("src_extra_ts")
)

# =========================
# Chaves impactadas e snapshot anterior
# =========================
keys_today = (
    purchases_sel.select("purchase_id")
    .unionByName(items_agg.select("purchase_id"))
    .unionByName(extra_agg.select("purchase_id"))
    .distinct()
)

prev_snap = (
    load_prev_snapshot(prev_date)
    .select("purchase_id", "buyer_id", "producer_id", "order_date", "release_date", "status", "is_paid", "subsidiary", "gmv")
    .withColumnRenamed("gmv", "gmv_prev")
)

# =========================
# Consolidação e carry-forward (lógica de atualização)
# =========================
base_today = (
    keys_today
    .join(purchases_sel, on="purchase_id", how="left")
    .join(items_agg, on="purchase_id", how="left")
    .join(extra_agg, on="purchase_id", how="left")
    .join(prev_snap, on="purchase_id", how="left")
)

consolidated = base_today.select(
    F.col("purchase_id"),
    F.lit(process_date).cast(DateType()).alias("snapshot_date"),

    F.coalesce(F.col("buyer_id"), F.col("prev_snap.buyer_id")).alias("buyer_id"),
    F.coalesce(F.col("producer_id"), F.col("prev_snap.producer_id")).alias("producer_id"),
    F.coalesce(F.to_date("order_date"), F.col("prev_snap.order_date")).alias("order_date"),
    F.coalesce(F.to_date("release_date"), F.col("prev_snap.release_date")).alias("release_date"),
    F.coalesce(F.col("status"), F.col("prev_snap.status")).alias("status"),
    F.coalesce(safe_bool_paid(F.col("status")), F.col("prev_snap.is_paid")).alias("is_paid"),
    F.coalesce(F.col("subsidiary"), F.col("prev_snap.subsidiary")).alias("subsidiary"),

    # GMV via purchase_value; se ausente no dia, manter valor anterior
    F.coalesce(F.col("purchase_value").cast(DecimalType(18, 2)), F.col("prev_snap.gmv")).alias("gmv"),

    F.col("src_purchase_ts").cast("timestamp"),
    F.col("src_items_ts").cast("timestamp"),
    F.col("src_extra_ts").cast("timestamp"),
)

# =========================
# Escrita no S3 particionado
# =========================
logger.info(f"Gravando partição snapshot_date={process_date} em {S3_TARGET_CONSOLIDATED}")
(
    consolidated
    .repartition(1)
    .write
    .mode("overwrite")
    .partitionBy("snapshot_date")
    .parquet(S3_TARGET_CONSOLIDATED)
)

logger.info("Consolidação concluída com sucesso.")
job.commit()
