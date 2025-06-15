# Databricks notebook source
# MAGIC %run ../qualidade/valida_presenca_coluna

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType
from pyspark.sql.functions import current_timestamp

def ingestao_bronze(spark: SparkSession, caminho_raw: str, nome_tabela: str):
    df_bronze = (
        spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("delimiter", ";")
            .csv(caminho_raw)
    )

    usuario = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

    # Fixamos todas as tabelas como string para evitarmos perder dados na transformação
    # Controle de ingestão feito por current_timestamp, assim sabemos da última atualização
    df_bronze = (
        df_bronze
        .select(*[f.col(col).cast(StringType()).alias(col) for col in df_bronze.columns])
        .withColumn("data_ingestao", current_timestamp())
        .withColumn("fonte_dado", f.lit(caminho_raw))
        .withColumn("usuario_ingestao", f.lit(usuario))
        .withColumn("modo_ingestao", f.lit("overwrite"))
    )

    if valida_presenca_colunas(df_bronze, "bronze." + nome_tabela):
        spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

        (
            df_bronze.write
                .format("delta")
                .mode("overwrite")  # Usamos "append" se quisermos histórico acumulado. Mais recomendado para incremental
                .partitionBy("data_ingestao")
                .option("mergeSchema", "true")
                .saveAsTable(f"bronze.{nome_tabela}")
        )
        print(f"✅ Tabela {nome_tabela} salva com sucesso.")

        spark.sql(f"""
            OPTIMIZE bronze.{nome_tabela}
            ZORDER BY (dataEntrega, DataEnvio)
        """)
    else:
        print(f"Tabela {nome_tabela} não salva. Cheque os logs.")

spark = SparkSession.builder.getOrCreate()
caminho_raw = dbutils.widgets.get("caminho_raw")
nome_tabela = dbutils.widgets.get("nome_tabela")

ingestao_bronze(spark, caminho_raw, nome_tabela)

# COMMAND ----------


