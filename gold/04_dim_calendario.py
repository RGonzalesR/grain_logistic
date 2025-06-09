# Databricks notebook source
# MAGIC %run ../qualidade/define_schema

# COMMAND ----------

# MAGIC %run ../qualidade/valida_integridade

# COMMAND ----------

# MAGIC %run ../qualidade/valida_presenca_coluna

# COMMAND ----------

from pyspark.sql import SparkSession, functions as f
from delta.tables import DeltaTable

def criar_dim_calendario(spark: SparkSession, tabela_envios: str):
    nome_dim = "gold.dim_calendario"

    # Seleciona datas de envio
    df_envios = (
        spark.table(tabela_envios)
        .select(
            f.col("dt_envio").alias("dt_completa"),
            f.col("ds_dia_da_semana_envio").alias("ds_dia_da_semana"),
            f.col("nr_dia_envio").alias("nr_dia"),
            f.col("ds_mes_envio").alias("ds_mes"),
            f.col("nr_mes_envio").alias("nr_mes"),
            f.col("nr_ano_envio").alias("nr_ano")
        )
    )

    # Seleciona datas de entrega
    df_entregas = (
        spark.table(tabela_envios)
        .select(
            f.col("dt_entrega").alias("dt_completa"),
            f.col("ds_dia_da_semana_entrega").alias("ds_dia_da_semana"),
            f.col("nr_dia_entrega").alias("nr_dia"),
            f.col("ds_mes_entrega").alias("ds_mes"),
            f.col("nr_mes_entrega").alias("nr_mes"),
            f.col("nr_ano_entrega").alias("nr_ano")
        )
    )

    # União e criação da dimensão calendario
    df_dim_calendario = (
        df_envios.union(df_entregas)
        .distinct()
        .withColumn("id_calendario", f.sha2(f.col("dt_completa").cast("string"), 256))
    )

    df_dim_calendario = define_tipo_schema(df_dim_calendario)

    if not (valida_integridade(df_dim_calendario, nome_dim) and valida_presenca_colunas(df_dim_calendario, nome_dim)):
        print(f"Tabela dim_calendario não salva. Cheque os logs.")
        return None
        
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

    if not spark._jsparkSession.catalog().tableExists(nome_dim):
        (
            df_dim_calendario.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(nome_dim)
        )
        print(f"✅ Tabela dim_calendario criada com sucesso.")
    else:
        dim_calendario_table = DeltaTable.forName(spark, nome_dim)

        dim_calendario_table.alias("target").merge(
            source=df_dim_calendario.alias("source"),
            condition="target.dt_completa = source.dt_completa"
        ).whenNotMatchedInsert(
            values={
                "id_calendario": f.col("source.id_calendario"),
                "dt_completa": f.col("source.dt_completa"),
                "nr_ano": f.col("source.nr_ano"),
                "nr_mes": f.col("source.nr_mes"),
                "nr_dia": f.col("source.nr_dia"),
                "ds_dia_da_semana": f.col("source.ds_dia_da_semana"),
                "ds_mes": f.col("source.ds_mes")
            }
        ).execute()
        print(f"✅ Tabela dim_calendario atualizada com sucesso.")

spark = SparkSession.builder.getOrCreate()
criar_dim_calendario(spark, "silver.envios")
