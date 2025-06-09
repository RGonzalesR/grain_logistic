# Databricks notebook source
# MAGIC %run ../qualidade/define_schema

# COMMAND ----------

# MAGIC %run ../qualidade/valida_integridade

# COMMAND ----------

# MAGIC %run ../qualidade/valida_presenca_coluna

# COMMAND ----------

from pyspark.sql import SparkSession, functions as f
from delta.tables import DeltaTable

def criar_dim_importancia(spark: SparkSession, tabela_envios: str):
    nome_dim = "gold.dim_importancia"

    df_dim_importancia = (
        spark.table(tabela_envios)
        .select(
            f.sha2(
                f.concat_ws("-", f.col("ds_importancia"), f.col("nr_importancia").cast("string")),
                256
            ).alias("id_importancia"),
            f.col("ds_importancia"),
            f.col("nr_importancia")
        )
        .distinct()
    )

    df_dim_importancia = define_tipo_schema(df_dim_importancia)

    if not (valida_integridade(df_dim_importancia, nome_dim) and valida_presenca_colunas(df_dim_importancia, nome_dim)):
        print(f"Tabela dim_importancia não salva. Cheque os logs.")
        return None
        
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

    if not spark._jsparkSession.catalog().tableExists(nome_dim):
        (
            df_dim_importancia
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(nome_dim)
        )
        print(f"✅ Tabela dim_importancia criada com sucesso.")
    else:
        dim_importancia_table = DeltaTable.forName(spark, nome_dim)

        dim_importancia_table.alias("target").merge(
            source=df_dim_importancia.alias("source"),
            condition="""
                target.ds_importancia = source.ds_importancia AND
                target.nr_importancia = source.nr_importancia
            """
        ).whenNotMatchedInsert(
            values={
                "id_importancia": f.col("source.id_importancia"),
                "ds_importancia": f.col("source.ds_importancia"),
                "nr_importancia": f.col("source.nr_importancia")
            }
        ).execute()
        print(f"✅ Tabela dim_importancia atualizada com sucesso.")

spark = SparkSession.builder.getOrCreate()
criar_dim_importancia(spark, "silver.envios")
