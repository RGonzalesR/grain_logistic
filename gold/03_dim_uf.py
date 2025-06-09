# Databricks notebook source
# MAGIC %run ../qualidade/define_schema

# COMMAND ----------

# MAGIC %run ../qualidade/valida_integridade

# COMMAND ----------

# MAGIC %run ../qualidade/valida_presenca_coluna

# COMMAND ----------

from pyspark.sql import SparkSession, functions as f
from delta.tables import DeltaTable
from itertools import chain

def criar_dim_uf(spark: SparkSession, tabela_envios: str):

    MAPA_UF = {
        "Acre": "AC",
        "Alagoas": "AL",
        "Amazonas": "AM",
        "Bahia": "BA",
        "Ceará": "CE",
        "Distrito Federal": "DF",
        "Espírito Santo": "ES",
        "Goiás": "GO",
        "Maranhão": "MA",
        "Mato Grosso": "MT",
        "Mato Grosso do Sul": "MS",
        "Minas Gerais": "MG",
        "Pará": "PA",
        "Paraíba": "PB",
        "Paraná": "PR",
        "Pernambuco": "PE",
        "Piauí": "PI",
        "Rio de Janeiro": "RJ",
        "Rio Grande do Norte": "RN",
        "Rio Grande do Sul": "RS",
        "Rondônia": "RO",
        "Roraima": "RR",
        "Santa Catarina": "SC",
        "São Paulo": "SP",
        "Sergipe": "SE",
        "Tocantins": "TO"
    }

    map_expr = f.create_map([f.lit(x) for x in chain(*MAPA_UF.items())])

    df_dim_uf = (
        spark.table(tabela_envios)
        .select(
            f.sha2(f.col("ds_uf_destino"), 256).alias("id_uf"),
            f.col("ds_uf_destino").alias("ds_uf"), 
            map_expr.getItem(f.col("ds_uf_destino")).alias("sg_uf")
        )
        .distinct()
    )

    df_dim_uf = define_tipo_schema(df_dim_uf)

    if not (valida_integridade(df_dim_uf, "gold.dim_uf") and valida_presenca_colunas(df_dim_uf, "gold.dim_uf")):
        print(f"Tabela dim_uf não salva. Cheque os logs.")
        return None
        
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

    if not spark._jsparkSession.catalog().tableExists("gold.dim_uf"):
        (
            df_dim_uf
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("gold.dim_uf")
        )
        print(f"✅ Tabela dim_uf criada com sucesso.")
    else:
        dim_uf_table = DeltaTable.forName(spark, "gold.dim_uf")

        dim_uf_table.alias("target").merge(
            source=df_dim_uf.alias("source"),
            condition="target.ds_uf = source.ds_uf"
        ).whenNotMatchedInsert(
            values={
                "id_uf": f.col("source.id_uf"),
                "ds_uf": f.col("source.ds_uf"),
                "sg_uf": f.col("source.sg_uf")
            }
        ).execute()
        print(f"✅ Tabela dim_uf atualizada com sucesso.")

spark = SparkSession.builder.getOrCreate()
criar_dim_uf(spark, "silver.envios")
