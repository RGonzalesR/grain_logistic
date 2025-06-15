# Databricks notebook source
!pip install unidecode

# COMMAND ----------

# MAGIC %run ../qualidade/define_schema

# COMMAND ----------

# MAGIC %run ../qualidade/valida_presenca_coluna

# COMMAND ----------

# MAGIC %run ../qualidade/valida_integridade

# COMMAND ----------

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import BooleanType, DateType, DecimalType, DecimalType, IntegerType, StringType
from itertools import chain
import re
import unidecode
# import define_tipo_schema feito via MAGIC run

def padronizar_nome(col):
    col = unidecode.unidecode(col)
    col = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', col)
    col = re.sub(r'\W+', '_', col).lower()
    return col.strip('_')

def transformar_silver(spark: SparkSession, tabela_bronze: str):    
    df_bronze = spark.table(tabela_bronze)

    colunas_renomeadas = [padronizar_nome(c) for c in df_bronze.columns]
    df = df_bronze.toDF(*colunas_renomeadas).distinct()

    UF_LIST = [
        "Acre", "Alagoas", "Amapá", "Amazonas", "Bahia", "Ceará", "Distrito Federal",
        "Espírito Santo", "Goiás", "Maranhão", "Mato Grosso", "Mato Grosso do Sul",
        "Minas Gerais", "Pará", "Paraíba", "Paraná", "Pernambuco", "Piauí",
        "Rio de Janeiro", "Rio Grande do Norte", "Rio Grande do Sul", "Rondônia",
        "Roraima", "Santa Catarina", "São Paulo", "Sergipe", "Tocantins"
    ]

    MESES_DICT = {
        "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
        "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
        "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12
    }

    MESES_MAP = f.create_map(*chain.from_iterable([(f.lit(k), f.lit(v)) for k, v in MESES_DICT.items()]))

    # ENVIO
    df_envios = df.select(
        f.col("id_envio"),
        f.col("corredor_de_armazenagem").alias("id_corredor_de_armazenagem"),
        f.col("metodo_de_envio").alias("ds_metodo_de_envio"),
        f.col("importancia").alias("ds_importancia"),
        f.when(f.col("importancia") == "low", 1)
         .when(f.col("importancia") == "medium", 2)
         .when(f.col("importancia") == "high", 3)
         .alias("nr_importancia"),
        f.when(f.col("destino").isin(UF_LIST), f.col("destino")).otherwise("desconhecido").alias("ds_uf_destino"),
        f.col("chegou_no_tempo").cast(BooleanType()).alias("in_chegou_no_tempo"),
        f.regexp_extract("data_envio", r'^([^,]+)', 1).alias("ds_dia_da_semana_envio"),
        f.regexp_extract("data_envio", r'(\d{1,2}) de', 1).alias("nr_dia_envio"),
        f.regexp_extract("data_envio", r'de ([^ ]+) de', 1).alias("ds_mes_envio"),
        f.regexp_extract("data_envio", r'de (\d{4})$', 1).alias("nr_ano_envio"),
        f.regexp_extract("data_entrega", r'^([^,]+)', 1).alias("ds_dia_da_semana_entrega"),
        f.regexp_extract("data_entrega", r'(\d{1,2}) de', 1).alias("nr_dia_entrega"),
        f.regexp_extract("data_entrega", r'de ([^ ]+) de', 1).alias("ds_mes_entrega"),
        f.regexp_extract("data_entrega", r'de (\d{4})$', 1).alias("nr_ano_entrega"),
        f.current_timestamp().alias("data_ingestao")
    ).withColumn(
        "nr_mes_envio", MESES_MAP[f.col("ds_mes_envio")]
    ).withColumn(
        "nr_mes_entrega", MESES_MAP[f.col("ds_mes_entrega")]
    ).withColumn(
        "dt_envio", f.to_date(
            f.concat_ws("-", f.col("nr_ano_envio"), f.lpad(f.col("nr_mes_envio"), 2, "0"), f.lpad(f.col("nr_dia_envio"), 2, "0"))
        )
    ).withColumn(
        "dt_entrega", f.to_date(
            f.concat_ws("-", f.col("nr_ano_entrega"), f.lpad(f.col("nr_mes_entrega"), 2, "0"), f.lpad(f.col("nr_dia_entrega"), 2, "0"))
        )
    )

    # PRODUTO
    df_produto = df.select(
        f.col("id_envio"),
        f.col("preco").alias("vl_preco"),
        f.col("qtd_itens").alias("qt_itens"),
        f.col("peso_g").alias("vl_peso_gramas"),
        (f.col("desconto") / 100).alias("pc_desconto"),
        f.current_timestamp().alias("data_ingestao")
    )

    # CLIENTE
    df_cliente = df.select(
        f.col("id_envio"),
        f.col("ligacoes_do_cliente").alias("qt_ligacoes_do_cliente"),
        f.col("genero").alias("ds_genero"),
        f.col("avaliacao_do_cliente").alias("nr_avaliacao_do_cliente"),
        f.col("avaliacao_entrega").alias("nr_avaliacao_entrega"),
        f.current_timestamp().alias("data_ingestao")
    )

    if valida_presenca_colunas(df_envios, "silver.envios") and valida_integridade(df_envios, "silver.envios"):
        spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
        salvar_como_tabela(define_tipo_schema(df_envios), "envios")
        print(f"✅ Tabela envios salva com sucesso.")
    else:
        print(f"Tabela envios não salva. Cheque os logs.")

    if valida_presenca_colunas(df_produto, "silver.produto") and valida_integridade(df_produto, "silver.produto"):
        spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
        salvar_como_tabela(define_tipo_schema(df_produto), "produto")
        print(f"✅ Tabela produto salva com sucesso.")
    else:
        print(f"Tabela produto não salva. Cheque os logs.")

    if valida_presenca_colunas(df_cliente, "silver.cliente") and valida_integridade(df_cliente, "silver.cliente"):
        spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
        salvar_como_tabela(define_tipo_schema(df_cliente), "cliente")
        print(f"✅ Tabela cliente salva com sucesso.")
    else:
        print(f"Tabela cliente não salva. Cheque os logs.")


def salvar_como_tabela(df, nome_tabela):
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"silver.{nome_tabela}")
    )

spark = SparkSession.builder.getOrCreate()
tabela_origem = dbutils.widgets.get("tabela_bronze")
transformar_silver(spark, tabela_origem)


# COMMAND ----------


