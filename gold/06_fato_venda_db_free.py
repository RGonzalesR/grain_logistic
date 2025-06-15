# Databricks notebook source
# MAGIC %run ../qualidade/define_schema

# COMMAND ----------

# MAGIC %run ../qualidade/valida_integridade

# COMMAND ----------

# MAGIC %run ../qualidade/valida_presenca_coluna

# COMMAND ----------

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

def criar_fato_venda(spark: SparkSession):
    nome_fato = "gold.fato_venda"

    # Tabelas necessárias
    df_envios = spark.table("silver.envios")
    df_produto = spark.table("silver.produto")
    df_dim_uf = spark.table("gold.dim_uf")
    df_dim_importancia = spark.table("gold.dim_importancia")
    df_dim_calendario = spark.table("gold.dim_calendario")

    # Join métricas
    df_venda = df_envios.join(df_produto, "id_envio")

    # Join dimensão UF
    df_venda = df_venda.join(
        df_dim_uf,
        df_venda["ds_uf_destino"] == df_dim_uf["ds_uf"],
        how="left"
    )

    # Join dimensão importância
    df_venda = df_venda.join(
        df_dim_importancia,
        on=["ds_importancia", "nr_importancia"],
        how="left"
    )

    # Join dimensão tempo (envio)
    df_venda = df_venda.join(
        df_dim_calendario.select(f.col("dt_completa"), f.col("id_calendario").alias("id_calendario_envio")),
        df_venda["dt_envio"] == f.col("dt_completa"),
        how="left"
    ).drop("dt_completa")

    # Join dimensão tempo (entrega)
    df_venda = df_venda.join(
        df_dim_calendario.select(f.col("dt_completa"), f.col("id_calendario").alias("id_calendario_entrega")),
        df_venda["dt_entrega"] == f.col("dt_completa"),
        how="left"
    )

    df_fato = df_venda.select(
        f.col("id_envio"),
        f.col("id_uf"),
        f.col("id_importancia"),
        f.col("id_calendario_envio"),
        f.col("id_calendario_entrega"),
        f.col("id_corredor_de_armazenagem").alias("ds_corredor_de_armazenagem"),
        f.col("qt_itens").alias("qt_produtos_adquiridos"),
        f.col("vl_preco").alias("vl_preco_unitario"),
        f.col("pc_desconto"),
        (f.col("vl_preco") * f.col("qt_itens") * (1 - f.col("pc_desconto")))
            .cast(DecimalType(10, 2))
            .alias("vl_receita_total"),
        (f.col("vl_peso_gramas") * f.col("qt_itens")).alias("vl_peso_total"),
        f.datediff("dt_entrega", "dt_envio").alias("qt_tempo_entrega_dias"),
        f.col("in_chegou_no_tempo")
    )

    df_fato = define_tipo_schema(df_fato)

    if not (valida_integridade(df_fato, nome_fato) and valida_presenca_colunas(df_fato, nome_fato)):
        print(f"Tabela fato_venda não salva. Cheque os logs.")
        return None
        
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
    
    tabelas = spark.catalog.listTables("gold")
    nomes_tabelas = [t.name for t in tabelas]

    if nome_fato not in nomes_tabelas:
        (
            df_fato
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            # Aqui seria bom particionar a tabela por tempo, mas não há dados suficientes para sabermos qual melhor opção
            # .partitionBy("id_calendario_envio")
            .partitionBy("id_uf")
            .saveAsTable(nome_fato)
        )
        print(f"✅ Tabela fato_venda criada com sucesso.")

        # Para melhor aplicação, seria bom termos metrificação de colunas mais consultadas
        # Também seria bom aplicar Vacuum, porém não há longo pra no Community Edition
        spark.sql(f"""
            OPTIMIZE {nome_fato}
            ZORDER BY (vl_receita_total, qt_tempo_entrega_dias)
        """)
        print(f"✅ Aplicação de ZORDER em fato_venda.")
    else:
        fato_venda_table = DeltaTable.forName(spark, nome_fato)
        fato_venda_table.alias("target").merge(
            source=df_fato.alias("source"),
            condition="target.id_envio = source.id_envio"
        ).whenNotMatchedInsert(
            values={col: f.col(f"source.{col}") for col in df_fato.columns}
        ).execute()
        print(f"✅ Tabela fato_venda atualizada com sucesso.")

spark = SparkSession.builder.getOrCreate()
criar_fato_venda(spark)


# COMMAND ----------


