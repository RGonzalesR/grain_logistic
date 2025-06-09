# Databricks notebook source
# MAGIC %run ../qualidade/define_schema

# COMMAND ----------

# MAGIC %run ../qualidade/valida_integridade

# COMMAND ----------

# MAGIC %run ../qualidade/valida_presenca_coluna

# COMMAND ----------

from pyspark.sql import SparkSession, functions as f
import datetime

def criar_feature_store(spark: SparkSession):
    nome_tabela = "gold.fs_historico_envios_corredor"

    df_fato = spark.table("gold.fato_venda")
    df_calendario = spark.table("gold.dim_calendario")
    df_cliente = spark.table("silver.cliente")

    # Join origens
    df_full = (
        df_fato
        .join(df_cliente, "id_envio", "left")
        .join(df_calendario, df_fato["id_calendario_envio"] == df_calendario["id_calendario"], "left")
    )

    # Aqui, calculamos o período de exploração da feature store
    min_date, max_date = df_full.select(f.min("dt_completa"), f.max("dt_completa")).first()
    data_range = [(min_date + datetime.timedelta(days=i),) for i in range((max_date - min_date).days + 1)]
    df_datas = spark.createDataFrame(data_range, ["dt_historico"])

    df_corredores = df_fato.select("ds_corredor_de_armazenagem").distinct()
    df_corredor_data = df_corredores.crossJoin(df_datas)

    # Renomeação para evitar conflitos no select
    df_full = df_full.selectExpr(
        "ds_corredor_de_armazenagem as ds_corr",
        "dt_completa", "qt_produtos_adquiridos", "vl_receita_total",
        "vl_peso_total", "qt_tempo_entrega_dias", "in_chegou_no_tempo",
        "qt_ligacoes_do_cliente", "nr_avaliacao_do_cliente",
        "nr_avaliacao_entrega", "ds_genero"
    )

    cond_nao_futura = f.col("dt_completa") <= f.col("dt_historico")

    # Aqui, possibilidade de utilização de join com range
    # A criação de origens incrementais tende a melhorar a performance deste cruzamento. A morosidade é por lermos uma base full
    df_feature = (
        df_corredor_data
        .join(df_full, df_full["ds_corr"] == df_corredor_data["ds_corredor_de_armazenagem"], "left")
        .groupBy("ds_corredor_de_armazenagem", "dt_historico")
        .agg(
            f.count(f.when(cond_nao_futura, f.col("dt_completa"))).alias("qt_envios_acumulado"),
            f.sum(f.when(cond_nao_futura, f.col("qt_produtos_adquiridos"))).alias("qt_total_itens"),
            f.avg(f.when(cond_nao_futura, f.col("qt_produtos_adquiridos"))).alias("qt_media_itens"),
            f.sum(f.when(cond_nao_futura, f.col("vl_receita_total"))).alias("vl_total_receita"),
            f.avg(f.when(cond_nao_futura, f.col("vl_receita_total"))).alias("vl_media_receita"),
            f.avg(f.when(cond_nao_futura, f.col("vl_peso_total"))).alias("vl_peso_medio"),
            f.avg(f.when(cond_nao_futura, f.col("qt_tempo_entrega_dias"))).alias("vl_medio_tempo_entrega"),
            f.avg(f.when(cond_nao_futura, f.col("in_chegou_no_tempo").cast("double"))).alias("pc_entregas_no_prazo"),
            f.avg(f.when(cond_nao_futura, f.col("qt_ligacoes_do_cliente"))).alias("vl_medio_ligacoes_cliente"),
            f.avg(f.when(cond_nao_futura, f.col("nr_avaliacao_do_cliente"))).alias("vl_medio_avaliacao_cliente"),
            f.avg(f.when(cond_nao_futura, f.col("nr_avaliacao_entrega"))).alias("vl_medio_avaliacao_entrega"),
            f.sum(f.when((f.col("ds_genero") == "F") & cond_nao_futura, 1).otherwise(0)).alias("qt_envios_feminino"),
            f.sum(f.when((f.col("ds_genero") == "M") & cond_nao_futura, 1).otherwise(0)).alias("qt_envios_masculino")
        )
        .withColumn("pc_envio_publico_feminino", f.col("qt_envios_feminino") / f.col("qt_envios_acumulado"))
        .withColumn("pc_envio_publico_masculino", f.col("qt_envios_masculino") / f.col("qt_envios_acumulado"))
    )

    df_feature = define_tipo_schema(df_feature)

    if not (valida_integridade(df_feature, nome_tabela) and valida_presenca_colunas(df_feature, nome_tabela)):
        print(f"Tabela fs_historico_envios_corredor não salva. Cheque os logs.")
        return None

    (
        df_feature
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("ds_corredor_de_armazenagem")
        .saveAsTable(nome_tabela)
    )
    print(f"✅ Tabela fs_historico_envios_corredor escrita com sucesso.")

    # Para melhor aplicação, seria bom termos metrificação de colunas mais consultadas
    # Também seria bom aplicar Vacuum, porém não há longo pra no Community Edition
    spark.sql(f"""
        OPTIMIZE {nome_tabela}
        ZORDER BY (qt_total_itens, vl_peso_medio)
    """)
    print(f"✅ Aplicação de ZORDER em fs_historico_envios_corredor.")

spark = SparkSession.builder.getOrCreate()
criar_feature_store(spark)


# COMMAND ----------

