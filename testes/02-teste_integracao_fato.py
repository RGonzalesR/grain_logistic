# Databricks notebook source
try:
    df_fato = spark.table("gold.fato_venda")
    df_dim_uf = spark.table("gold.dim_uf")
    df_dim_imp = spark.table("gold.dim_importancia")
    df_dim_calendario = spark.table("gold.dim_calendario")

    fato_count = df_fato.count()
    teste_sem_erro = True

    if df_fato.join(df_dim_uf, "id_uf").count() != fato_count:
        print("🔴 Inconsistência com dim_uf")
        teste_sem_erro = False

    if df_fato.join(df_dim_imp, "id_importancia").count() != fato_count:
        print("🔴 Inconsistência com dim_importancia")
        teste_sem_erro = False

    if df_fato.join(df_dim_calendario, df_fato["id_calendario_envio"] == df_dim_calendario["id_calendario"]).count() != fato_count:
        print("🔴 Inconsistência com id_calendario_envio")
        teste_sem_erro = False

    if df_fato.join(df_dim_calendario, df_fato["id_calendario_entrega"] == df_dim_calendario["id_calendario"]).count() != fato_count:
        print("🔴 Inconsistência com id_calendario_entrega")
        teste_sem_erro = False

    if teste_sem_erro:
        print("✅ Integração entre fato e dimensões validada")

except Exception as e:
    print("Erro ao executar teste de integração:", str(e))