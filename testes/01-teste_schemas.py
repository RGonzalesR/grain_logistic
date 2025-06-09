# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# O mais correto seria usar assert, porém no Databricks Community Edition temos erros de traceback

def tipo_esperado_por_prefixo(coluna, nome_tabela):
    if nome_tabela.startswith("bronze"): 
        if coluna == "data_ingestao":
            return TimestampType()
        else:
            return StringType()
    if coluna.startswith("in_"): return BooleanType()
    if coluna.startswith(("ds_", "id_", "sg_")): return StringType()
    if coluna.startswith("dt_"): return DateType()
    if coluna.startswith("vl_"): return DecimalType(10, 2)
    if coluna.startswith("pc_"): return DecimalType(6, 4)
    if coluna.startswith(("qt_", "nr")): return IntegerType()
    return None

def validar_schema(nome_tabela: str, colunas_esperadas: list):
    try:
        df = spark.table(nome_tabela)
        schema = dict(df.dtypes)
        colunas_ausentes = [col for col in colunas_esperadas if col not in schema]

        if colunas_ausentes:
            print(f"🔴 Tabela {nome_tabela} com colunas ausentes:")
            print(colunas_ausentes)
            return False

        erros_tipo = []
        for col in colunas_esperadas:
            tipo_esperado = tipo_esperado_por_prefixo(col, nome_tabela)
            if tipo_esperado:
                tipo_real = df.schema[col].dataType
                if tipo_real != tipo_esperado:
                    erros_tipo.append((col, str(tipo_real), str(tipo_esperado)))

        if erros_tipo:
            print(f"🔴 Tabela {nome_tabela} com tipos inconsistentes:")
            for col, tipo_real, tipo_esperado in erros_tipo:
                print(f" - {col}: {tipo_real} != {tipo_esperado}")
            return False

        print(f"✅ Schema válido para {nome_tabela}")
        return True

    except Exception as e:
        print(f"Erro ao validar {nome_tabela}: {str(e)}")
        return False


tabelas_para_testar = {
    "bronze.grain_logistic_shipping": [
        "id_envio", "corredor_de_armazenagem", "metodo_de_envio", "ligações_do_cliente", 
        "avaliação_do_cliente", "preço", "qtd_itens", "importancia", "genero", "desconto", 
        "peso_g", "Chegou_no_tempo", "Destino", "DataEnvio", "dataEntrega", "avaliacaoEntrega", "data_ingestao"
    ],
    "silver.envios": [
        "id_envio", "id_corredor_de_armazenagem", "ds_metodo_de_envio",
        "ds_importancia", "nr_importancia", "ds_uf_destino",
        "in_chegou_no_tempo", "dt_envio", "dt_entrega"
    ],
    "silver.produto": [
        "id_envio", "vl_preco", "qt_itens", "vl_peso_gramas", "pc_desconto"
    ],
    "silver.cliente": [
        "id_envio", "qt_ligacoes_do_cliente", "ds_genero",
        "nr_avaliacao_do_cliente", "nr_avaliacao_entrega"
    ],
    "gold.dim_uf": ["id_uf", "ds_uf", "sg_uf"],
    "gold.dim_importancia": ["id_importancia", "ds_importancia", "nr_importancia"],
    "gold.dim_calendario": ["id_calendario", "dt_completa", "nr_ano", "nr_mes", "nr_dia", "ds_dia_da_semana", "ds_mes"],
    "gold.fato_venda": [
        "id_envio", "id_uf", "id_importancia", "id_calendario_envio", "id_calendario_entrega",
        "ds_corredor_de_armazenagem", "qt_produtos_adquiridos", "vl_preco_unitario",
        "pc_desconto", "vl_receita_total", "vl_peso_total", "qt_tempo_entrega_dias", "in_chegou_no_tempo"
    ],
    "gold.fs_historico_envios_corredor": [
        "ds_corredor_de_armazenagem", "dt_historico", "qt_envios_acumulado", "qt_total_itens", "qt_media_itens", 
        "vl_total_receita", "vl_media_receita", "vl_peso_medio", "vl_medio_tempo_entrega", "pc_entregas_no_prazo", 
        "vl_medio_ligacoes_cliente", "vl_medio_avaliacao_cliente", "vl_medio_avaliacao_entrega", "qt_envios_feminino", 
        "qt_envios_masculino", "pc_envio_publico_feminino", "pc_envio_publico_masculino"
    ]
}

for nome_tabela, colunas in tabelas_para_testar.items():
    validar_schema(nome_tabela, colunas)
