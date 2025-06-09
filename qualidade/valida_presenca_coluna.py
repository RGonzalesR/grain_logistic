# Databricks notebook source
def valida_presenca_colunas(df, nome_df):
    colunas_atuais = set(df.columns)
    colunas_ausentes = [col for col in dic_tabela_coluna[nome_df] if col not in colunas_atuais]
    if colunas_ausentes:
        print("🔴 Colunas ausentes:", colunas_ausentes)
        return False
    return True

# COMMAND ----------

# Feito via dicionário devido à limitação do Databricks Community Edition para criar arquivos
# O mais adequado seria a construção de YAML
dic_tabela_coluna = {   
    "bronze.grain_logistic_shipping": [
        "id_envio", "corredor_de_armazenagem","metodo_de_envio", "ligações_do_cliente","avaliação_do_cliente", 
        "preço","qtd_itens","importancia","genero","desconto","peso_g","Chegou_no_tempo","Destino","DataEnvio",
        "dataEntrega","avaliacaoEntrega","data_ingestao"
    ],
    "silver.cliente": [
        "id_envio", "qt_ligacoes_do_cliente", "ds_genero", "nr_avaliacao_do_cliente", "nr_avaliacao_entrega", "data_ingestao"
    ],
    "silver.envios": [
        "id_envio", "id_corredor_de_armazenagem", "ds_metodo_de_envio", "ds_importancia", "nr_importancia", 
        "ds_uf_destino", "in_chegou_no_tempo", "ds_dia_da_semana_envio", "nr_dia_envio", "ds_mes_envio", 
        "nr_ano_envio", "ds_dia_da_semana_entrega", "nr_dia_entrega", "ds_mes_entrega", "nr_ano_entrega", 
        "data_ingestao", "nr_mes_envio", "nr_mes_entrega", "dt_envio", "dt_entrega"
    ],
    "silver.produto": [
        "id_envio", "vl_preco", "qt_itens", "vl_peso_gramas", "pc_desconto", "data_ingestao"
    ],
    "gold.dim_importancia": [
        "id_importancia", "ds_importancia", "nr_importancia"
    ],
    "gold.dim_calendario": [
        "id_calendario", "dt_completa", "nr_ano", "nr_mes", "nr_dia", "ds_dia_da_semana", "ds_mes"
    ],
    "gold.dim_uf": [
        "id_uf", "ds_uf", "sg_uf"
    ],
    "gold.fato_venda": [
        "id_envio", "id_uf", "id_importancia", "id_calendario_envio", "id_calendario_entrega", "ds_corredor_de_armazenagem", 
        "qt_produtos_adquiridos", "vl_preco_unitario", "pc_desconto", "vl_receita_total", "vl_peso_total", 
        "qt_tempo_entrega_dias", "in_chegou_no_tempo"
    ],
    "gold.fs_historico_envios_corredor": [
        "ds_corredor_de_armazenagem", "dt_historico", "qt_envios_acumulado", "qt_total_itens", "qt_media_itens", 
        "vl_total_receita", "vl_media_receita", "vl_peso_medio", "vl_medio_tempo_entrega", "pc_entregas_no_prazo", 
        "vl_medio_ligacoes_cliente", "vl_medio_avaliacao_cliente", "vl_medio_avaliacao_entrega", "qt_envios_feminino", 
        "qt_envios_masculino", "pc_envio_publico_feminino", "pc_envio_publico_masculino"
    ]
}