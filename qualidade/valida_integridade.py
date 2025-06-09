# Databricks notebook source
def valida_integridade(df, tabela_nome):
    print(f"\nValidando dados da tabela `{tabela_nome}`:")

    # Feito via dicionário devido à limitação do Databricks Community Edition para criar arquivos
    # O mais adequado seria a construção de YAML
    # Bronze não elencada por desejarmos o arquivo mais próximo possível do presente na landing
    regras = {
        # --- SILVER ---
        "silver.envios": {
            "colunas_obrigatorias": ["id_envio", "dt_envio", "dt_entrega"],
            "colunas_chave": ["id_envio"],
            "negocio": [
                ("Data de entrega ≥ data de envio", f.col("dt_entrega") >= f.col("dt_envio"))
            ]
        },
        "silver.produto": {
            "colunas_obrigatorias": ["id_envio", "vl_preco", "qt_itens"],
            "colunas_chave": ["id_envio"],
            "negocio": [
                ("Preço > 0", f.col("vl_preco") > 0),
                ("Quantidade ≥ 1", f.col("qt_itens") >= 1)
            ]
        },
        "silver.cliente": {
            "colunas_obrigatorias": ["id_envio", "ds_genero", "nr_avaliacao_do_cliente"],
            "colunas_chave": ["id_envio"],
            "negocio": [
                ("Avaliação cliente entre 0 e 5", f.col("nr_avaliacao_do_cliente").between(0, 5)),
                ("Avaliação entrega entre 0 e 5", f.col("nr_avaliacao_entrega").between(0, 5))
            ]
        },

        # --- GOLD ---
        "gold.dim_uf": {
            "colunas_obrigatorias": ["id_uf", "ds_uf"],
            "colunas_chave": ["id_uf"],
            "negocio": []
        },
        "gold.dim_importancia": {
            "colunas_obrigatorias": ["id_importancia", "ds_importancia", "nr_importancia"],
            "colunas_chave": ["id_importancia"],
            "negocio": [
                ("Importância entre 1 e 3", f.col("nr_importancia").between(1, 3))
            ]
        },
        "gold.dim_calendario": {
            "colunas_obrigatorias": ["id_calendario", "dt_completa", "nr_dia", "nr_mes", "nr_ano"],
            "colunas_chave": ["id_calendario"],
            "negocio": [
                ("Dia entre 1 e 31", f.col("nr_dia").between(1, 31)),
                ("Mês entre 1 e 12", f.col("nr_mes").between(1, 12)),
                ("Ano >= 2000", f.col("nr_ano") >= 2000)
            ]
        },
        "gold.fato_venda": {
            "colunas_obrigatorias": ["id_envio", "id_calendario_envio", "id_calendario_entrega", "vl_receita_total", "qt_tempo_entrega_dias"],
            "colunas_chave": ["id_envio", "id_calendario_envio", "id_calendario_entrega", "id_importancia", "id_uf"],
            "negocio": [
                ("Receita total > 0", f.col("vl_receita_total") > 0),
                ("Tempo de entrega > 0", f.col("qt_tempo_entrega_dias") > 0)
            ]
        },
        "gold.fs_historico_envios_corredor": {
            "colunas_obrigatorias": [
                "ds_corredor_de_armazenagem", "dt_historico", "qt_envios_acumulado"
            ],
            "colunas_chave": ["ds_corredor_de_armazenagem", "dt_historico"],
            "negocio": [
                ("Envios acumulados ≥ 0", f.col("qt_envios_acumulado") >= 0),
                ("% entregas no prazo entre 0 e 1", f.col("pc_entregas_no_prazo").between(0.0, 1.0)),
                ("% feminino entre 0 e 1", f.col("pc_envio_publico_feminino").between(0.0, 1.0)),
                ("% masculino entre 0 e 1", f.col("pc_envio_publico_masculino").between(0.0, 1.0)),
                ("Data histórica ≥ 2000", f.year(f.col("dt_historico")) >= 2000)
            ]
        }
    }

    if tabela_nome not in regras:
        print(f"⚠️ Nenhuma regra definida para a tabela `{tabela_nome}`.")
        return True

    definicoes = regras[tabela_nome]
    erros = []

    # Nulos
    for col in definicoes["colunas_obrigatorias"]:
        if col in df.columns and df.filter(f"{col} IS NULL").count() > 0:
            erros.append(f"Nulos encontrados na coluna `{col}`")

    # Duplicatas
    if definicoes["colunas_chave"]:
        total = df.count()
        distintos = df.dropDuplicates(definicoes["colunas_chave"]).count()
        if total != distintos:
            erros.append(f'Duplicatas com base em {definicoes["colunas_chave"]}: {total - distintos} registros')

    # Regras de negócio
    for descricao, condicao in definicoes["negocio"]:
        if df.filter(~condicao).count() > 0:
            erros.append(f"Falha em regra de negócio: {descricao}")

    if erros:
        print("🔴 Erros de qualidade:")
        for e in erros:
            print(" -", e)
        return False
    else:
        print("✅ Qualidade dos dados validada")
        return True
