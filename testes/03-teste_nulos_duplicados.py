# Databricks notebook source
def validar_nulos_duplicatas(nome_tabela: str, colunas_nao_nulas: list, colunas_chave: list):
    try:
        df = spark.table(nome_tabela)
        erros = []

        # Verificação de nulos
        for col in colunas_nao_nulas:
            if col in df.columns:
                nulos = df.filter(f"{col} IS NULL").count()
                if nulos > 0:
                    erros.append(f"Coluna {col} possui {nulos} nulos")

        # Verificação de duplicatas
        if colunas_chave:
            total = df.count()
            distintos = df.dropDuplicates(colunas_chave).count()
            if total != distintos:
                erros.append(f"Duplicatas detectadas com base em {colunas_chave}: {total - distintos} registros")

        if erros:
            print(f"🔴 Problemas encontrados na tabela {nome_tabela}:")
            for erro in erros:
                print(" -", erro)
            return False

        print(f"✅ Tabela {nome_tabela} sem nulos críticos ou duplicatas")
        return True

    except Exception as e:
        print(f"Erro ao validar nulos/duplicatas em {nome_tabela}: {str(e)}")
        return False

tabelas_para_testar = {
    "silver.envios": {
        "colunas_nao_nulas": ["id_envio", "dt_envio", "dt_entrega"],
        "colunas_chave": ["id_envio"]
    },
    "silver.produto": {
        "colunas_nao_nulas": ["id_envio", "vl_preco"],
        "colunas_chave": ["id_envio"]
    },
    "silver.cliente": {
        "colunas_nao_nulas": ["id_envio"],
        "colunas_chave": ["id_envio"]
    },
    "gold.fato_venda": {
        "colunas_nao_nulas": ["id_envio", "id_uf", "vl_receita_total"],
        "colunas_chave": ["id_envio"]
    },
    "gold.dim_uf": {
        "colunas_nao_nulas": ["id_uf", "ds_uf"],
        "colunas_chave": ["id_uf"]
    },
    "gold.dim_importancia": {
        "colunas_nao_nulas": ["id_importancia", "ds_importancia"],
        "colunas_chave": ["id_importancia"]
    },
    "gold.dim_calendario": {
        "colunas_nao_nulas": ["id_calendario", "dt_completa"],
        "colunas_chave": ["id_calendario"]
    }
}

for nome_tabela, regras in tabelas_para_testar.items():
    validar_nulos_duplicatas(nome_tabela, regras["colunas_nao_nulas"], regras["colunas_chave"])