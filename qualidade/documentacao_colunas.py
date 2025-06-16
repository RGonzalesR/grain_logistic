# Databricks notebook source
import yaml
from pyspark.sql import SparkSession

def gerar_comments_para_colunas(tabela: str, yaml_path: str):
    """
    Gera os comentários das colunas no Unity Catalog com base em um arquivo YAML.

    Args:
        tabela (str): Nome da tabela no Unity Catalog.
        yaml_path (str): Caminho para o arquivo YAML contendo as definições das colunas.
    """
    try:
        with open(yaml_path, 'r') as file:
            yaml_data = yaml.safe_load(file)

        print(f"{tabela}: YAML lido")

        if 'features' not in yaml_data:
            raise ValueError("O arquivo YAML não contém a chave 'features'.")

        spark = SparkSession.builder.getOrCreate()

        for feature in yaml_data['features']:
            column_name = feature['name']
            comment = feature['description']
            print(f"    Gerando comentário para a coluna {column_name}...", end=" ")

            spark.sql(f"COMMENT ON COLUMN {tabela}.`{column_name}` IS '{comment}'")
            print("Comentário gerado")

        print(f"✅ Comentários gerados com sucesso para a tabela {tabela}.")

    except Exception as e:
        print(f"❌ Erro ao gerar comentários: {e}")

# COMMAND ----------

dict_tabelas = {
    "bronze.grain_logistic_shipping": "/Workspace/Users/renan.gonzales@usp.br/grain_logistic/bronze/bronze.yaml",
    "silver.cliente": "/Workspace/Users/renan.gonzales@usp.br/grain_logistic/silver/cliente.yaml",
    "silver.produto": "/Workspace/Users/renan.gonzales@usp.br/grain_logistic/silver/produto.yaml",
    "silver.envios": "/Workspace/Users/renan.gonzales@usp.br/grain_logistic/silver/envio.yaml",
    "gold.dim_calendario": "/Workspace/Users/renan.gonzales@usp.br/grain_logistic/gold/calendario.yaml",
    "gold.dim_uf": "/Workspace/Users/renan.gonzales@usp.br/grain_logistic/gold/uf.yaml",
    "gold.dim_importancia": "/Workspace/Users/renan.gonzales@usp.br/grain_logistic/gold/importancia.yaml",
    "gold.fato_venda": "/Workspace/Users/renan.gonzales@usp.br/grain_logistic/gold/fato.yaml",
    "gold.fs_historico_envios_corredor": "/Workspace/Users/renan.gonzales@usp.br/grain_logistic/feature_store/feature_store.yaml",
}

for tabela, yaml_path in dict_tabelas.items():
    gerar_comments_para_colunas(tabela, yaml_path)