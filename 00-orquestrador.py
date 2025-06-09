# Databricks notebook source
# MAGIC %run ./bronze/01_ingestao_bronze

# COMMAND ----------

# MAGIC %run ./silver/02_transformacoes_silver

# COMMAND ----------

# MAGIC %run ./gold/03_dim_uf

# COMMAND ----------

# MAGIC %run ./gold/04_dim_calendario

# COMMAND ----------

# MAGIC %run ./gold/05_dim_importancia

# COMMAND ----------

# MAGIC %run ./gold/06_fato_venda

# COMMAND ----------

# MAGIC %run ./feature_store/07_fs_historico_envios_corredor

# COMMAND ----------

