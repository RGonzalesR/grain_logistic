# Databricks notebook source
from pyspark.sql.types import BooleanType, DateType, DecimalType, IntegerType, StringType

def define_tipo_schema(df):

    for col in df.columns:
        if col.startswith("in_"):
            df = df.withColumn(col, f.col(col).cast(BooleanType()))
        elif col.startswith("dt_"):
            df = df.withColumn(col, f.col(col).cast(DateType()))
        elif col.startswith("vl_"):
            df = df.withColumn(col, f.col(col).cast(DecimalType(10, 2)))
        elif col.startswith("pc_"):
            df = df.withColumn(col, f.col(col).cast(DecimalType(6, 4)))
        elif col.startswith(("qt_", "nr")):
            df = df.withColumn(col, f.col(col).cast(IntegerType()))
        elif col.startswith(("ds_", "id_", "sg_")):
            df = df.withColumn(col, f.col(col).cast(StringType()))
        elif col != "data_ingestao":
            print(f"coluna {col} não definida.")

    return df
