from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, expr, to_date 
import os

def run_spark_pipeline():
    spark = SparkSession.builder \
        .appName("DataEngineer_SanMiguel_Partitioning") \
        .getOrCreate()
    
    print("🚀 Iniciando Pipeline con Particionamiento por Fecha...")

    # 1. INGESTA (Capa Bronze)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/bronze/ventas_raw.csv")

    # 2. TRANSFORMACIÓN Y LIMPIEZA (Capa Silver)
    # Convertimos 'monto' de forma segura y 'fecha' a tipo Date real
    df_transformed = df.withColumn("monto_double", expr("try_cast(monto as double)")) \
                       .withColumn("fecha_dt", to_date(col("fecha"), "yyyy-MM-dd"))

    # Filtramos registros válidos
    df_clean = df_transformed.filter(
        (col("producto").isNotNull()) & 
        (col("monto_double").isNotNull()) & 
        (col("monto_double") > 0) &
        (col("fecha_dt").isNotNull())
    )

    # 3. MODELADO (Capa Gold)
    # Agrupamos por producto Y fecha para mantener el detalle diario
    df_gold = df_clean.groupBy("producto", "fecha_dt").agg(_sum("monto_double").alias("total_ventas"))

    # 4. SALIDA PARTICIONADA
    # Aquí ocurre la magia: partitionBy crea carpetas físicas por fecha
    df_gold.write.mode("overwrite") \
        .partitionBy("fecha_dt") \
        .parquet("data/gold/reporte_ventas_particionado")
    
    print("✅ Pipeline finalizado. Datos particionados por fecha.")
    df_gold.show()

if __name__ == "__main__":
    run_spark_pipeline()