from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, expr 
import os

def run_spark_pipeline():
    spark = SparkSession.builder \
        .appName("DataEngineer_SanMiguel_Governance") \
        .getOrCreate()
    
    print("🚀 Iniciando Pipeline con Gobernanza de Datos...")

    # 1. INGESTA (Capa Bronze)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/bronze/ventas_raw.csv")

    # 2. VALIDACIÓN (Capa Silver)
    # Aplicamos try_cast para identificar qué es basura
    df_transformed = df.withColumn("monto_double", expr("try_cast(monto as double)"))

    # SEPARACIÓN DE CAMINOS:
    # Registros válidos
    df_clean = df_transformed.filter(
        (col("producto").isNotNull()) & 
        (col("monto_double").isNotNull()) & 
        (col("monto_double") > 0)
    )

    # Registros RECHAZADOS (Lo que el casting no pudo procesar)
    df_rejected = df_transformed.filter(
        (col("producto").isNull()) | 
        (col("monto_double").isNull()) | 
        (col("monto_double") <= 0)
    )

    # 3. GUARDAR RESULTADOS (Capa Gold y Rejected)
    # Guardamos los buenos en Parquet para analítica masiva
    df_gold = df_clean.groupBy("producto").agg(_sum("monto_double").alias("total_ventas"))
    df_gold.write.mode("overwrite").parquet("data/gold/reporte_ventas_parquet")

    # Guardamos los malos en CSV para que alguien los revise manualmente
    df_rejected.write.mode("overwrite").option("header", "true").csv("data/rejected/ventas_fallidas")
    
    print(f"✅ Proceso terminado.")
    print(f"📊 Registros procesados correctamente: {df_clean.count()}")
    print(f"⚠️ Registros rechazados: {df_rejected.count()}")
    df_gold.show()

if __name__ == "__main__":
    run_spark_pipeline()