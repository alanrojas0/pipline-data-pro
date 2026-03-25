from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when
import os

def run_spark_pipeline():
    # 1. Iniciar la SparkSession (Equivalente al Driver en Cloudera/YARN)
    spark = SparkSession.builder \
        .appName("DataEngineer_SanMiguel_Spark") \
        .getOrCreate()
    
    print("🚀 Spark Session iniciada (Motor de Big Data listo)...")

    # Crear carpetas de salida si no existen
    os.makedirs("data/gold", exist_ok=True)
    os.makedirs("data/rejected", exist_ok=True)

    # 2. INGESTA (Capa Bronze)
    # Spark lee el CSV y 'adivina' los tipos de datos (inferSchema)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/bronze/ventas_raw.csv")

# 3. CALIDAD Y LIMPIEZA (Capa Silver)
    # Convertimos monto a double. Si no es un número, Spark pondrá NULL automáticamente.
    df_with_types = df.withColumn("monto_double", col("monto").cast("double"))

    # Ahora filtramos: 
    # 1. Que el producto no sea nulo
    # 2. Que el monto_double NO sea nulo (esto elimina la palabra 'ERROR')
    # 3. Que el monto sea mayor a 0
    df_clean = df_with_types.filter(
        (col("producto").isNotNull()) & 
        (col("monto_double").isNotNull()) & 
        (col("monto_double") > 0)
    )

    # 4. MODELADO ESTRELLA (Capa Gold)
    # Agrupación por producto (Operación de Shuffling en el cluster)
    df_gold = df_clean.groupBy("producto").agg(_sum("monto_double").alias("total_ventas"))

    # 5. SALIDA (Formato Parquet - El estándar de la industria)
    # Guardamos en Parquet porque es mucho más rápido para analytics que el CSV
    df_gold.write.mode("overwrite").parquet("data/gold/reporte_ventas_parquet")
    
    print("🏆 Pipeline Spark completado. Datos guardados en formato Parquet.")
    df_gold.show() # Muestra una vista previa en los logs de GitHub Actions

if __name__ == "__main__":
    run_spark_pipeline()