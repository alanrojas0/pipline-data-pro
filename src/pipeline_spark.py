from pyspark.sql import SparkSession
# Añadimos 'expr' para poder usar sintaxis SQL de try_cast
from pyspark.sql.functions import col, sum as _sum, expr 
import os

def run_spark_pipeline():
    # 1. Iniciar la SparkSession (El 'Driver' que coordina el cluster)
    spark = SparkSession.builder \
        .appName("DataEngineer_SanMiguel_Spark") \
        .getOrCreate()
    
    print("🚀 Spark Session iniciada (Motor de Big Data listo)...")

    # Asegurar carpetas de salida
    os.makedirs("data/gold", exist_ok=True)
    os.makedirs("data/rejected", exist_ok=True)

    # 2. INGESTA (Capa Bronze)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/bronze/ventas_raw.csv")

    # 3. CALIDAD Y LIMPIEZA (Capa Silver)
    # try_cast es la clave: si encuentra 'ERROR', pone NULL en vez de fallar.
    df_with_types = df.withColumn("monto_double", expr("try_cast(monto as double)"))

    # Filtramos los registros que no sirven para el reporte final
    df_clean = df_with_types.filter(
        (col("producto").isNotNull()) & 
        (col("monto_double").isNotNull()) & 
        (col("monto_double") > 0)
    )

    # 4. MODELADO ESTRELLA (Capa Gold)
    # Agrupamos por producto y sumamos el monto ya convertido
    df_gold = df_clean.groupBy("producto").agg(_sum("monto_double").alias("total_ventas"))

    # 5. SALIDA (Formato Parquet)
    # Guardamos el resultado final de forma eficiente
    df_gold.write.mode("overwrite").parquet("data/gold/reporte_ventas_parquet")
    
    print("🏆 Pipeline Spark completado con éxito.")
    df_gold.show()

if __name__ == "__main__":
    run_spark_pipeline()