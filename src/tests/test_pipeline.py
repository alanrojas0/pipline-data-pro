import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

@pytest.fixture(scope="session")
def spark():
    """Crea una SparkSession local para las pruebas"""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("Pytest-Spark-Local") \
        .getOrCreate()

def test_data_cleaning_logic(spark):
    """Prueba que el try_cast y try_to_date funcionen como esperamos"""
    
    # 1. Creamos datos de prueba con los errores que ya conocemos
    data = [
        ("1", "Mouse", "25.5", "2026-03-25"),   # Válido
        ("2", "Teclado", "ERROR", "2026-03-25"), # Monto malformado
        ("3", "Monitor", "100.0", "2026-99-99")  # Fecha malformada
    ]
    columns = ["id", "producto", "monto", "fecha"]
    df = spark.createDataFrame(data, columns)

    # 2. Aplicamos tu lógica de limpieza (Capa Silver)
    df_transformed = df.withColumn("monto_double", expr("try_cast(monto as double)")) \
                       .withColumn("fecha_dt", expr("try_to_date(fecha, 'yyyy-MM-dd')"))

    # 3. Verificamos los resultados (Assertions)
    results = df_transformed.collect()

    # El ID 1 debe ser válido
    assert results[0]["monto_double"] == 25.5
    assert results[0]["fecha_dt"] is not None

    # El ID 2 debe tener monto NULL (por el "ERROR")
    assert results[1]["monto_double"] is None

    # El ID 3 debe tener fecha NULL (por el "99-99")
    assert results[2]["fecha_dt"] is None