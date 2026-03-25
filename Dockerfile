# 1. Imagen base (la que ya usas)
FROM python:3.10-slim

# 2. INSTALACIÓN DE JAVA (Crítico para Spark)
# Instalación de JAVA (Actualizado a la versión 21)
RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 3. Configuración del entorno
WORKDIR /app

# 4. Instalación de librerías (Pandas, Matplotlib y PySpark)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar el código y datos
COPY . .

# 6. Comando de ejecución
# Nota: Si quieres probar Spark, cambia el nombre del script aquí a src/pipeline_spark.py
CMD ["python", "src/pipeline_spark.py"]