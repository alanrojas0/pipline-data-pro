# 🚀 Data Engineering Pipeline: Spark & Docker (Medallion Architecture)

Este proyecto implementa un pipeline de datos robusto utilizando **Apache Spark** sobre un entorno contenedorizado con **Docker**, automatizado mediante **GitHub Actions**. El objetivo es procesar ventas crudas, validar la calidad de los datos y generar reportes optimizados en formato **Parquet**.

## 🏗️ Arquitectura del Proyecto
El pipeline sigue el patrón de **Arquitectura de Medallion**:

1.  **Capa Bronze (Raw):** Ingesta de archivos CSV crudos (`data/bronze`).
2.  **Capa Silver (Cleansing):** Validación de esquema, manejo de nulos y filtrado de datos malformados mediante `try_cast`.
3.  **Capa Gold (Analytics):** Agregación de datos y exportación a **Parquet** para consumo de alta eficiencia.
4.  **Data Governance:** Captura automática de registros rechazados en una ruta independiente (`data/rejected`) para auditoría.

## 🛠️ Tecnologías Utilizadas
* **Lenguaje:** Python 3.10
* **Motor de Big Data:** Apache Spark (PySpark)
* **Entorno:** Docker (Base Debian Slim + OpenJDK 21)
* **CI/CD:** GitHub Actions (Automatización de Build y Test)
* **Formatos:** CSV (Origen) y Parquet (Destino)

## 🚦 Manejo de Errores y Calidad (Data Quality)
El pipeline está diseñado para ser **tolerante a fallos**. Utiliza una lógica de "blindaje" para evitar que datos corruptos detengan el procesamiento:
- **`try_cast`:** Convierte tipos de datos de forma segura. Si un campo numérico contiene texto (ej: "ERROR"), Spark lo marca como nulo en lugar de fallar.
- **Filtros de Integridad:** Se descartan registros con productos nulos o montos negativos antes de la capa Gold.
- **Trazabilidad:** Todos los registros que fallan las reglas de negocio se exportan a la carpeta `rejected/` con su formato original.

## 🚀 Cómo ejecutar el proyecto

### 1. Ejecución con Docker (Recomendado)
Para asegurar que el entorno sea idéntico al de producción:
```bash
docker build -t pipeline-spark-pro .
docker run -v ${PWD}/data:/app/data pipeline-spark-pro