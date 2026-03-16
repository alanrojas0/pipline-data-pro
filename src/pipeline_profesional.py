import pandas as pd
import os
from datetime import datetime

# 1. CONFIGURACIÓN DE RUTAS
BRONZE_PATH = "data/bronze/ventas_raw.csv"
SILVER_PATH = "data/silver/ventas_limpias.csv"
GOLD_PATH = "data/gold/reporte_ventas.csv"

def run_pipeline():
    print("🚀 Iniciando Pipeline de Datos...")

    # --- PASO 1: CAPA BRONZE (Generación de datos sucios) ---
    # Simulamos datos que vienen con errores de una tienda
    data_raw = {
        'id_venta': [1, 2, 3, 4, 5],
        'fecha': ['2024-01-01', '2024-01-02', 'ERROR', '2024-01-04', '2024-01-05'],
        'monto': [100.50, -50.00, 200.00, 150.75, 0.00],
        'producto': ['Laptop', 'Mouse', 'Teclado', 'Laptop', 'Monitor']
    }
    df_raw = pd.DataFrame(data_raw)
    os.makedirs("data/bronze", exist_ok=True)
    df_raw.to_csv(BRONZE_PATH, index=False)
    print("✅ Capa Bronze: Datos crudos guardados.")

    # --- PASO 2: CAPA SILVER (Limpieza y Validación) ---
    df_silver = pd.read_csv(BRONZE_PATH)
    
    # Regla 1: Solo montos positivos (Data Quality)
    df_silver = df_silver[df_silver['monto'] > 0]
    
    # Regla 2: Corregir fechas (Si no es fecha, poner la de hoy)
    df_silver['fecha'] = pd.to_datetime(df_silver['fecha'], errors='coerce').fillna(datetime.now())
    
    os.makedirs("data/silver", exist_ok=True)
    df_silver.to_csv(SILVER_PATH, index=False)
    print("✅ Capa Silver: Datos limpiados y validados.")

    # --- PASO 3: CAPA GOLD (Agregación para Negocio) ---
    # Queremos saber cuánto se vendió por cada tipo de producto
    df_gold = df_silver.groupby('producto')['monto'].sum().reset_index()
    
    os.makedirs("data/gold", exist_ok=True)
    df_gold.to_csv(GOLD_PATH, index=False)
    print("✅ Capa Gold: Reporte final generado con éxito.")
    
    print("\n--- RESUMEN PARA EL JEFE ---")
    print(df_gold)

if __name__ == "__main__":
    run_pipeline()