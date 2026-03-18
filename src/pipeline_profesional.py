import pandas as pd
import sqlite3
import os

def run_pro_pipeline():
    print("🚀 Iniciando Pipeline de Alto Nivel...")
    
    # --- PASO 1: INGESTA (BRONZE) ---
    df_raw = pd.read_csv('data/bronze/ventas_raw.csv')
    
    # --- PASO 2: LIMPIEZA TÉCNICA (PANDAS -> SILVER) ---
    print("🧹 Limpiando datos con DataFrames...")
    
    # A. Eliminar filas donde el producto es nulo
    df_clean = df_raw.dropna(subset=['producto']).copy()
    
    # B. Limpieza de strings (Quitar espacios y poner Capitalize)
    df_clean['producto'] = df_clean['producto'].str.strip().str.title()
    
    # C. Asegurar que los montos sean números y filtrar negativos
    df_clean['monto'] = pd.to_numeric(df_clean['monto'], errors='coerce')
    df_clean = df_clean[df_clean['monto'] > 0]
    
    # D. Guardar en SQL (Capa Silver)
    conn = sqlite3.connect('data/silver/analytics.db')
    df_clean.to_sql('ventas_silver', conn, if_exists='replace', index=False)
    print("✅ Datos limpios cargados en Silver (SQLite)")

    # --- PASO 3: LÓGICA DE NEGOCIO (SQL -> GOLD) ---
    print("📊 Generando reportes con SQL...")
    query_gold = """
    SELECT 
        producto,
        SUM(monto) as venta_total,
        COUNT(*) as cantidad_operaciones
    FROM ventas_silver
    GROUP BY producto
    ORDER BY venta_total DESC
    """
    
    df_gold = pd.read_sql(query_gold, conn)
    
    # Guardar resultado final
    df_gold.to_csv('data/gold/reporte_final.csv', index=False)
    conn.close()
    
    print("🏆 Pipeline finalizado. Reporte Gold generado:")
    print(df_gold)

if __name__ == "__main__":
    run_pro_pipeline()