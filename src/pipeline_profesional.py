import pandas as pd
import sqlite3
import matplotlib.pyplot as plt # <--- Nueva librería para gráficas
import os

def run_pro_pipeline():
    print("🚀 Iniciando Pipeline de Alto Nivel con Reporte Visual...")
    
    # Asegurar que las carpetas existan (Buena práctica de ingeniería)
    for folder in ['data/silver', 'data/gold']:
        os.makedirs(folder, exist_ok=True)
    
    # --- PASO 1: INGESTA (BRONZE) ---
    df_raw = pd.read_csv('data/bronze/ventas_raw.csv')
    
    # --- PASO 2: LIMPIEZA TÉCNICA (PANDAS -> SILVER) ---
    print("🧹 Limpiando datos con DataFrames...")
    df_clean = df_raw.dropna(subset=['producto']).copy()
    df_clean['producto'] = df_clean['producto'].str.strip().str.title()
    df_clean['monto'] = pd.to_numeric(df_clean['monto'], errors='coerce')
    df_clean = df_clean[df_clean['monto'] > 0]
    
    # Guardar en SQL (Capa Silver)
    conn = sqlite3.connect('data/silver/analytics.db')
    df_clean.to_sql('ventas_silver', conn, if_exists='replace', index=False)
    print("✅ Datos limpios cargados en Silver (SQLite)")

    # --- PASO 3: LÓGICA DE NEGOCIO Y GRÁFICA (GOLD) ---
    print("📊 Generando reportes y visualizaciones...")
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
    
    # --- NUEVO: Generar Gráfica de Barras ---
    plt.figure(figsize=(10, 6))
    plt.bar(df_gold['producto'], df_gold['venta_total'], color='skyblue')
    plt.title('Ventas Totales por Producto (Capa Gold)')
    plt.xlabel('Producto')
    plt.ylabel('Monto ($)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Guardar los "Artifacts" (Resultados finales)
    df_gold.to_csv('data/gold/reporte_final.csv', index=False)
    plt.savefig('data/gold/grafico_ventas.png') # <--- Guarda la imagen
    
    conn.close()
    print("🏆 Pipeline finalizado con éxito.")
    print("📂 Archivos generados en data/gold/: reporte_final.csv y grafico_ventas.png")
    print(df_gold)

if __name__ == "__main__":
    run_pro_pipeline()