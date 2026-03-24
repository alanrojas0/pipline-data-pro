import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import os

def run_pro_pipeline():
    print("🚀 Iniciando Pipeline con Validación de Calidad...")
    
    # Crear carpetas necesarias incluyendo la de rechazos
    for folder in ['data/silver', 'data/gold', 'data/rejected']:
        os.makedirs(folder, exist_ok=True)
    
    # --- PASO 1: INGESTA ---
    df_raw = pd.read_csv('data/bronze/ventas_raw.csv')
    
    # --- PASO 2: VALIDACIÓN Y LIMPIEZA (PANDAS) ---
    print("🔍 Validando calidad de datos...")
    
    # Intentar convertir monto a número (lo que no sea número será NaN)
    df_raw['monto_limpio'] = pd.to_numeric(df_raw['monto'], errors='coerce')
    
    # Intentar convertir fecha (lo que sea '99-99' será NaT)
    df_raw['fecha_limpia'] = pd.to_datetime(df_raw['fecha'], errors='coerce')
    
    # ❌ SEPARAR RECHAZADOS: Filas con monto inválido, sin producto o fecha errónea
    mask_error = (
        df_raw['monto_limpio'].isna() | 
        df_raw['producto'].isna() | 
        df_raw['monto_limpio'] <= 0 |
        df_raw['fecha_limpia'].isna()
    )
    
    df_rejected = df_raw[mask_error]
    df_clean = df_raw[~mask_error].copy()
    
    # Guardar rechazados para auditoría
    if not df_rejected.empty:
        df_rejected.to_csv('data/rejected/ventas_fallidas.csv', index=False)
        print(f"⚠️ Se encontraron {len(df_rejected)} filas con errores. Guardadas en data/rejected/")

    # --- PASO 3: CARGA A SILVER (SQL) ---
    df_clean['producto'] = df_clean['producto'].str.strip().str.title()
    conn = sqlite3.connect('data/silver/analytics.db')
    df_clean[['id', 'producto', 'monto_limpio', 'fecha_limpia']].to_sql(
        'ventas_silver', conn, if_exists='replace', index=False
    )
    
    # --- PASO 4: REPORTE GOLD ---
    query_gold = "SELECT producto, SUM(monto_limpio) as total FROM ventas_silver GROUP BY producto"
    df_gold = pd.read_sql(query_gold, conn)
    
    # Guardar resultados
    df_gold.to_csv('data/gold/reporte_final.csv', index=False)
    
    # Gráfica
    plt.figure(figsize=(8, 5))
    plt.bar(df_gold['producto'], df_gold['total'], color='orange')
    plt.title('Ventas Validadas (Capa Gold)')
    plt.savefig('data/gold/grafico_ventas.png')
    
    conn.close()
    print("🏆 Pipeline finalizado. Solo los datos de calidad llegaron al reporte.")

if __name__ == "__main__":
    run_pro_pipeline()