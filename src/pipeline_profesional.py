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

    # --- PASO 3: MODELADO ESTRELLA (SILVER) ---
    conn = sqlite3.connect('data/silver/analytics.db')
    
    # A. Tabla de DIMENSIÓN: Productos (Solo valores únicos)
    df_dim_productos = df_clean[['producto']].drop_duplicates().reset_index(drop=True)
    df_dim_productos['id_producto'] = df_dim_productos.index + 1
    df_dim_productos.to_sql('dim_productos', conn, if_exists='replace', index=False)

    # B. Tabla de HECHOS: Ventas (Referenciando el id_producto)
    df_fact_ventas = df_clean.merge(df_dim_productos, on='producto')
    df_fact_ventas = df_fact_ventas[['id', 'id_producto', 'monto_limpio', 'fecha_limpia']]
    df_fact_ventas.to_sql('fact_ventas', conn, if_exists='replace', index=False)
    
    print("✨ Modelo Estrella creado: dim_productos y fact_ventas.")
    
# --- PASO 4: REPORTE GOLD ---
    # --- PASO 4: REPORTE GOLD (USANDO JOINS) ---
    query_gold = """
    SELECT 
        p.producto, 
        SUM(v.monto_limpio) as total 
    FROM fact_ventas v
    JOIN dim_productos p ON v.id_producto = p.id_producto
    GROUP BY p.producto
    """
    df_gold = pd.read_sql(query_gold, conn)
    
    # Guardar resultados CSV
    df_gold.to_csv('data/gold/reporte_final.csv', index=False)
    
    # 📊 Gráfica Segura
    if not df_gold.empty: # <--- Seguridad: Solo graficar si hay datos
        plt.figure(figsize=(8, 5))
        # Aseguramos que no haya nulos antes de graficar
        df_plot = df_gold.dropna(subset=['producto', 'total'])
        plt.bar(df_plot['producto'], df_plot['total'], color='orange')
        plt.title('Ventas Validadas (Capa Gold)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('data/gold/grafico_ventas.png')
        print("✅ Gráfica generada exitosamente.")
    else:
        print("⚠️ No hay datos válidos para graficar.")
    
    conn.close()
    print("🏆 Pipeline finalizado. Solo los datos de calidad llegaron al reporte.")

if __name__ == "__main__":
    run_pro_pipeline()