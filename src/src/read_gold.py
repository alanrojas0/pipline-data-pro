import pandas as pd
import glob
import os

def leer_reporte_final():
    # Buscamos la carpeta donde Spark guardó el Parquet
    path = "data/gold/reporte_ventas_parquet"
    
    if not os.path.exists(path):
        print(f"❌ No se encuentra la carpeta {path}. ¿Ya descargaste y descomprimiste los artifacts?")
        return

    # Parquet es un formato de carpeta. Pandas puede leer la carpeta completa.
    try:
        df = pd.read_parquet(path)
        
        print("📊 --- REPORTE FINAL DE VENTAS (Capa Gold) ---")
        print(df.to_string(index=False))
        print("----------------------------------------------")
        print(f"✅ Total de productos únicos vendidos: {len(df)}")
        
    except Exception as e:
        print(f"Error al leer Parquet: {e}")

if __name__ == "__main__":
    leer_reporte_final()