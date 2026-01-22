import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config.db_config import get_connection

def cargar_dim_sucursal():
    conn = None
    
    try:
        print("Iniciando carga de Dim_Sucursal...")
        conn = get_connection()
        
        # Unificamos los datos de ubicación en una sola consulta
        query = """
        SELECT 
            s.id AS sucursal_id_origen,
            s.nombre,
            s.direccion,
            loc.nombre AS localidad,
            prov.nombre AS provincia
        FROM public.sucursales s
        LEFT JOIN public.localidades loc ON s.localidad_id = loc.id
        LEFT JOIN public.provincias prov ON loc.provincia_id = prov.id
        """
        
        df_origen = pd.read_sql(query, conn)

        # Limpieza básica: asegurar que no haya nulos en campos geográficos
        df_origen['localidad'] = df_origen['localidad'].fillna('No Definida')
        df_origen['provincia'] = df_origen['provincia'].fillna('No Definida')

        # Insertar en el DW
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO dw.dim_sucursal (
            sucursal_id_origen, nombre, direccion, localidad, provincia
        ) VALUES (%s, %s, %s, %s, %s)
        """
        
        data = [tuple(row) for row in df_origen.values]
        cur.executemany(insert_query, data)
        
        conn.commit()
        cur.close()
        
        print(f"Carga finalizada. Sucursales insertadas: {len(df_origen)}")
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error: {str(e)}")
        raise
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    cargar_dim_sucursal()