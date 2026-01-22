import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config.db_config import get_connection

def cargar_dim_cliente():
    conn = None
    
    try:
        print("Iniciando carga de Dim_Cliente...")
        conn = get_connection()
        
        # El SELECT desnormaliza las tablas de ubicación (localidades y provincias)
        # para cumplir con el modelo de Kimball de tablas "anchas"
        query = """
        SELECT 
            c.id AS cliente_id_origen,
            c.nombre,
            c.apellido,
            c.genero,
            c.fecha_nacimiento,
            loc.nombre AS localidad,
            prov.nombre AS provincia,
            c.fecha_alta
        FROM public.clientes c
        LEFT JOIN public.localidades loc ON c.localidad_id = loc.id
        LEFT JOIN public.provincias prov ON loc.provincia_id = prov.id
        """
        
        df_origen = pd.read_sql(query, conn)

        # En el DW de una PyME de ropa, a veces el género o la localidad pueden ser nulos.
        # Es buena práctica limpiar los nulos para que en el dashboard no aparezca "NaN".
        df_origen['genero'] = df_origen['genero'].fillna('No Definido')
        df_origen['localidad'] = df_origen['localidad'].fillna('Desconocida')
        df_origen['provincia'] = df_origen['provincia'].fillna('Desconocida')

        # Insertar con cursor
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO dw.dim_cliente (
            cliente_id_origen, nombre, apellido, genero, 
            fecha_nac, localidad, provincia, fecha_alta
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        data = [tuple(row) for row in df_origen.values]
        cur.executemany(insert_query, data)
        
        conn.commit()
        cur.close()
        
        print(f"Carga finalizada. Clientes insertados: {len(df_origen)}")
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error: {str(e)}")
        raise
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    cargar_dim_cliente()