import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config.db_config import get_connection

def cargar_dim_promocion():
    conn = None
    try:
        print("Iniciando carga de Dim_Promocion...")
        conn = get_connection()

        query = """
        SELECT
            p.id AS promocion_id_origen,
            p.nombre AS nombre_promocion,
            p.tipo_descuento,
            p.valor_descuento,
            p.fecha_inicio,
            p.fecha_fin,
            c.nombre AS campania
        FROM public.promociones p
        LEFT JOIN public.campanias_promociones cp
            ON p.id = cp.promocion_id
        LEFT JOIN public.campanias_marketing c
            ON cp.campania_id = c.id
        """

        df_origen = pd.read_sql(query, conn)

        # limpieza de nulos (buena práctica BI)
        df_origen['campania'] = df_origen['campania'].fillna('Sin Campaña')

        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE dw.dim_promocion CASCADE")

        insert_query = """
        INSERT INTO dw.dim_promocion (
            promocion_id_origen,
            nombre_promocion,
            tipo_descuento,
            valor_descuento,
            fecha_inicio,
            fecha_fin,
            campania
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        data = [tuple(row) for row in df_origen.values]
        cur.executemany(insert_query, data)

        conn.commit()
        cur.close()

        print(f"Carga finalizada. Promociones insertadas: {len(df_origen)}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
if __name__ == "__main__":
    cargar_dim_promocion()