import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config.db_config import get_connection


def cargar_dim_empleado():
    conn = None
    try:
        print("Iniciando carga de Dim_Empleado...")
        conn = get_connection()

        query = """
        SELECT 
            e.id AS empleado_id_origen,
            e.nombre,
            e.apellido,
            e.cargo,
            e.fecha_ingreso
        FROM public.empleados e
        """

        df_origen = pd.read_sql(query, conn)

        cur = conn.cursor()

        # buena práctica: limpiar antes de cargar
        cur.execute("TRUNCATE TABLE dw.dim_empleado CASCADE")

        insert_query = """
        INSERT INTO dw.dim_empleado (
            empleado_id_origen, nombre, apellido, cargo, fecha_ingreso
        ) VALUES (%s, %s, %s, %s, %s)
        """

        data = [tuple(row) for row in df_origen.values]
        cur.executemany(insert_query, data)

        conn.commit()
        cur.close()

        print(f"Carga finalizada. Empleados insertados: {len(df_origen)}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error: {str(e)}")
        raise

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    cargar_dim_empleado()