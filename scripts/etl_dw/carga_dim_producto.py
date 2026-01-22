import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config.db_config import get_connection

def cargar_dim_producto():
    conn = None
    
    try:
        print("Iniciando carga de Dim_Producto...")
        conn = get_connection()
        
        # 1. Agregamos v.activo al SELECT para poder compararlo
        query = """
        SELECT 
            v.id AS variante_id_origen,
            p.nombre,
            c.nombre AS categoria,
            t.descripcion AS talle,
            col.descripcion AS color,
            v.sku,
            p.precio_lista,
            p.fecha_alta,
            p.activo AS producto_activo,
            v.activo AS variante_activo
        FROM public.productos p
        JOIN public.categorias c ON p.categoria_id = c.id
        JOIN public.variantes v ON v.producto_id = p.id
        JOIN public.talles t ON v.talle_id = t.id
        JOIN public.colores col ON v.color_id = col.id
        """
        
        df_origen = pd.read_sql(query, conn)
        
        # 2. Lógica de negocio: El producto en el DW está activo 
        # SOLO SI el producto padre Y la variante están activos.
        df_origen['activo_final'] = df_origen['producto_activo'] & df_origen['variante_activo']
        
        # 3. Seleccionamos solo las columnas necesarias para el DW, en el orden correcto
        # Usamos el id de la variante como el origen real de la fila
        df_final = df_origen[[
            'variante_id_origen', 'nombre', 'categoria', 'talle', 'color', 
            'sku', 'precio_lista', 'fecha_alta', 'activo_final'
        ]]
        
        # Insertar con cursor
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO dw.dim_producto (
            producto_id_origen, nombre, categoria, talle, color, 
            sku, precio_lista, fecha_alta, activo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Convertimos a lista de tuplas para executemany
        data = [tuple(row) for row in df_final.values]
        
        # Limpiar tabla antes de cargar
        cur.execute("TRUNCATE TABLE dw.dim_producto CASCADE") 
        
        cur.executemany(insert_query, data)
        
        conn.commit()
        cur.close()
        
        print(f"Carga finalizada. Filas insertadas: {len(df_final)}")
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error: {str(e)}")
        raise
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    cargar_dim_producto()