import pandas as pd
from psycopg2.extras import execute_values
import sys
from pathlib import Path

# Agregar la raíz del proyecto al path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.db_config import get_connection

def load_ventas():
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # 1. Cargar archivos procesados por el ETL
        path_cabecera = 'data/processed/ventas/ventas_cabecera_proc.csv'
        path_detalle = 'data/processed/ventas/ventas_detalle_proc.csv'
        
        df_cab = pd.read_csv(path_cabecera)
        df_det = pd.read_csv(path_detalle)
        
        # 2. Obtener mapeos (Traducción de Negocio -> ID de BD)
        # Clientes: DNI -> ID
        cur.execute("SELECT dni, id FROM clientes")
        map_clientes = dict(cur.fetchall())
        
        # Sucursales: Nombre -> ID
        cur.execute("SELECT nombre, id FROM sucursales")
        map_sucursales = dict(cur.fetchall())
        
        # Variantes: SKU -> ID
        cur.execute("SELECT sku, id FROM variantes")
        map_variantes = dict(cur.fetchall())

        print(f"Iniciando carga de {len(df_cab)} tickets y sus detalles...")

        # 3. Iterar por cada ticket de la cabecera
        for _, row in df_cab.iterrows():
            
            # Obtener IDs de las FKs
            # .get() y str() para evitar errores si el DNI viene como int
            cliente_id = map_clientes.get(str(row['dni_cliente']))
            sucursal_id = map_sucursales.get(row['sucursal'])
            
            if not cliente_id or not sucursal_id:
                print(f"Error: No se encontró ID para cliente {row['dni_cliente']} o sucursal {row['sucursal']}")
                continue

            # --- INSERTAR CABECERA ---
            query_cab = """
                INSERT INTO ventas_cabecera (
                    fecha_hora, cliente_id, sucursal_id, empleado_id, metodo_pago_id, total_venta
                ) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
            """
            
            cur.execute(query_cab, (
                row['fecha_hora'],
                cliente_id,
                sucursal_id,
                row['empleado_id'],
                row['metodo_pago_id'],
                row['total_venta']
            ))
            
            # Recuperamos el ID autogenerado para la venta
            venta_id_db = cur.fetchone()[0]

            # --- INSERTAR DETALLES ASOCIADOS ---
            # Filtramos en el dataframe de detalles por el ticket_nro actual
            items_ticket = df_det[df_det['ticket_nro'] == row['ticket_nro']]
            
            valores_detalle = []
            for _, item in items_ticket.iterrows():
                variante_id = map_variantes.get(item['sku_variante'])
                
                if variante_id:
                    valores_detalle.append((
                        venta_id_db,
                        variante_id,
                        item['cantidad'],
                        item['precio_unitario_cobrado'],
                        item['costo_unitario_historico'],
                        item['subtotal']
                    ))
            
            if valores_detalle:
                query_det = """
                    INSERT INTO ventas_detalle (
                        venta_id, variante_id, cantidad, precio_unitario_cobrado, costo_unitario_historico, subtotal
                    ) VALUES %s;
                """
                execute_values(cur, query_det, valores_detalle)

        # 4. Confirmar carga
        conn.commit()
        print("Carga finalizada exitosamente en la base de datos.")

    except Exception as e:
        conn.rollback()
        print(f"Error crítico en el proceso de carga: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    load_ventas()