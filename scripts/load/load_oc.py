import pandas as pd
from psycopg2.extras import execute_values
import sys
from pathlib import Path

# Agregar la raíz del proyecto al path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.db_config import get_connection

def load_compras():
    conn = get_connection()
    cur = conn.cursor()

    try:
        path_cabecera = 'data/processed/compras/oc_cabecera_proc.csv'
        path_detalle = 'data/processed/compras/oc_detalle_proc.csv'

        df_cab = pd.read_csv(path_cabecera)
        df_det = pd.read_csv(path_detalle)

        # Mapeos
        cur.execute("SELECT id FROM proveedores")
        proveedores_validos = {row[0] for row in cur.fetchall()}
        cur.execute("SELECT sku, id FROM variantes")
        map_variantes = dict(cur.fetchall())

        # IMPORTANTE: Obtener órdenes que YA están en la base de datos 
        # para no intentar re-insertar sus detalles
        cur.execute("SELECT id FROM ordenes_compra_cabecera")
        ordenes_existentes = {row[0] for row in cur.fetchall()}

        print(f"Procesando {len(df_cab)} órdenes...")

        for _, row in df_cab.iterrows():
            
            oc_nro = int(row['orden_compra_nro'])
            
        
            # Si el ID ya existe, pasamos a la siguiente
            cur.execute("SELECT id FROM ordenes_compra_cabecera WHERE id = %s", (oc_nro,))
            if cur.fetchone():
                continue # Salta a la siguiente orden

            # --- INSERT CABECERA ---
               
            query_cab = """
                INSERT INTO ordenes_compra_cabecera (
                    id, proveedor_id, fecha_pedido, estado_pedido, total_compra
                ) 
                OVERRIDING SYSTEM VALUE -- <--- ESTO ES LA CLAVE
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                RETURNING id;
            """

            cur.execute(query_cab, (
                    oc_nro, 
                    int(row['proveedor_id']),           
                    row['fecha_pedido'],
                    row['estado_pedido'],
                    row['total_compra']
                ))
            
            res = cur.fetchone()
            if not res: continue # Si el ON CONFLICT actuó, res será None
            
            compra_id_db = res[0]

            # --- INSERT DETALLES ---
            items_oc = df_det[df_det['orden_compra_nro'] == oc_nro]
            valores_detalle = []

            for _, item in items_oc.iterrows():
                variante_id = map_variantes.get(item['sku_variante'])
                if variante_id:
                    valores_detalle.append((
                        compra_id_db,
                        variante_id,
                        item['cantidad'],
                        item['costo_unitario_pactado']
                    ))

            if valores_detalle:
                query_det = """
                    INSERT INTO ordenes_compra_detalle (
                        orden_compra_id, variante_id, cantidad, costo_unitario_pactado
                    ) VALUES %s
                    ON CONFLICT DO NOTHING;
                """
                execute_values(cur, query_det, valores_detalle)

        conn.commit()
        print("Carga finalizada de ordenes de compras.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()
