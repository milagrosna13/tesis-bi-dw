import pandas as pd
from psycopg2.extras import execute_values
import sys
from pathlib import Path

# Agregar la raíz del proyecto al path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.db_config import get_connection


def load_compras():
    conn = get_connection()
    cur = conn.cursor()

    try:
        # 1. Cargar archivos procesados
        path_cabecera = 'data/processed/compras/oc_cabecera_proc.csv'
        path_detalle = 'data/processed/compras/oc_detalle_proc.csv'

        df_cab = pd.read_csv(path_cabecera)
        df_det = pd.read_csv(path_detalle)

        # 2. Mapeos de negocio -> IDs de BD
        # Proveedores: ID operacional -> ID BD
        cur.execute("SELECT id FROM proveedores")
        proveedores_validos = {row[0] for row in cur.fetchall()}

        # Variantes: SKU -> ID
        cur.execute("SELECT sku, id FROM variantes")
        map_variantes = dict(cur.fetchall())

        print(f"Iniciando carga de {len(df_cab)} órdenes de compra...")

        # 3. Iterar cabeceras
        for _, row in df_cab.iterrows():

            proveedor_id = int(row['proveedor_id'])

            if proveedor_id not in proveedores_validos:
                print(f"Proveedor no encontrado: {proveedor_id}")
                continue

            # --- INSERT CABECERA ---
            query_cab = """
                INSERT INTO compras_cabecera (
                     proveedor_id,fecha_pedido, estado_pedido, total_compra
                ) VALUES (%s, %s, %s, %s)
                RETURNING id;
            """

            cur.execute(query_cab, (
                row['fecha_pedido'],
                proveedor_id,
                row['estado_pedido'],
                row['total_compra']
            ))

            compra_id_db = cur.fetchone()[0]

            # --- INSERT DETALLES ---
            items_oc = df_det[df_det['orden_compra_nro'] == row['orden_compra_nro']]

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
                else:
                    print(f"SKU no encontrado: {item['sku_variante']}")

            if valores_detalle:
                query_det = """
                    INSERT INTO compras_detalle (
                        compra_id, variante_id, cantidad, costo_unitario_pactado
                    ) VALUES %s;
                """
                execute_values(cur, query_det, valores_detalle)

        # 4. Commit final
        conn.commit()
        print("Carga de órdenes de compra finalizada correctamente.")

    except Exception as e:
        conn.rollback()
        print(f"Error crítico en LOAD compras: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_compras()
