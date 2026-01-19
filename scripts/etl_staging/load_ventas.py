import pandas as pd
from psycopg2.extras import execute_values
import sys
from pathlib import Path

# Agregar raíz del proyecto al path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.db_config import get_connection

def load_ventas():
    conn = get_connection()
    cur = conn.cursor()

    try:
        # 1. CARGAR ARCHIVOS PROCESADOS
        path_cabecera = 'data/processed/ventas/ventas_cabecera_proc.csv'
        path_detalle = 'data/processed/ventas/ventas_detalle_proc.csv'

        cur.execute("TRUNCATE TABLE ventas_cabecera RESTART IDENTITY CASCADE")
        cur.execute("TRUNCATE TABLE ventas_detalle RESTART IDENTITY CASCADE")

        print("Tabla productos limpiada")
        # Leemos todo como string/objeto donde sea necesario para mantener la integridad
        df_cab = pd.read_csv(path_cabecera, dtype={'dni_cliente': str})
        df_det = pd.read_csv(path_detalle, dtype={'sku_variante': str})

        # 2. MAPEOS DE REFERENCIA (Para validar Foreign Keys)
        cur.execute("SELECT dni, id FROM clientes")
        map_clientes = {str(dni): cid for dni, cid in cur.fetchall()}

        cur.execute("SELECT nombre, id FROM sucursales")
        map_sucursales = {nombre.upper(): sid for nombre, sid in cur.fetchall()}

        cur.execute("SELECT sku, id FROM variantes")
        map_variantes = {sku.upper(): vid for sku, vid in cur.fetchall()}

        print(f"Iniciando carga de {len(df_cab)} tickets...")
        
        tickets_insertados = 0
        tickets_saltados = 0
        detalle_sin_variante = 0
        clientes_faltantes = set()
        sucursales_faltantes = set()

        # 3. PROCESAR CADA TICKET
        for _, row in df_cab.iterrows():
            # Match directo (ya vienen limpios de la transformación)
            cliente_id = map_clientes.get(row['dni_cliente'])
            sucursal_id = map_sucursales.get(str(row['sucursal']).upper())
            
            # Manejo de nulos simple con pandas
            campania_id = row['campania_id'] if pd.notna(row['campania_id']) else None
            canal = row['canal_venta'] # Ya tiene default 'Presencial' desde la transformación

            # Validaciones de integridad
            if not cliente_id:
                clientes_faltantes.add(row['dni_cliente'])
                tickets_saltados += 1
                continue

            if not sucursal_id:
                sucursales_faltantes.add(row['sucursal'])
                tickets_saltados += 1
                continue

            # --- INSERT CABECERA ---
            query_cab = """
                INSERT INTO ventas_cabecera (
                    fecha_hora, cliente_id, sucursal_id, empleado_id,
                    metodo_pago_id, total_venta, campania_id, canal_venta
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """
            cur.execute(query_cab, (
                row['fecha_hora'], cliente_id, sucursal_id, row['empleado_id'],
                row['metodo_pago_id'], row['total_venta'], campania_id, canal
            ))

            venta_id_db = cur.fetchone()[0]
            tickets_insertados += 1 

            # --- PREPARAR DETALLE ---
            items_ticket = df_det[df_det['ticket_nro'] == row['ticket_nro']]
            valores_detalle = []

            for _, item in items_ticket.iterrows():
                variante_id = map_variantes.get(str(item['sku_variante']).upper())
                promo_id = item['promocion_id'] if pd.notna(item['promocion_id']) else None

                if variante_id:
                    valores_detalle.append((
                        venta_id_db, variante_id, item['cantidad'],
                        item['precio_unitario_cobrado'], item['costo_unitario_historico'],
                        item['subtotal'], promo_id
                    ))
                else:
                    detalle_sin_variante += 1

            # --- INSERT DETALLE (Bulk) ---
            if valores_detalle:
                query_det = """
                    INSERT INTO ventas_detalle (
                        venta_id, variante_id, cantidad, precio_unitario_cobrado,
                        costo_unitario_historico, subtotal, promocion_id
                    ) VALUES %s;
                """
                execute_values(cur, query_det, valores_detalle)

        conn.commit()
        print(f"\nCARGA FINALIZADA: {tickets_insertados} tickets en base de datos.")

    except Exception as e:
        conn.rollback()
        print(f"Error crítico en LOAD: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    load_ventas()