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
        # 1. Cargar archivos procesados - Forzamos DNI y SKU a string
        path_cabecera = 'data/processed/ventas/ventas_cabecera_proc.csv'
        path_detalle = 'data/processed/ventas/ventas_detalle_proc.csv'

        # Leemos forzando tipos para evitar el .0 en los números
        df_cab = pd.read_csv(path_cabecera, dtype={'dni_cliente': str})
        df_det = pd.read_csv(path_detalle, dtype={'sku_variante': str})

        # 2. Mapeos de claves con NORMALIZACIÓN (Strip y Upper)
        cur.execute("SELECT dni, id FROM clientes")
        # Normalizamos: quitamos espacios y aseguramos que el DNI sea string
        map_clientes = {str(dni).strip(): cid for dni, cid in cur.fetchall()}

        cur.execute("SELECT nombre, id FROM sucursales")
        # Normalizamos nombres de sucursales
        map_sucursales = {str(nombre).strip().upper(): sid for nombre, sid in cur.fetchall()}

        cur.execute("SELECT sku, id FROM variantes")
        # Normalizamos SKUs a Mayúsculas y sin espacios
        map_variantes = {str(sku).strip().upper(): vid for sku, vid in cur.fetchall()}

        print(f"Iniciando carga de {len(df_cab)} tickets...")
        
        tickets_insertados = 0
        tickets_saltados = 0
        detalle_sin_variante = 0
        clientes_faltantes = set()
        sucursales_faltantes = set()

        # 3. Insertar cabeceras y detalles
        for _, row in df_cab.iterrows():
            # Limpiamos el dato que viene del CSV para el match
            dni_csv = str(row['dni_cliente']).strip()
            sucursal_csv = str(row['sucursal']).strip().upper()
            
            cliente_id = map_clientes.get(dni_csv)
            sucursal_id = map_sucursales.get(sucursal_csv)
            
            canal = row.get('canal_venta', 'Presencial')
            campania_id = row.get('campania_id')

            if pd.isna(campania_id):
                campania_id = None

            # Validaciones de existencia
            if not cliente_id:
                clientes_faltantes.add(dni_csv)
                tickets_saltados += 1
                continue

            if not sucursal_id:
                sucursales_faltantes.add(sucursal_csv)
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
                row['fecha_hora'],
                cliente_id,
                sucursal_id,
                row['empleado_id'],
                row['metodo_pago_id'],
                row['total_venta'],
                campania_id,
                canal
            ))

            venta_id_db = cur.fetchone()[0]
            tickets_insertados += 1 

            # --- DETALLE ---
            items_ticket = df_det[df_det['ticket_nro'] == row['ticket_nro']]
            valores_detalle = []

            for _, item in items_ticket.iterrows():
                # Normalizamos el SKU del detalle igual que el mapa
                sku_detalle = str(item['sku_variante']).strip().upper().replace('.0', '')

                variante_id = map_variantes.get(sku_detalle)
                promocion_id = item.get('promocion_id')

                # Limpieza de promocion_id (evitar el float NaN)
                if pd.isna(promocion_id) or promocion_id == 0:
                    promocion_id = None
                else:
                    promocion_id = int(promocion_id)

                if variante_id:
                    valores_detalle.append((
                        venta_id_db,
                        variante_id,
                        item['cantidad'],
                        item['precio_unitario_cobrado'],
                        item['costo_unitario_historico'],
                        item['subtotal'],
                        promocion_id
                    ))
                else:
                    detalle_sin_variante += 1

            if valores_detalle:
                query_det = """
                    INSERT INTO ventas_detalle (
                        venta_id, variante_id, cantidad, precio_unitario_cobrado,
                        costo_unitario_historico, subtotal, promocion_id
                    ) VALUES %s;
                """
                execute_values(cur, query_det, valores_detalle)

        conn.commit()
        
        # Reporte final
        print(f"\n{'='*60}")
        print(f"RESUMEN DE CARGA FINAL:")
        print(f"{'='*60}")
        print(f"Total tickets procesados: {len(df_cab)}")
        print(f"Tickets insertados correctamente: {tickets_insertados}")
        print(f"Tickets saltados (Error FK): {tickets_saltados}")
        
        if detalle_sin_variante > 0:
            print(f"\nLíneas de detalle perdidas (SKU no existe): {detalle_sin_variante}")
        
        if clientes_faltantes:
            print(f"\nDNIs Clientes no encontrados en BD: {list(clientes_faltantes)}")
        
        if sucursales_faltantes:
            print(f"\nSucursales no encontradas en BD: {list(sucursales_faltantes)}")
        print(f"{'='*60}\n")

    except Exception as e:
        conn.rollback()
        print(f"Error crítico en LOAD: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    load_ventas()