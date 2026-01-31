import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from config.db_config import get_engine

def get_mapeo(engine, tabla_dw, col_origen, col_dw):
    """Carga mapeos usando SQLAlchemy."""
    query = text(f"SELECT {col_origen}, {col_dw} FROM dw.{tabla_dw}")
    with engine.connect() as conn:
        result = conn.execute(query)
        return {row[0]: row[1] for row in result}

def cargar_fact_ventas():
    # Obtener engine de SQLAlchemy
    engine = get_engine()
    
    print("Cargando diccionarios de mapeo...")
    map_producto = get_mapeo(engine, "dim_producto", "producto_id_origen", "id_producto")    
    map_cliente   = get_mapeo(engine, "dim_cliente", "cliente_id_origen", "id_cliente")
    map_sucursal  = get_mapeo(engine, "dim_sucursal", "sucursal_id_origen", "id_sucursal")
    map_empleado  = get_mapeo(engine, "dim_empleado", "empleado_id_origen", "id_empleado")
    map_promocion = get_mapeo(engine, "dim_promocion", "promocion_id_origen", "id_promocion")
    map_canal     = get_mapeo(engine, "dim_canal", "canal_venta", "id_canal")
    map_pago      = get_mapeo(engine, "dim_metodo_pago", "metodo_pago", "id_metodo_pago")

    print("Extrayendo datos de origen...")
    query_origen = """ 
            SELECT 
            vc.id AS venta_id, vc.fecha_hora, vc.cliente_id, vc.sucursal_id, 
            vc.empleado_id, vc.canal_venta, mp.nombre AS metodo_pago_nombre,
            vd.variante_id, -- <--- USAR EL ID DE LA VARIANTE
            vd.cantidad, vd.precio_unitario_cobrado, 
            vd.costo_unitario_historico, vd.subtotal, vd.promocion_id
        FROM public.ventas_cabecera vc
        INNER JOIN public.ventas_detalle vd ON vc.id = vd.venta_id
        INNER JOIN public.metodos_pago mp  ON vc.metodo_pago_id = mp.id
    """
    df = pd.read_sql(query_origen, engine)

    print(f"Procesando e insertando {len(df)} registros...")
    
    # Usar transacción para mejor rendimiento
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dw.fact_ventas CASCADE"))

        for _, row in df.iterrows():
            try:
                # Lookups
                id_p = map_producto.get(row["variante_id"])                
                id_c = map_cliente.get(row["cliente_id"])
                id_s = map_sucursal.get(row["sucursal_id"])
                id_e = map_empleado.get(row["empleado_id"])
                id_can = map_canal.get(row["canal_venta"])
                id_mp = map_pago.get(row["metodo_pago_nombre"])

                
                # Manejo especial para promoción
                id_promo = map_promocion.get(row["promocion_id"]) if pd.notna(row["promocion_id"]) else None

                # Fecha a ID_Tiempo e ID degenerado
                fecha = row["fecha_hora"].date()
                id_tiempo = int(row["fecha_hora"].strftime("%Y%m%d"))
                nro_ticket = f"V-{row['venta_id']}"

                # Validación de claves obligatorias
                if any(x is None for x in [id_p, id_c, id_s, id_e, id_can, id_mp]):
                    print(f"DEBUG: Venta {row['venta_id']} saltada. Producto: {id_p}, Cliente: {id_c}, Sucursal: {id_s}")
                    continue
                insert_query = text("""
                    INSERT INTO dw.fact_ventas (
                        id_producto, id_cliente, id_tiempo, fecha, id_sucursal,
                        id_empleado, id_promocion, id_canal, id_metodo_pago,
                        cantidad, precio_unitario_cobrado, costo_historico,
                        subtotal, nro_ticket
                    ) VALUES (
                        :id_p, :id_c, :id_t, :fecha, :id_s,
                        :id_e, :id_promo, :id_can, :id_mp, 
                        :cant, :precio, :costo, :subtotal, :ticket
                    )
                    """)

                conn.execute(insert_query, {
                    'id_p': id_p,
                    'id_c': id_c,
                    'id_t': id_tiempo,
                    'fecha': fecha,
                    'id_s': id_s,
                    'id_e': id_e,
                    'id_promo': id_promo,
                    'id_can': id_can,
                    'id_mp': id_mp,
                    'cant': row["cantidad"],
                    'precio': row["precio_unitario_cobrado"],
                    'costo': row["costo_unitario_historico"],
                    'subtotal': row["subtotal"],
                    'ticket': nro_ticket
                })

            except Exception as e:
                print(f"Error en venta {row['venta_id']}: {e}")

    print("Carga de Fact_Ventas finalizada con éxito.")

if __name__ == "__main__":
    cargar_fact_ventas()