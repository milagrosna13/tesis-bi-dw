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
        
        print(f" Cargando {len(df_cab)} órdenes de compra...")
        print(f"Total de líneas de detalle: {len(df_det)}")
        
        # --- LIMPIAR DATOS EXISTENTES ---
        print("\n Limpiando datos existentes...")
        cur.execute("DELETE FROM ordenes_compra_detalle;")
        detalle_borrados = cur.rowcount
        cur.execute("DELETE FROM ordenes_compra_cabecera;")
        cabecera_borrados = cur.rowcount
        print(f"   Cabeceras eliminadas: {cabecera_borrados}")
        print(f"   Detalles eliminados: {detalle_borrados}")
        
        # --- MAPEO DE VARIANTES ---
        cur.execute("SELECT sku, id FROM variantes")
        map_variantes = dict(cur.fetchall())
        
        
        # --- INSERTAR CABECERAS ---
        print("\nInsertando cabeceras...")
        valores_cabecera = []
        
        for _, row in df_cab.iterrows():
            valores_cabecera.append((
                int(row['orden_compra_nro']),
                int(row['proveedor_id']),
                row['fecha_pedido'],
                row['estado_pedido'],
                float(row['total_compra'])
            ))
        
        query_cab = """
            INSERT INTO ordenes_compra_cabecera (
                id, proveedor_id, fecha_pedido, estado_pedido, total_compra
            )
            OVERRIDING SYSTEM VALUE
            VALUES %s;
        """
        execute_values(cur, query_cab, valores_cabecera)
        print(f"   {len(valores_cabecera)} cabeceras insertadas")
        
        # --- INSERTAR DETALLES ---
        print("\nInsertando detalles...")
        valores_detalle = []
        
        for _, row in df_det.iterrows():
            oc_nro = int(row['orden_compra_nro'])
            sku = row['sku_variante']
            variante_id = map_variantes.get(sku)
            
            # Si el SKU no existe, se ignora (ya fue filtrado en transform)
            if variante_id is None:
                continue
            
            valores_detalle.append((
                oc_nro,
                variante_id,
                int(row['cantidad']),
                float(row['costo_unitario_pactado'])
            ))
        
        if valores_detalle:
            query_det = """
                INSERT INTO ordenes_compra_detalle (
                    orden_compra_id, variante_id, cantidad, costo_unitario_pactado
                )
                VALUES %s;
            """
            execute_values(cur, query_det, valores_detalle)
            print(f"   ✓ {len(valores_detalle)} detalles insertados")
        
        conn.commit()
        
        # --- RESUMEN FINAL ---
        print("\n" + "="*60)
        print("CARGA FINALIZADA")
        print(f"   Cabeceras: {len(valores_cabecera)}")
        print(f"   Detalles: {len(valores_detalle)}")
        
    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_compras()