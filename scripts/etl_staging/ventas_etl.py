import pandas as pd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config.db_config import get_connection

def get_mapeo_campanias():
    try:
        conn = get_connection()
        if not conn: return {}
        query = "SELECT promocion_id, campania_id FROM campanias_promociones;"
        df_mapeo = pd.read_sql(query, conn)
        conn.close()
        return dict(zip(df_mapeo['promocion_id'], df_mapeo['campania_id']))
    except Exception as e:
        print(f"Error conectando a la BDD: {e}")
        return {}

def transform_ventas():
    input_path = 'data/raw/ventas.csv'
    output_dir = 'data/processed/ventas'
    output_cabecera = os.path.join(output_dir, 'ventas_cabecera_proc.csv')
    output_detalle = os.path.join(output_dir, 'ventas_detalle_proc.csv')

    if not os.path.exists(input_path):
        print(f"Error: No se encuentra el archivo {input_path}")
        return

    print("Iniciando transformación de ventas...")
    df = pd.read_csv(input_path)

    # --- 1. LIMPIEZA Y NORMALIZACIÓN INICIAL ---
    df['fecha_hora'] = pd.to_datetime(df['fecha_hora'], errors='coerce')
    
    # Normalizar Canal (Corrige 'web' -> 'Web' y quita espacios)
    if 'canal' in df.columns:
        df['canal'] = df['canal'].astype(str).str.strip().str.capitalize()
    
    # Normalizar DNI y SKU (Crucial para el match en el Load)
    df['dni_cliente'] = df['dni_cliente'].fillna(0).astype(int).astype(str).str.strip()
    df['sku_variante'] = df['sku_variante'].astype(str).str.strip().str.upper()

    # --- 2. ASIGNACIÓN DE CAMPAÑA (Dinamica desde BDD) ---
    mapeo_promo_campania = get_mapeo_campanias()
    if 'promocion_id' in df.columns:
        # Mapeamos y forzamos a Int64 (permite NaN sin convertir a float 4.0)
        df['campania_id'] = df['promocion_id'].map(mapeo_promo_campania).astype('Int64')

    # --- 3. FILTROS DE CALIDAD ---
    df = df.dropna(subset=['ticket_nro', 'fecha_hora', 'dni_cliente', 'sku_variante'])
    df = df[(df['cantidad'] > 0) & (df['precio_unitario_cobrado'] >= 0)]

    # --- 4. CABECERA ---
    agg_dict = {
        'fecha_hora': 'first',
        'dni_cliente': 'first',
        'sucursal': 'first',
        'empleado_id': 'first',
        'metodo_pago_id': 'first',
        'canal': 'first',
        'subtotal': 'sum'
    }
    
    if 'campania_id' in df.columns:
        # Usamos 'max' para capturar la campaña si alguna fila del ticket la tiene
        agg_dict['campania_id'] = 'max'

    df_cabecera = df.groupby('ticket_nro', as_index=False).agg(agg_dict)
    
    df_cabecera.rename(columns={
        'subtotal': 'total_venta',
        'canal': 'canal_venta'
    }, inplace=True)

    # Asegurar que canal_venta nunca sea nulo para la base de datos
    df_cabecera['canal_venta'] = df_cabecera['canal_venta'].fillna('Presencial')

    # --- 5. DETALLE ---
    detalle_cols = ['ticket_nro', 'sku_variante', 'cantidad', 
                    'precio_unitario_cobrado', 'costo_unitario_historico', 'subtotal']

    if 'promocion_id' in df.columns:
        detalle_cols.append('promocion_id')

    df_detalle = df[detalle_cols].copy()
    if 'promocion_id' in df_detalle.columns:
        df_detalle['promocion_id'] = df_detalle['promocion_id'].astype('Int64')

    # --- 6. GUARDADO ---
    os.makedirs(output_dir, exist_ok=True)
    df_cabecera.to_csv(output_cabecera, index=False)
    df_detalle.to_csv(output_detalle, index=False)
    print("Transformación completada exitosamente.")

if __name__ == "__main__":
    transform_ventas()