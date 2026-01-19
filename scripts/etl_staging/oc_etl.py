import pandas as pd
import os
import sys
import unicodedata

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config.db_config import get_connection

def normalizar_sku(s):
    if pd.isna(s):
        return s
    s = str(s).strip().upper()
    # Descompone + convierte compatibles
    s = unicodedata.normalize('NFKD', s)
    # Quita acentos/diacríticos
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return s

def get_skus_validos():
    """Obtiene la lista de SKUs válidos desde la BD"""
    try:
        conn = get_connection()
        if not conn:
            print("No se pudo conectar a la BD, no se validarán SKUs")
            return set()
        
        cur = conn.cursor()
        cur.execute("SELECT sku FROM variantes")
        skus = {row[0] for row in cur.fetchall()}
        conn.close()
        
        print(f"SKUs válidos cargados desde BD: {len(skus)}")
        return skus
    except Exception as e:
        print(f"Error obteniendo SKUs de la BD: {e}")
        return set()

def transform_ordenes_compra():
    input_path = 'data/raw/oc_raw.csv'
    output_dir = 'data/processed/compras/'
    output_cabecera = os.path.join(output_dir, 'oc_cabecera_proc.csv')
    output_detalle = os.path.join(output_dir, 'oc_detalle_proc.csv')

    if not os.path.exists(input_path):
        print(f"Error: No se encuentra el archivo {input_path}")
        return

    print("Iniciando transformación de órdenes de compra...")
    df = pd.read_csv(input_path)
    print(f"Registros iniciales: {len(df)}")

    # Normalizar nombres (RAW → PROCESSED)
    df.rename(columns={
        'orden_compra_id': 'orden_compra_nro',
        'proveedor_id': 'proveedor_id'
    }, inplace=True)

    # --- 1. LIMPIEZA GENERAL ---
    df['fecha_pedido'] = pd.to_datetime(df['fecha_pedido'], errors='coerce')

    antes = len(df)
    df = df.dropna(subset=[
        'orden_compra_nro',
        'fecha_pedido',
        'proveedor_id',
        'sku_variante'
    ])
    despues = len(df)
    print(f"Filas eliminadas por datos faltantes: {antes - despues}")

    # Validaciones de negocio
    df = df[df['cantidad'] > 0]
    df = df[df['costo_unitario_pactado'] >= 0]

    estados_validos = ['Pendiente', 'Recibido', 'Cancelado']
    df = df[df['estado_pedido'].isin(estados_validos)]

    # Formateo
    df['proveedor_id'] = df['proveedor_id'].astype(int)
    df['costo_unitario_pactado'] = df['costo_unitario_pactado'].round(2)
    
    # Normalizar SKU
    df['sku_variante'] = df['sku_variante'].apply(normalizar_sku)
    
    # --- 2. VALIDACIÓN DE SKUs CONTRA BD ---
    skus_validos = get_skus_validos()
    
    if skus_validos:
        antes_sku = len(df)
        skus_invalidos = df[~df['sku_variante'].isin(skus_validos)]['sku_variante'].unique()
        
        if len(skus_invalidos) > 0:
            print(f"\n SKUs no encontrados en BD ({len(skus_invalidos)}):")
            for sku in sorted(skus_invalidos):
                count = len(df[df['sku_variante'] == sku])
                print(f"  - {sku} ({count} registros)")
            
           
        
        # Filtrar solo SKUs válidos
        df = df[df['sku_variante'].isin(skus_validos)]
        despues_sku = len(df)
        print(f"Filas eliminadas por SKU inválido: {antes_sku - despues_sku}")
    
    # Subtotal
    df['subtotal'] = (df['cantidad'] * df['costo_unitario_pactado']).round(2)

    # --- 3. CABECERA ---
    agg_dict = {
        'fecha_pedido': 'first',
        'proveedor_id': 'first',
        'estado_pedido': 'first',
        'subtotal': 'sum'
    }

    df_cabecera = (
        df.groupby('orden_compra_nro')
        .agg(agg_dict)
        .reset_index()
        .rename(columns={'subtotal': 'total_compra'})
    )

    # --- 4. DETALLE ---
    df_detalle = df[[
        'orden_compra_nro',
        'sku_variante',
        'cantidad',
        'costo_unitario_pactado'
    ]].copy()

    # --- 5. GUARDADO ---
    os.makedirs(output_dir, exist_ok=True)
    df_cabecera.to_csv(output_cabecera, index=False)
    df_detalle.to_csv(output_detalle, index=False)

    print("\n" + "="*60)
    print("TRANSFORMACIÓN COMPLETADA")
    print(f"  Cabeceras: {len(df_cabecera)} órdenes {output_cabecera}")
    print(f"  Detalles: {len(df_detalle)} líneas {output_detalle}")

    print("\nColumnas cabecera:", list(df_cabecera.columns))
    print("Columnas detalle:", list(df_detalle.columns))
    
    print("\nResumen por estado:")
    print(df_cabecera['estado_pedido'].value_counts())

if __name__ == "__main__":
    transform_ordenes_compra()