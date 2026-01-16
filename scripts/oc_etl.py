import pandas as pd
import os

def transform_ordenes_compra():
    input_path = 'data/raw/oc_raw.csv'
    output_dir = 'data/processed/compras/'
    output_cabecera = os.path.join(output_dir, 'oc_cabecera_proc.csv')
    output_detalle = os.path.join(output_dir, 'oc_detalle_proc.csv')

    if not os.path.exists(input_path):
        print(f"Error: No se encuentra el archivo {input_path}")
        return

    print("Iniciando transformacion de ordenes de compra...")
    df = pd.read_csv(input_path)

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

    # Subtotal
    df['subtotal'] = (df['cantidad'] * df['costo_unitario_pactado']).round(2)

    # --- 2. CABECERA ---
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

    # --- 3. DETALLE ---
    df_detalle = df[[
        'orden_compra_nro',
        'sku_variante',
        'cantidad',
        'costo_unitario_pactado'
    ]].copy()

    # --- 4. GUARDADO ---
    os.makedirs(output_dir, exist_ok=True)
    df_cabecera.to_csv(output_cabecera, index=False)
    df_detalle.to_csv(output_detalle, index=False)

    print("Transformación completada.")
    print(f"Cabeceras: {len(df_cabecera)} órdenes")
    print(f"Detalles: {len(df_detalle)} líneas")

    print("\nColumnas cabecera:", list(df_cabecera.columns))
    print("Columnas detalle:", list(df_detalle.columns))
    print("\nResumen por estado:")
    print(df_cabecera['estado_pedido'].value_counts())

if __name__ == "__main__":
    transform_ordenes_compra()
