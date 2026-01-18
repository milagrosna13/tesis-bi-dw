import pandas as pd
import os


def transform_ventas():
    # Rutas
    input_path = 'data/raw/ventas.csv'
    output_dir = 'data/processed/ventas'
    output_cabecera = os.path.join(output_dir, 'ventas_cabecera_proc.csv')
    output_detalle = os.path.join(output_dir, 'ventas_detalle_proc.csv')

    if not os.path.exists(input_path):
        print(f"Error: No se encuentra el archivo {input_path}")
        return

    print("Iniciando transformación de ventas...")
    df = pd.read_csv(input_path)

    # --- 1. LIMPIEZA GENERAL ---
    df['fecha_hora'] = pd.to_datetime(df['fecha_hora'], errors='coerce')

    antes = len(df)
    df = df.dropna(subset=[
        'ticket_nro',
        'fecha_hora',
        'dni_cliente',
        'sku_variante'
    ])
    despues = len(df)
    print(f"Filas eliminadas por datos faltantes: {antes - despues}")

    # Validaciones de negocio
    df = df[df['cantidad'] > 0]
    df = df[df['precio_unitario_cobrado'] >= 0]
    df = df[df['costo_unitario_historico'] >= 0]

    # Validación de canal
    if 'canal' in df.columns:
        df = df[df['canal'].isin(['Presencial', 'Web'])]

    # Formateo
    df['dni_cliente'] = df['dni_cliente'].apply(lambda x: str(int(float(x))) if pd.notnull(x) else x)
    df['sku_variante'] = df['sku_variante'].astype(str).str.strip().str.upper()
    df['precio_unitario_cobrado'] = df['precio_unitario_cobrado'].round(2)
    df['costo_unitario_historico'] = df['costo_unitario_historico'].round(2)
    df['subtotal'] = df['subtotal'].round(2)

    # --- 2. CABECERA ---
    agg_dict = {
        'fecha_hora': 'first',
        'dni_cliente': 'first',
        'sucursal': 'first',
        'empleado_id': 'first',
        'metodo_pago_id': 'first',
        'canal': 'first',
        'subtotal': 'sum'
    }

    # Si existe campaña
    if 'campania_id' in df.columns:
        agg_dict['campania_id'] = 'first'

    df_cabecera = (
        df
        .groupby('ticket_nro', as_index=False)
        .agg(agg_dict)
    )

    df_cabecera.rename(columns={
        'subtotal': 'total_venta',
        'canal': 'canal_venta'
    }, inplace=True)

    # --- 3. DETALLE ---
    detalle_cols = [
        'ticket_nro',
        'sku_variante',
        'cantidad',
        'precio_unitario_cobrado',
        'costo_unitario_historico',
        'subtotal'
    ]

    if 'promocion_id' in df.columns:
        detalle_cols.append('promocion_id')

    df_detalle = df[detalle_cols].copy()

    if 'promocion_id' in df_detalle.columns:
        # None en lugar de 0
        df_detalle['promocion_id'] = (
            df_detalle['promocion_id']
            .fillna(0)
            .astype(int)
            .replace(0, None)
        )

    # --- 4. GUARDADO ---
    os.makedirs(output_dir, exist_ok=True)
    df_cabecera.to_csv(output_cabecera, index=False)
    df_detalle.to_csv(output_detalle, index=False)

    print("Transformación completada.")
    print(f"Cabeceras: {len(df_cabecera)} tickets → {output_cabecera}")
    print(f"Detalles: {len(df_detalle)} líneas → {output_detalle}")
    print(f"\nColumnas cabecera: {list(df_cabecera.columns)}")
    print(f"Columnas detalle: {list(df_detalle.columns)}")


if __name__ == "__main__":
    transform_ventas()
