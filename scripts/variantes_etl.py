import pandas as pd
import sys
from pathlib import Path

# Agregar la raíz del proyecto al path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.db_config import get_connection

# Rutas
raw_path = project_root / 'data/raw/maestro_productos.xlsx'
processed_path = project_root / 'data/processed/dimensiones'



def obtener_diccionarios_bd():
    """Obtiene colores y talles de la BD"""
    conn = get_connection()
    if conn is None:
        raise Exception("No se pudo conectar a la base de datos")
    
    cur = conn.cursor()
    
    # Colores
    cur.execute("SELECT id, descripcion FROM colores")
    colores_dict = {desc: id for id, desc in cur.fetchall()}
    
    # Talles
    cur.execute("SELECT id, descripcion FROM talles")
    talles_dict = {desc: id for id, desc in cur.fetchall()}
    
    cur.close()
    conn.close()
    
    return colores_dict, talles_dict


def etl_variantes():
    """ETL para variantes (producto-color-talle)"""
    
    if not raw_path.exists():
        print(f"Error: No se encuentra {raw_path}")
        return
    
    print("Leyendo datos desde Excel...")
    df = pd.read_excel(raw_path)
    
    print("Obteniendo colores y talles desde la base de datos...")
    colores_dict, talles_dict = obtener_diccionarios_bd()
    print(f" {len(colores_dict)} colores y {len(talles_dict)} talles cargados desde BD")
    
    print("Procesando variantes...")
    variantes = df[[
        'Categoria', 'Producto', 'Color', 'Talle', 'SKU', 'Stock_Minimo'
    ]].copy()
    
    # Limpieza
    variantes['SKU'] = variantes['SKU'].str.strip().str.upper()
    variantes['Stock_Minimo'] = variantes['Stock_Minimo'].fillna(0).astype(int)
    variantes['Producto'] = variantes['Producto'].str.strip()
    variantes['Categoria'] = variantes['Categoria'].str.strip()
    
    # Validaciones
    variantes = variantes[variantes['Stock_Minimo'] >= 0]
    variantes = variantes.drop_duplicates(subset=['SKU'])
    
    # Mapear color y talle a IDs
    variantes['color_id'] = variantes['Color'].map(colores_dict)
    variantes['talle_id'] = variantes['Talle'].map(talles_dict)
    
    # Verificar que todos los colores y talles existan
    if variantes['color_id'].isna().any():
        colores_faltantes = variantes[variantes['color_id'].isna()]['Color'].unique()
        print(f"Advertencia: Los siguientes colores no existen en la BD:")
        for col in colores_faltantes:
            print(f"   - {col}")
        variantes = variantes.dropna(subset=['color_id'])
    
    if variantes['talle_id'].isna().any():
        talles_faltantes = variantes[variantes['talle_id'].isna()]['Talle'].unique()
        print(f"Advertencia: Los siguientes talles no existen en la BD:")
        for tal in talles_faltantes:
            print(f"   - {tal}")
        variantes = variantes.dropna(subset=['talle_id'])
    
    # Preparar DataFrame final
    # Nota: producto_id se asignará en el script de carga, aquí guardamos la referencia por nombre
    variantes_final = variantes[[
        'Categoria', 'Producto', 'talle_id', 'color_id', 'SKU', 'Stock_Minimo'
    ]].copy()
    
    variantes_final.columns = [
        'categoria_nombre', 'producto_nombre', 'talle_id', 
        'color_id', 'sku', 'stock_minimo'
    ]
    variantes_final = variantes_final.reset_index(drop=True)
    
    # Guardar
    output_path = processed_path / 'variantes.csv'
    variantes_final.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"{len(variantes_final)} variantes procesadas → {output_path}")
    print(f"\nColumnas: categoria_nombre, producto_nombre, talle_id, color_id, sku, stock_minimo")
    print("Nota: producto_id se resolverá en el script de carga")


if __name__ == "__main__":
    etl_variantes()