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



def obtener_categorias_dict():
    """Obtiene categorías de la BD y retorna dict {nombre: id}"""
    conn = get_connection()
    if conn is None:
        raise Exception("No se pudo conectar a la base de datos")
    
    cur = conn.cursor()
    cur.execute("SELECT id, nombre FROM categorias")
    cat_dict = {nombre: id for id, nombre in cur.fetchall()}
    
    cur.close()
    conn.close()
    
    return cat_dict


def etl_productos():
    """ETL para productos base (sin variantes)"""
    
    if not raw_path.exists():
        print(f"Error: No se encuentra {raw_path}")
        return
    
    print("Leyendo datos desde Excel...")
    df = pd.read_excel(raw_path)
    
    print("Obteniendo categorías desde la base de datos...")
    cat_dict = obtener_categorias_dict()
    print(f"{len(cat_dict)} categorías cargadas desde BD")
    
    print("Procesando productos...")
    productos = df.groupby(
        ['Categoria', 'Producto'],as_index=False).agg({'Descripcion': 'first',
        'Precio_Lista': 'mean',   
        'Activo': 'max' })
    
    
    # Limpieza
    productos['Producto'] = productos['Producto'].str.strip()
    productos['Descripcion'] = productos['Descripcion'].str.strip()
    productos['Precio_Lista'] = productos['Precio_Lista'].round(2)
    
    # Validaciones
    productos = productos[productos['Precio_Lista'] >= 0]
    
    # Mapear categoria_nombre a categoria_id
    productos['categoria_id'] = productos['Categoria'].map(cat_dict)
    
    # Verificar que todas las categorías existan
    if productos['categoria_id'].isna().any():
        categorias_faltantes = productos[productos['categoria_id'].isna()]['Categoria'].unique()
        print(f"Advertencia: Las siguientes categorías no existen en la BD:")
        for cat in categorias_faltantes:
            print(f"   - {cat}")
        productos = productos.dropna(subset=['categoria_id'])
    
    # Preparar DataFrame final
    productos_final = productos[[
        'categoria_id', 'Producto', 'Descripcion', 'Precio_Lista', 'Activo'
    ]].copy()
    
    productos_final.columns = ['categoria_id', 'nombre', 'descripcion', 'precio_lista', 'activo']
    productos_final = productos_final.reset_index(drop=True)
    
    # Guardar
    output_path = processed_path / 'productos.csv'
    productos_final.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"{len(productos_final)} productos procesados {output_path}")
    print(f"\nColumnas: categoria_id, nombre, descripcion, precio_lista, activo")


if __name__ == "__main__":
    etl_productos()