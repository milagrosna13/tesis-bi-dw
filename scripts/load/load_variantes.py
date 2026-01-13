import pandas as pd
import sys
from pathlib import Path

# Agregar la raíz del proyecto al path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.db_config import get_connection

# Rutas
processed_path = project_root / 'data/processed/dimensiones/variantes.csv'


def obtener_productos_dict():
    """Obtiene productos de la BD y retorna dict {(categoria_nombre, producto_nombre): producto_id}"""
    conn = get_connection()
    if conn is None:
        raise Exception("No se pudo conectar a la base de datos")
    
    cur = conn.cursor()
    cur.execute("""
        SELECT p.id, p.nombre, c.nombre as categoria
        FROM productos p
        JOIN categorias c ON p.categoria_id = c.id
    """)
    
    productos_dict = {(cat, nombre): id for id, nombre, cat in cur.fetchall()}
    
    cur.close()
    conn.close()
    
    return productos_dict


def load_variantes():
    """Carga variantes desde CSV a la base de datos"""
    
    # Verificar que existe el archivo procesado
    if not processed_path.exists():
        print(f" Error: No se encuentra {processed_path}")
        print("   Ejecuta primero: python scripts/etl_variantes.py")
        return
    
    print(f"Leyendo datos desde {processed_path}...")
    df = pd.read_csv(processed_path)
    print(f"{len(df)} variantes a cargar")
    
    # Obtener el mapeo de productos
    print("Obteniendo productos desde la BD...")
    productos_dict = obtener_productos_dict()
    print(f"{len(productos_dict)} productos disponibles en BD")
    
    # Conectar a la BD
    conn = get_connection()
    if conn is None:
        print("no se pudo conectar a la base de datos")
        return
    
    cur = conn.cursor()
    
    try:
        print("\nIniciando carga de variantes...")
        insertados = 0
        actualizados = 0
        errores = 0
        sin_producto = 0
        
        for _, row in df.iterrows():
            # Mapear producto_nombre a producto_id
            producto_key = (row['categoria_nombre'], row['producto_nombre'])
            producto_id = productos_dict.get(producto_key)
            
            if producto_id is None:
                sin_producto += 1
                print(f"Producto no encontrado: {row['categoria_nombre']} - {row['producto_nombre']}")
                continue
            
            try:
                # Intentar insertar
                cur.execute("""
                    INSERT INTO variantes (producto_id, talle_id, color_id, sku, stock_minimo)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (sku) DO UPDATE SET
                        producto_id = EXCLUDED.producto_id,
                        talle_id = EXCLUDED.talle_id,
                        color_id = EXCLUDED.color_id,
                        stock_minimo = EXCLUDED.stock_minimo
                    RETURNING (xmax = 0) AS inserted
                """, (
                    int(producto_id),
                    int(row['talle_id']),
                    int(row['color_id']),
                    row['sku'],
                    int(row['stock_minimo'])
                ))
                
                result = cur.fetchone()
                if result and result[0]:
                    insertados += 1
                else:
                    actualizados += 1
                    
            except Exception as e:
                errores += 1
                print(f"Error en SKU '{row['sku']}': {e}")
        
        # Commit
        conn.commit()
        
        print("\n=== Carga completada ===")
        print(f"Insertados: {insertados}")
        print(f"Actualizados: {actualizados}")
        if sin_producto > 0:
            print(f"Sin producto asociado: {sin_producto}")
        if errores > 0:
            print(f"Errores: {errores}")
        
        # Mostrar total en BD
        cur.execute("SELECT COUNT(*) FROM variantes")
        total = cur.fetchone()[0]
        print(f"\n Total variantes en BD: {total}")
        
    except Exception as e:
        conn.rollback()
        print(f"\n Error durante la carga: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_variantes()