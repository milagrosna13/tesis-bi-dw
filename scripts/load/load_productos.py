import pandas as pd
import sys
from pathlib import Path

# Agregar la raíz del proyecto al path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.db_config import get_connection

# Rutas
processed_path = project_root / 'data/processed/dimensiones/productos.csv'


def load_productos():
    """Carga productos desde CSV a la base de datos"""
    
    # Verificar que existe el archivo procesado
    if not processed_path.exists():
        print(f"Error: No se encuentra {processed_path}")
        print("Ejecuta primero: python scripts/etl_productos.py")
        return
    
    print(f"Leyendo datos desde {processed_path}...")
    df = pd.read_csv(processed_path)
    print(f"{len(df)} productos a cargar")
    
    # Conectar a la BD
    conn = get_connection()
    if conn is None:
        print("No se pudo conectar a la base de datos")
        return
    
    cur = conn.cursor()
    
    try:
        print("\nIniciando carga de productos...")
        insertados = 0
        actualizados = 0
        errores = 0
        
        for _, row in df.iterrows():
            try:
                # Intentar insertar
                cur.execute("""
                    INSERT INTO productos (categoria_id, nombre, descripcion, precio_lista, activo)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (nombre) DO UPDATE SET
                        categoria_id = EXCLUDED.categoria_id,
                        descripcion = EXCLUDED.descripcion,
                        precio_lista = EXCLUDED.precio_lista,
                        activo = EXCLUDED.activo
                    RETURNING (xmax = 0) AS inserted
                """, (
                    int(row['categoria_id']),
                    row['nombre'],
                    row['descripcion'],
                    float(row['precio_lista']),
                    bool(row['activo'])
                ))
                
                # xmax = 0 significa que fue INSERT, xmax > 0 significa UPDATE
                result = cur.fetchone()
                if result and result[0]:
                    insertados += 1
                else:
                    actualizados += 1
                    
            except Exception as e:
                errores += 1
                print(f"Error en producto '{row['nombre']}': {e}")
        
        # Commit
        conn.commit()
        
        print("\n=== Carga completada ===")
        print(f"Insertados: {insertados}")
        print(f"Actualizados: {actualizados}")
        if errores > 0:
            print(f"Errores: {errores}")
        
        # Mostrar total en BD
        cur.execute("SELECT COUNT(*) FROM productos")
        total = cur.fetchone()[0]
        print(f"\nTotal productos en BD: {total}")
        
    except Exception as e:
        conn.rollback()
        print(f"\nError durante la carga: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_productos()