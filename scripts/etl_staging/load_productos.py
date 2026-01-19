import pandas as pd
import sys
from pathlib import Path

# Agregar la raíz del proyecto al path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.db_config import get_connection

# Rutas
processed_path = project_root / 'data/processed/dimensiones/productos.csv'


def load_productos():
    """Carga productos desde CSV a la base de datos"""
    
    if not processed_path.exists():
        print(f"Error: No se encuentra {processed_path}")
        print("Ejecuta primero: python scripts/etl_productos.py")
        return
    
    print(f"Leyendo datos desde {processed_path}...")
    df = pd.read_csv(processed_path)
    print(f"{len(df)} productos a cargar")
    
    conn = get_connection()
    if conn is None:
        print("No se pudo conectar a la base de datos")
        return
    
    cur = conn.cursor()
    
    try:
        # Limpiar tabla antes de cargar
        cur.execute("TRUNCATE TABLE productos RESTART IDENTITY CASCADE")
        print("Tabla productos limpiada")
        
        print("\nIniciando carga de productos...")
        insertados = 0
        errores = 0
        
        for _, row in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO productos (categoria_id, nombre, descripcion, precio_lista,fecha_alta, activo)
                    VALUES (%s, %s, %s, %s, %s,%s)
                """, (
                    int(row['categoria_id']),
                    row['nombre'],
                    row['descripcion'],
                    float(row['precio_lista']),
                    row['fecha_alta'],
                    bool(row['activo'])

                ))
                insertados += 1
                    
            except Exception as e:
                errores += 1
                print(f"Error en producto '{row['nombre']}': {e}")
                conn.rollback()  # Rollback del error individual
        
        conn.commit()
        
        print("\n=== Carga completada ===")
        print(f"Insertados: {insertados}")
        if errores > 0:
            print(f"Errores: {errores}")
        
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