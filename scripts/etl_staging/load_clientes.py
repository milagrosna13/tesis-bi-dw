import pandas as pd
import sys
from pathlib import Path

# Agregar la raíz del proyecto al path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.db_config import get_connection

# Ruta al archivo que ya tiene los IDs listos
processed_path = project_root / 'data/processed/dimensiones/clientes.csv'

def load_clientes():
    """Carga directa de clientes con IDs ya procesados"""
    
    if not processed_path.exists():
        print(f"Error: No se encuentra {processed_path}")
        return
    
    df = pd.read_csv(processed_path)
    
    conn = get_connection()
    if conn is None: return
    
    cur = conn.cursor()
    
    try:
        print(f"Iniciando carga de {len(df)} clientes...")
        insertados = 0
        actualizados = 0
        
        for _, row in df.iterrows():
            # Solo actualizamos datos que SÍ pueden cambiar (email, localidad)
            # NO tocamos nombre, apellido, fecha_nacimiento, genero
            cur.execute("""
                INSERT INTO clientes (nombre, apellido, dni, email, fecha_nacimiento, genero, localidad_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dni) DO UPDATE SET
                    email = EXCLUDED.email,
                    localidad_id = EXCLUDED.localidad_id
                RETURNING (xmax = 0) AS inserted
            """, (
                row['nombre'],
                row['apellido'],
                str(row['dni']),
                row['email'],
                row['fecha_nacimiento'],
                row['genero'],
                int(row['localidad_id'])
            ))
            
            result = cur.fetchone()
            if result and result[0]:
                insertados += 1
            else:
                actualizados += 1
        
        conn.commit()
        print(f"\nCarga finalizada. Insertados: {insertados}, Actualizados: {actualizados}")
        
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    load_clientes()