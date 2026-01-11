import requests
#consumi api con requests
import sys
import os
#carpeta donde está el archivo actual:os.path.dirname(__file__), le saca .., y busca desde la raiz
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from config.db_config import get_connection

def seed_geo():
    conn=get_connection()
    if not conn:
        return
    #cursor permite ejecutar consultas sql a la base
    cur = conn.cursor()
    try:
        url_provs= "https://apis.datos.gob.ar/georef/api/provincias?campos=nombre"
        data_provs= requests.get(url_provs).json()

        for p in data_provs['provincias']:
            # Usamos ON CONFLICT para no duplicar si ejecutas el script dos veces
            cur.execute("""
            INSERT INTO provincias (nombre) 
            VALUES (%s) 
            ON CONFLICT (nombre) DO NOTHING;
            """, (p['nombre'],))
        conn.commit()
        print("pronvincias cargadas")

        cur.execute("select id, nombre from provincias;")
        provincias_db=cur.fetchall()#trae las filas a python

        for prov_id, prov_nombre in provincias_db:
            url_locs=f"https://apis.datos.gob.ar/georef/api/localidades?provincia={prov_nombre}&max=1000&campos=nombre"
            data_locs = requests.get(url_locs).json()

            for l in data_locs['localidades']:
                cur.execute("""
                    insert into localidades (nombre, provincia_id)
                    values (%s,%s);
                     """, (l['nombre'],prov_id))

        conn.commit()
        print("carga geografica completada")
    except Exception as e:
        conn.rollback()
        print(f"error durante la carga {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    seed_geo()