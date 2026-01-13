import pandas as pd
import random
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Agregar la raíz del proyecto al path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from config.db_config import get_connection

def generar_ordenes_compra():
    random.seed(42)

    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT id FROM proveedores")
    proveedores = [p[0] for p in cur.fetchall()]
    
    df_prod = pd.read_excel('data/raw/maestro_productos.xlsx')
    dict_prod = df_prod[['SKU', 'Costo_Estimado']].to_dict('records')

    data_raw = []
    oc_id_counter = 1
    
    for _ in range(50):
        prov = random.choice(proveedores)
        fecha = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365))
        estado = random.choice(['Recibido', 'Recibido', 'Pendiente'])
        
        items_en_oc = random.randint(3, 10)
        
        for _ in range(items_en_oc):
            prod = random.choice(dict_prod)
            cantidad = random.randint(50, 200)
            costo_pactado = round(prod['Costo_Estimado'] * random.uniform(0.9, 1.0), 2)
            
            data_raw.append({
                "orden_compra_id": oc_id_counter,
                "proveedor_id": prov,
                "fecha_pedido": fecha,
                "estado_pedido": estado,
                "sku_variante": prod['SKU'],
                "cantidad": cantidad,
                "costo_unitario_pactado": costo_pactado
            })
        
        oc_id_counter += 1

    pd.DataFrame(data_raw).to_csv('data/raw/oc_raw.csv', index=False)
    print("Órdenes de compra RAW generadas.")

    cur.close()
    conn.close()  

if __name__ == "__main__":
    generar_ordenes_compra()
