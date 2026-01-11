import pandas as pd
import random
from datetime import datetime, timedelta
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.db_config import get_connection

def obtener_promociones():
    """Obtiene las promociones desde la base de datos"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, tipo_descuento, valor_descuento, fecha_inicio, fecha_fin FROM promociones")
        promos_db = cur.fetchall()
        cur.close()
        conn.close()
        return promos_db
    except Exception as e:
        print(f"Error al obtener promociones: {e}")
        return []
def aplicar_promocion(precio_original, fecha_venta, promos_db):
    # Filtrar solo promos que estén vigentes en la fecha de la venta
    promos_validas = [
        p for p in promos_db 
        if p[3] <= fecha_venta.date() <= p[4]
    ]
    
    if promos_validas and random.random() < 0.20: # 20% de probabilidad de tener promo
        promo = random.choice(promos_validas)
        id_p, tipo, valor, _, _ = promo
        valor = float(valor)
        if tipo == 'Porcentaje':
            precio_final = precio_original * (1 - (valor / 100))
        else: # Monto Fijo
            precio_final = max(0, precio_original - valor)
            
        return round(float(precio_final), 2), id_p
    
    return precio_original, None   
def generar_ventas_csv():
    random.seed(42)
    promos_db = obtener_promociones()
    ruta_base = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    ruta_productos = os.path.join(ruta_base, 'maestro_productos.xlsx')
    ruta_clientes = os.path.join(ruta_base, 'clientes.xlsx')
    try:
        df_productos = pd.read_excel(ruta_productos)
        df_clientes = pd.read_excel(ruta_clientes)
    except FileNotFoundError:
        print("no se encontro los excels de productos y clientes")
        return
    info_productos = df_productos.set_index('SKU')[['Precio_Lista','Costo_Estimado']].to_dict('index')
    skus=list(info_productos.keys())
    dnis = df_clientes['dni'].tolist()

    data_ventas=[]
    fecha_inicio=datetime(2024,1,1)
    # 
    for ticket_nro in range(1,3001):
        #datos de la venta cabecera
        fecha=fecha_inicio + timedelta(
            #700 dias 2 años aprox, 24hs en minutos
            days=random.randint(0,700),
            minutes=random.randint(0,1440)
        )
        dni = random.choice(dnis)
        metodo_pago=random.choice([1,2,3,4,5])
        empleado= random.choice([1,2,3,4,5,6,7,8,9,10])
        # Determinar canal y sucursal
        canales = ['Presencial', 'Web']
        canal = random.choices(
            canales, 
            weights=[60, 40]  
        )[0]
        
        if canal == 'Web':
            sucursal = 'Tienda Online'
        else:  # Presencial
            sucursal = random.choice(['Centro', 'Norte', 'Shopping'])
        
        #items del ticket
        cant_items=random.randint(1,5)
        
        for _ in range(cant_items):
            sku=random.choice(skus)
            precio=info_productos[sku]['Precio_Lista']
            costo = info_productos[sku]['Costo_Estimado']
            #probalidad de 90 a 10 entre 1 o 2
            #choices devuelve una lista
            cantidad=random.choices([1,2],weights=[90,10])[0]

            precio_cobrado, promo_aplicada_id = aplicar_promocion(precio, fecha, promos_db)

            
            data_ventas.append({
                "ticket_nro": ticket_nro,
                "fecha_hora": fecha,
                "dni_cliente": dni,
                "sucursal": sucursal,
                "canal": canal,
                "empleado_id": empleado,
                "metodo_pago_id": metodo_pago,
                "sku_variante": sku,
                "cantidad": cantidad,
                "costo_unitario_historico": costo,
                "precio_unitario_cobrado": precio_cobrado,
                "promocion_id": promo_aplicada_id,
                "subtotal": round(precio_cobrado * cantidad, 2)
            })
    df_final =pd.DataFrame(data_ventas)
    path= "data/raw/ventas.csv"
    df_final.to_csv(path, index=False)
    print(f"generadas{len(df_final)} lineas de venta en 3000 tickets")

if __name__ == "__main__":
    generar_ventas_csv() 
        