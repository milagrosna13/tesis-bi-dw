import pandas as pd
import random
import os


def genera_catalogo():
    random.seed(42)
    categorias={
      "Remeras": ["Remera Básica Algodón", "Remera Oversize", "Musculosa Deportiva"],
        "Pantalones": ["Jean Slim Fit", "Chupín Gabardina", "Jogging Urban"],
        "Abrigos": ["Campera Trucker", "Buzo Hoodie", "Sweater Lana"],
        "Accesorios": ["Gorra Snapback", "Cinturón Cuero"]
    }
    #diccionario
    colores= {"Rojo": "#FF0000",
        "Azul": "#0000FF",
        "Verde": "#00FF00",
        "Amarillo": "#FFFF00",
        "Naranja": "#FFA500",
        "Violeta": "#8B00FF",
        "Rosa": "#FFC0CB",
        "Celeste": "#87CEEB",
        "Turquesa": "#40E0D0",
        "Coral": "#FF7F50",
        "Lavanda": "#E6E6FA",
        "Menta": "#98FF98",
        "Negro": "#000000",
        "Blanco": "#FFFFFF",
        "Gris": "#808080",
        "Beige": "#F5F5DC",
        "Marrón": "#8B4513",
        "Bordó": "#800020",
        "Fucsia": "#FF00FF",
        "Índigo": "#4B0082"
    }
    talles = ["S", "M", "L", "XL", "XXL", "unico"]
    
    
    data=[]

    for cat_nombre,lista_productos in categorias.items():
        for prod_nombre in lista_productos:
            descripcion=f"{prod_nombre} calidad premium, temporada 2025"
            precio_lista=round(random.uniform(15000,45000),2)
            costo_base=precio_lista * 0.4
            #2 a 3 colores por productos
            colores_seleccionados = random.sample(list(colores.keys()), k=random.randint(2, 3))
            for col in colores_seleccionados:

                for tal in random.sample(talles, k=random.randint(3,5)):
                    #tomo los 3 primeros caracteres
                    sku = f"{prod_nombre[:3].upper()}-{col[:3].upper()}-{tal}"
                    data.append({
                        "Categoria": cat_nombre,
                        "Producto": prod_nombre,
                        "Descripcion": descripcion,
                        "Color": col,
                        "Codigo_Hex": colores[col],
                        "Talle": tal,
                        "Precio_Lista": precio_lista,
                        "Costo_Estimado": round(costo_base, 2),
                        "SKU": sku,
                        "Stock_Minimo": random.randint(3, 15),
                        "Activo": random.choice([True, True, True, False])
                    })
    df = pd.DataFrame(data)
    
    # Guardar a Excel
    path= "data/raw/maestro_productos.xlsx"
    df.to_excel(path,index=False)
    print(f"archivo generado existosamente en:{path}")

if __name__ == "__main__":
    genera_catalogo()