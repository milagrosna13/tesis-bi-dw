import pandas as pd #pandas para manejar datos en forma de tabla y excel
from faker import Faker #solo importo la clase Faker no toda la libreria
import random
import os

fake= Faker('es_AR')

def generar_clientes(n=500): #defino la funcion solo crear 500 por defecto
    # Esto hace que Faker siempre genere los mismos nombres en el mismo orden
    fake.seed_instance(42) 
    random.seed(42)
    clientes=[]
    
    for i in range (1, n+1): #del 1 al 500
        nombre =fake.first_name()
        apellido=fake.last_name()
        
        clientes.append({
            "id_cliente": i,
            "nombre": nombre,
            "apellido": apellido,
            "dni": random.randint(10000000, 45000000),
            "email": f"{apellido.lower()}.{nombre.lower()}@{fake.free_email_domain()}",#fstring
            "fecha_nacimiento":fake.date_of_birth(minimum_age=18, maximum_age=80),
            "direccion":fake.street_address(),
            "id_localidad":random.randint(1,50),
            "fecha_alta": fake.date_between(start_date='-3y',end_date='today'),
            "genero":random.choice(['F','M','X'])

        })

    df=pd.DataFrame(clientes)
    #guardo en mi pth
    
    path= "data/raw/clientes.xlsx"
    df.to_excel(path,index=False)
    print(f"archivo generado existosamente en:{path}")

if __name__ == "__main__":
    generar_clientes()
