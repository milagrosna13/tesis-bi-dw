import pandas as pd

df = pd.read_excel("data/raw/clientes.xlsx")

#mapeo genero para q coincida con la bdd
map_genero = {
    'F': 'Femenino',
    'M': 'Masculino',
    'X': 'otro'
}

df['genero'] = df['genero'].map(map_genero)

# Convertir DNI a string
df['dni'] = df['dni'].astype(str)

# Eliminar ID artificial
df = df.drop(columns=['id_cliente'])

# 3. Guardar PROCESSED
output_path = "data/processed/dimensiones/clientes.csv"
df.to_csv(output_path, index=False)

print(f"Clientes procesados guardados en {output_path}")
