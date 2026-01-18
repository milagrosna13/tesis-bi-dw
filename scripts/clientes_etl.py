import pandas as pd

# 1. Cargar datos RAW
df = pd.read_excel("data/raw/clientes.xlsx")

# Limpieza de nombres de columnas (espacios invisibles)
df.columns = df.columns.str.strip()

# 2. Mapeo de género
map_genero = {
    'F': 'Femenino',
    'M': 'Masculino',
    'X': 'otro'
}
df['genero'] = df['genero'].map(map_genero)


df['dni'] = (
    df['dni']
    .astype(str)
    .str.strip()
)

# 4. Eliminación de duplicados por email
cantidad_antes = len(df)
df = df.drop_duplicates(subset=['email'], keep='first')
cantidad_despues = len(df)

if cantidad_antes > cantidad_despues:
    print(f"Se eliminaron {cantidad_antes - cantidad_despues} registros con emails duplicados.")

# 5. Eliminar ID artificial si existe
if 'id_cliente' in df.columns:
    df = df.drop(columns=['id_cliente'])

# 6. Guardar PROCESSED
output_path = "data/processed/dimensiones/clientes.csv"
df.to_csv(output_path, index=False)

print(f"Clientes procesados guardados en {output_path}")
