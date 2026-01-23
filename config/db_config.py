import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Carga las variables del archivo .env

load_dotenv(encoding='utf-8')

#psycopg2 INSERT, UPDATE, DELETE cargas masivas
def get_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            port=os.getenv('DB_PORT')
        )
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None
    
#análisis de datos

#joins grandes de lectura
def get_connection_string():
    """Retorna connection string para SQLAlchemy"""
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_NAME')
    
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"

def get_engine():
    """Retorna engine de SQLAlchemy ya configurado"""
    return create_engine(get_connection_string())