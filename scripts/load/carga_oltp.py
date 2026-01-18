import logging

from load_productos import load_productos
from load_variantes import load_variantes
from load_clientes import load_clientes
from load_oc import load_compras
from load_ventas import load_ventas



logging.basicConfig(
    filename="etl_oltp.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_oltp_load():
    try:
        logging.info("Inicio carga OLTP")

        load_clientes()
        load_productos()   
        load_variantes()
        load_ventas()

        logging.info("Carga OLTP finalizada correctamente")

    except Exception as e:
        logging.error("Error en carga OLTP", exc_info=True)
        raise

if __name__ == "__main__":
    run_oltp_load()
