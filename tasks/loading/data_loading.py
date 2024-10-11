# data_loading.py
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from config.db_config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

def create_db_engine():
    """Crea y retorna una conexión de SQLAlchemy al Data Warehouse."""
    try:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        logging.info("Conexión a la base de datos PostgreSQL establecida correctamente.")
        return engine
    except SQLAlchemyError as e:
        logging.error(f"Error al crear el motor de base de datos: {e}")
        return None

def load_to_data_warehouse(df, table_name):
    """Carga un DataFrame en el Data Warehouse en la tabla especificada."""
    # Verificar que el DataFrame no esté vacío antes de cargar
    if df.empty:
        logging.warning(f"El DataFrame está vacío. No se cargará en la tabla {table_name}.")
        return

    # Crear motor de conexión
    engine = create_db_engine()
    if engine is None:
        logging.error(f"Conexión a la base de datos fallida. No se cargará el DataFrame en la tabla {table_name}.")
        return

    try:
        # Cargar los datos en la base de datos
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Datos cargados correctamente en la tabla '{table_name}'.")
    except ValueError as ve:
        logging.error(f"Error de valor al cargar datos en la tabla '{table_name}': {ve}")
    except SQLAlchemyError as sae:
        logging.error(f"Error de SQL al cargar datos en la tabla '{table_name}': {sae}")
    except Exception as e:
        logging.error(f"Error inesperado al cargar datos en la tabla '{table_name}': {e}")
    finally:
        # Cerrar la conexión
        engine.dispose()
        logging.info("Conexión a la base de datos cerrada.")