# main_etl.py
import logging
import os
from tasks.extraction.data_extraction import extraer_datos
from tasks.transformation.data_cleaning import task1_process
from tasks.transformation.data_transformation import task2_process
from tasks.transformation.data_integration import task3_process
from tasks.concatenation.data_concatenation import task4_process
from tasks.loading.dimensional_modeling import create_dimensional_tables
from tasks.loading.inmon_modeling import create_inmon_tables
from tasks.loading.data_loading import load_to_data_warehouse

# Configuración del log
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, "etl_log.txt"), 
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main_etl():
    print("Iniciando el pipeline ETL...")
    logging.info("Iniciando el pipeline ETL...")

    # Extracción de datos
    try:
        df_licencias_202104, df_licencias_202105, df_terrazas, df_books = extraer_datos()
        logging.info(f"Extracción completada: Licencias 202104 - {len(df_licencias_202104)} registros")
        logging.info(f"Extracción completada: Licencias 202105 - {len(df_licencias_202105) if df_licencias_202105 is not None else 'Archivo no generado'}")
        logging.info(f"Extracción completada: Terrazas - {len(df_terrazas)} registros")
        logging.info(f"Extracción completada: Libros - {len(df_books)} registros")
        
        if any(df is None or df.empty for df in [df_licencias_202104, df_terrazas, df_books]):
            logging.error("Error: uno o más DataFrames esenciales no se extrajeron correctamente.")
            print("Error durante la extracción de datos. Verifica los registros de extracción.")
            return
        logging.info("Extracción de datos completada.")
    except Exception as e:
        logging.error(f"Error inesperado durante la extracción de datos: {e}")
        print(f"Error inesperado durante la extracción de datos: {e}")
        return

    # Transformación - Tarea 1
    try:
        df_terrazas_normalizadas, df_licencias_normalizadas, df_locales_normalizadas, df_books_normalizadas = task1_process(
            df_terrazas, df_licencias_202104, None, df_books
        )
        if any(df is None or df.empty for df in [df_terrazas_normalizadas, df_licencias_normalizadas, df_books_normalizadas]):
            logging.error("Error durante la transformación de datos - Tarea 1. Verifica los registros.")
            print("Error durante la transformación de datos - Tarea 1. Verifica los registros.")
            return
        logging.info("Transformación de datos - Tarea 1 completada.")
    except Exception as e:
        logging.error(f"Error durante la transformación de datos - Tarea 1: {e}")
        print(f"Error durante la transformación de datos - Tarea 1: {e}")
        return

    # Transformación - Tarea 2
    try:
        df_licencias_limpio, df_locales_limpio, df_terrazas_limpio, df_books_limpio = task2_process(
            df_licencias_normalizadas, df_locales_normalizadas, df_terrazas_normalizadas, df_books_normalizadas
        )
        logging.info("Transformación de datos - Tarea 2 completada.")
    except Exception as e:
        logging.error(f"Error durante la transformación de datos - Tarea 2: {e}")
        print(f"Error durante la transformación de datos - Tarea 2: {e}")
        return

    # Integración - Tarea 3
    try:
        df_joined, df_surface_barrio, df_licencias_distrito, df_large_terrazas = task3_process(df_terrazas_limpio, df_licencias_limpio)
        logging.info("Integración de datos - Tarea 3 completada.")
    except Exception as e:
        logging.error(f"Error durante la integración de datos - Tarea 3: {e}")
        print(f"Error durante la integración de datos - Tarea 3: {e}")
        return

    # Concatenación - Tarea 4
    try:
        df_concatenated = task4_process(df_licencias_202104, df_licencias_202105)
        logging.info("Concatenación de datos - Tarea 4 completada.")
    except Exception as e:
        logging.error(f"Error durante la concatenación de datos - Tarea 4: {e}")
        print(f"Error durante la concatenación de datos - Tarea 4: {e}")
        return

    # Carga de datos al Data Warehouse
    try:
        if not df_joined.empty:
            load_to_data_warehouse(df_joined, 'licencias_terrazas_integradas')
        if not df_concatenated.empty:
            load_to_data_warehouse(df_concatenated, 'licencias_concatenadas')
        if not df_surface_barrio.empty:
            load_to_data_warehouse(df_surface_barrio, 'superficies_agregadas')
    except Exception as e:
        logging.error(f"Error durante la carga de datos al Data Warehouse: {e}")
        print(f"Error durante la carga de datos al Data Warehouse: {e}")
        return

    # Modelado - Creación de Tablas Dimensionales y Modelo Inmon
    try:
        create_dimensional_tables()
        create_inmon_tables()
        logging.info("Modelado completado: tablas Kimball e Inmon creadas.")
    except Exception as e:
        logging.error(f"Error durante la creación de tablas de modelado: {e}")
        print(f"Error durante la creación de tablas de modelado: {e}")
        return

    logging.info("Pipeline ETL completado con éxito.")
    print("Pipeline ETL completado.")

if __name__ == "__main__":
    main_etl()