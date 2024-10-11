# data_extraction.py
import pandas as pd
import logging
import os

ENCODING = 'ISO-8859-1'

def generar_variacion(df, nombre_nuevo_archivo):
    logging.info(f"Generando variación del archivo: {nombre_nuevo_archivo}")
    
    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce') + pd.DateOffset(months=1)
    else:
        logging.warning("La columna 'fecha' no se encontró; omitiendo ajuste de fechas.")

    if 'licencias_count' in df.columns:
        df['licencias_count'] = df['licencias_count'] * 1.05
    else:
        logging.warning("La columna 'licencias_count' no se encontró; omitiendo ajuste de conteo.")
    
    try:
        df.to_csv(nombre_nuevo_archivo, sep=';', index=False, encoding=ENCODING)
        logging.info(f"Archivo generado y guardado: {nombre_nuevo_archivo}")
        print(f"Archivo generado y guardado como {nombre_nuevo_archivo}.")
    except Exception as e:
        logging.error(f"Error al guardar el archivo {nombre_nuevo_archivo}: {e}")
        print(f"Error: No se pudo guardar el archivo {nombre_nuevo_archivo}.")

def extraer_datos():
    logging.info("Iniciando extracción de datos...")
    print("Extrayendo datos...")

    # Inicialización de variables para retorno
    df_licencias_202104, df_licencias_202105, df_terrazas, df_books = None, None, None, None

    try:
        df_licencias_202104 = pd.read_csv("datasets/Licencias_Locales_202104.csv", sep=';', encoding=ENCODING)
        df_terrazas = pd.read_csv("datasets/Terrazas_202104.csv", sep=';', encoding=ENCODING)
        df_books = pd.read_json("datasets/books.json", lines=True)
        logging.info("Datasets obligatorios cargados correctamente.")
        
        # Validación de que no están vacíos
        if df_licencias_202104.empty or df_terrazas.empty or df_books.empty:
            logging.error("Uno o más archivos obligatorios están vacíos. Abortar proceso de extracción.")
            print("Error: Uno o más archivos obligatorios están vacíos.")
            return None, None, None, None

    except FileNotFoundError as fnf_error:
        logging.error(f"Archivo no encontrado durante la extracción: {fnf_error}")
        print(f"Error: Archivo requerido no encontrado - {fnf_error}")
        return None, None, None, None
    except Exception as e:
        logging.error(f"Error inesperado durante la extracción de datos: {e}")
        print(f"Error: Error inesperado durante la extracción de datos - {e}")
        return None, None, None, None

    # Verificar y cargar archivo opcional, generando si falta
    ruta_licencias_202105 = "datasets/Licencias_Locales_202105.csv"
    try:
        if os.path.exists(ruta_licencias_202105):
            df_licencias_202105 = pd.read_csv(ruta_licencias_202105, sep=';', encoding=ENCODING)
            logging.info("Dataset opcional cargado: Licencias_Locales_202105.csv")
        else:
            logging.warning("Dataset opcional 'Licencias_Locales_202105.csv' no encontrado. Generando variación.")
            print("Advertencia: 'Licencias_Locales_202105.csv' no encontrado. Generando variación.")
            generar_variacion(df_licencias_202104, ruta_licencias_202105)
            df_licencias_202105 = pd.read_csv(ruta_licencias_202105, sep=';', encoding=ENCODING)
            logging.info("Variación generada y dataset opcional cargado: Licencias_Locales_202105.csv")
        
        # Validación de que el archivo opcional no está vacío
        if df_licencias_202105.empty:
            logging.warning("El archivo opcional 'Licencias_Locales_202105.csv' está vacío después de la generación.")
            df_licencias_202105 = None  # Marcarlo como None si está vacío

    except Exception as e:
        logging.error(f"Error al manejar el dataset opcional 'Licencias_Locales_202105.csv': {e}")
        print(f"Error: No se pudo procesar o generar el archivo opcional - {e}")
        return df_licencias_202104, None, df_terrazas, df_books

    logging.info("Extracción de datos completada correctamente.")
    print("Extracción de datos completada.")
    return df_licencias_202104, df_licencias_202105, df_terrazas, df_books
