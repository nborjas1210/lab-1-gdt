# data_transformation.py
import pandas as pd
import os
import logging
from textblob import TextBlob

# Función para eliminar duplicados
def quitar_duplicados(df, key_columns):
    """Elimina duplicados basándose en columnas clave especificadas."""
    if df is None:
        logging.error("DataFrame es None, no se pueden eliminar duplicados.")
        return None
    df_limpio = df.drop_duplicates(subset=key_columns)
    records_removed = len(df) - len(df_limpio)
    logging.info(f"Se eliminaron {records_removed} registros duplicados de {key_columns}.")
    return df_limpio

# Función para correcciones ortográficas (con limitación para grandes volúmenes)
def correct_typographical_errors(text):
    """Aplica corrección ortográfica a un texto dado si es de tipo str y de longitud manejable."""
    if isinstance(text, str) and len(text) < 100:  # Corrección solo para textos menores a 100 caracteres
        return str(TextBlob(text).correct())
    return text

# Función para limpiar y normalizar cadenas de texto
def clean_text_columns(df, text_columns):
    """Limpia y normaliza columnas de texto en un DataFrame."""
    if df is None:
        logging.error("DataFrame es None, no se pueden limpiar y normalizar columnas de texto.")
        return None
    logging.info("Limpieza y normalización de texto en columnas categóricas.")
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].fillna('').apply(lambda x: ', '.join(x) if isinstance(x, list) else str(x))
            df[col] = df[col].str.lower().str.strip().replace(r'\s+', ' ', regex=True)
            df[col] = df[col].apply(correct_typographical_errors)
        else:
            logging.warning(f"Columna {col} no encontrada en el DataFrame.")
    return df

# Normalizar el campo _id en JSON
def normalize_id_field(df):
    """Normaliza el campo _id en un DataFrame JSON."""
    if df is None:
        logging.error("DataFrame es None, no se puede normalizar el campo _id.")
        return None
    if '_id' in df.columns:
        df['_id'] = df['_id'].apply(lambda x: str(x['$oid']) if isinstance(x, dict) and '$oid' in x else str(x))
        logging.info("Campo '_id' normalizado en el dataset.")
    else:
        logging.warning("El campo '_id' no está presente en el DataFrame.")
    return df

def task2_process(df_licencias, df_locales, df_terrazas, df_books):
    """Procesa la tarea de transformación: elimina duplicados y limpia columnas de texto."""
    logging.info("Iniciando proceso de transformación de datos - Tarea 2")

    # Validación y eliminación de duplicados
    df_licencias_limpio = quitar_duplicados(df_licencias, ['id_local', 'ref_licencia'])
    df_locales_limpio = quitar_duplicados(df_locales, ['id_local']) if df_locales is not None else None
    df_terrazas_limpio = quitar_duplicados(df_terrazas, ['id_terraza']) if df_terrazas is not None else None

    # Limpieza y corrección de texto en el dataset de libros
    text_columns = ['title', 'authors', 'categories']
    df_books_limpio = clean_text_columns(df_books, text_columns)
    
    # Normalizar el campo _id en el dataset de libros
    df_books_limpio = normalize_id_field(df_books_limpio)

    # Guardar los datasets transformados en el directorio 'datasets'
    output_dir = 'datasets'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Directorio '{output_dir}' creado para almacenar resultados.")

    # Guardar los archivos transformados
    try:
        if df_licencias_limpio is not None:
            df_licencias_limpio.to_csv(os.path.join(output_dir, 'Licencias_SinDuplicados.csv'), index=False)
        if df_locales_limpio is not None:
            df_locales_limpio.to_csv(os.path.join(output_dir, 'Locales_SinDuplicados.csv'), index=False)
        if df_terrazas_limpio is not None:
            df_terrazas_limpio.to_csv(os.path.join(output_dir, 'Terrazas_SinDuplicados.csv'), index=False)
        if df_books_limpio is not None:
            df_books_limpio.to_json(os.path.join(output_dir, 'Books_Limpio.json'), orient='records', lines=True)
        
        logging.info("Tarea 2 completada: duplicados eliminados y textos limpiados.")
        print("Tarea 2 completada y guardada en 'datasets'.")
    except Exception as e:
        logging.error(f"Error al guardar archivos transformados: {e}")

    return df_licencias_limpio, df_locales_limpio, df_terrazas_limpio, df_books_limpio