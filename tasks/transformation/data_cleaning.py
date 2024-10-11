# data_cleaning.py
import pandas as pd
import numpy as np
import logging
import os

# Función para eliminar registros con más del 50% de valores nulos
def filter_null_records(df, dataset_name):
    limite = len(df.columns) * 0.5
    df_filtrado = df.dropna(thresh=limite)
    records_removed = len(df) - len(df_filtrado)
    logging.info(f"Se eliminaron {records_removed} registros en el dataset {dataset_name}.")
    return df_filtrado

# Función para normalizar columnas numéricas con un umbral
def normalizacion_necesaria(col, rango_umbral=950):
    return col.max() - col.min() > rango_umbral  # Solo normalizar si el rango es mayor al umbral

def normalize_numeric_columns(df, dataset_name, rango_umbral=950):
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df_normalized = df.copy()
    for col in numeric_cols:
        if normalizacion_necesaria(df[col], rango_umbral):  # Normalizar si el rango es mayor al umbral
            if df[col].std() != 0:
                df_normalized[col] = (df[col] - df[col].mean()) / df[col].std()
                logging.info(f"Columna '{col}' normalizada en el dataset {dataset_name}.")
            else:
                logging.warning(f"Columna '{col}' tiene desviación estándar 0 en el dataset {dataset_name}. No se normaliza.")
        else:
            logging.info(f"Columna '{col}' no requiere normalización en el dataset {dataset_name}.")
    return df_normalized

# Función para normalizar el campo _id en JSON
def normalize_id_field(df):
    if '_id' in df.columns:
        df['_id'] = df['_id'].apply(lambda x: str(x['$oid']) if isinstance(x, dict) and '$oid' in x else str(x))
    return df

# Función para crear la columna derivada
def add_derived_column(df, col1, col2, new_col_name):
    df[new_col_name] = df[col1] / df[col2].replace({0: np.nan})  # Evitar divisiones por cero
    return df

def task1_process(df_terrazas, df_licencias_202104, df_locales, df_books):
    """
    Realiza la primera etapa de transformación de datos: normalización y limpieza.
    """
    # Verificar que los DataFrames no sean None
    if df_terrazas is None:
        logging.error("El DataFrame 'df_terrazas' es None. Abortar Tarea 1.")
        return None, None, None, None
    if df_licencias_202104 is None:
        logging.error("El DataFrame 'df_licencias_202104' es None. Abortar Tarea 1.")
        return None, None, None, None
    if df_books is None:
        logging.error("El DataFrame 'df_books' es None. Abortar Tarea 1.")
        return None, None, None, None

    logging.info("Iniciando la limpieza y normalización de datos - Tarea 1")
    
    try:
        # Convertir diccionarios en las columnas a strings para evitar errores
        for df, nombre in zip([df_terrazas, df_licencias_202104, df_books], 
                              ['Terrazas', 'Licencias 202104', 'Books']):
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, dict)).any():
                    df[col] = df[col].apply(lambda x: str(x) if isinstance(x, dict) else x)
                    logging.info(f"Columna '{col}' en '{nombre}' convertida a string para evitar 'unhashable type: dict'.")

        # Ejemplo de normalización
        df_terrazas = filter_null_records(df_terrazas, 'Terrazas')
        df_terrazas_normalizadas = normalize_numeric_columns(df_terrazas, 'Terrazas')
        
        df_licencias_202104 = filter_null_records(df_licencias_202104, 'Licencias 202104')
        df_licencias_normalizadas = normalize_numeric_columns(df_licencias_202104, 'Licencias 202104')
        
        df_books = normalize_id_field(df_books)
        df_books = filter_null_records(df_books, 'Books')
        df_books_normalizadas = normalize_numeric_columns(df_books, 'Books')
        
        logging.info("Limpieza y normalización de datos completada para Tarea 1.")
        return df_terrazas_normalizadas, df_licencias_normalizadas, df_locales, df_books_normalizadas
    
    except Exception as e:
        logging.error(f"Error durante la transformación en Tarea 1: {e}")
        return None, None, None, None