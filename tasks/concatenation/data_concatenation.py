# data_concatenation.py
import pandas as pd
import logging
import os

def concatenate_datasets(df1, df2=None):
    """
    Concatenar dos datasets de licencias, eliminando duplicados y verificando columnas clave.
    
    Parámetros:
        df1 (DataFrame): Primer dataset de licencias (obligatorio).
        df2 (DataFrame): Segundo dataset de licencias (opcional).
    
    Retorna:
        DataFrame: DataFrame concatenado sin duplicados.
    """
    logging.info("Iniciando la concatenación de datasets.")
    
    # Verificar que los DataFrames no estén vacíos y contengan columnas clave
    if df1.empty:
        logging.warning("El primer dataset está vacío. La concatenación no se completará.")
        return pd.DataFrame()  # Retornar un DataFrame vacío si el primer dataset está vacío

    if df2 is not None and df2.empty:
        logging.warning("El segundo dataset está vacío. Procediendo solo con el primer dataset.")
        df2 = None

    # Registro de cantidad de registros inicial
    logging.info(f"Registros iniciales - Dataset 1: {len(df1)}")
    if df2 is not None:
        logging.info(f"Registros iniciales - Dataset 2: {len(df2)}")

    # Realizar la concatenación si ambos DataFrames están disponibles
    if df2 is not None:
        if set(df1.columns) != set(df2.columns):
            logging.warning("Los datasets no tienen las mismas columnas. La concatenación podría resultar en un DataFrame con NaNs.")
        
        # Concatenar y eliminar duplicados
        df_concatenated = pd.concat([df1, df2], ignore_index=True).drop_duplicates().reset_index(drop=True)
        logging.info(f"Concatenación completada. Número de registros resultantes: {len(df_concatenated)}.")
    else:
        df_concatenated = df1  # Si solo hay un dataset, continuar con él
        logging.warning("Solo un archivo disponible. Concatenación omitida, se usará el dataset original.")

    # Asegurar existencia del directorio 'datasets'
    output_dir = 'datasets'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Directorio '{output_dir}' creado.")

    # Guardar el dataset concatenado
    output_file = os.path.join(output_dir, 'Licencias_Concatenadas.csv')
    df_concatenated.to_csv(output_file, index=False)
    logging.info(f"Dataset Licencias_Concatenadas guardado en '{output_file}'.")

    return df_concatenated

def task4_process(df_licencias_202104, df_licencias_202105=None):
    """
    Ejecuta el proceso de concatenación para los datasets de licencias.
    
    Parámetros:
        df_licencias_202104 (DataFrame): Dataset de licencias de abril 2021.
        df_licencias_202105 (DataFrame): Dataset de licencias de mayo 2021 (opcional).
    
    Retorna:
        DataFrame: DataFrame concatenado.
    """
    logging.info("Iniciando proceso de concatenación de licencias - Tarea 4.")
    df_concatenated = concatenate_datasets(df_licencias_202104, df_licencias_202105)
    logging.info("Tarea 4 completada: Dataset 'Licencias_Concatenadas' guardado en 'datasets/Licencias_Concatenadas.csv'")
    return df_concatenated
