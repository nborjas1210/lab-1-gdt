# data_integration.py
import pandas as pd
import logging
import os

# Realizar un JOIN entre Terrazas y Licencias
def join_terrazas_licencias(df_terrazas, df_licencias):
    logging.info("Realizando JOIN entre Terrazas y Licencias en la columna 'id_local'.")
    df_integrated = pd.merge(df_terrazas, df_licencias, on="id_local", how="inner")
    
    if df_integrated.empty:
        logging.warning("JOIN resultó en un DataFrame vacío. Archivo no guardado.")
    else:
        logging.info(f"JOIN completado. Número de filas resultantes: {len(df_integrated)}.")
    return df_integrated

# Agregar superficies por barrio
def aggregate_surface_by_barrio(df):
    logging.info("Calculando superficies totales por barrio.")
    df_aggregated = df.groupby(['id_barrio_local', 'desc_barrio_local'])['Superficie_ES'].sum().reset_index()
    logging.info("Superficie agregada por barrio calculada correctamente.")
    return df_aggregated

# Contar licencias por distrito
def count_licencias_by_distrito(df):
    logging.info("Contando licencias por distrito.")
    df_count = df.groupby(['id_distrito_local', 'desc_distrito_local']).size().reset_index(name='Cantidad_Licencias')
    logging.info("Conteo de licencias por distrito completado.")
    return df_count

# Filtrar terrazas con más de 70 m²
def filter_large_terrazas(df, min_surface=70):
    logging.info(f"Filtrando terrazas con una superficie mayor a {min_surface} m².")
    # Asegurarse de que la columna sea numérica
    df['Superficie_ES'] = pd.to_numeric(df['Superficie_ES'], errors='coerce')
    
    # Aplicar el filtro solo después de convertir
    df_large = df[df['Superficie_ES'] > min_surface]
    
    if df_large.empty:
        logging.warning("No se encontraron terrazas con superficie mayor al umbral especificado.")
    return df_large

def task3_process(df_terrazas, df_licencias):
    logging.info("Iniciando proceso de integración de datos - Tarea 3")

    # Parte a: Realizar JOIN entre terrazas normalizadas y licencias
    df_integrated = join_terrazas_licencias(df_terrazas, df_licencias)
    
    # Guardar el resultado del JOIN
    output_dir = 'datasets'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Directorio '{output_dir}' creado.")

    if not df_integrated.empty:
        df_integrated.to_csv(os.path.join(output_dir, 'Licencias_Terrazas_Integradas.csv'), index=False)
        logging.info("Dataset Licencias_Terrazas_Integradas guardado.")
    
    # Parte b: Agregar superficie por barrio
    df_surface_barrio = aggregate_surface_by_barrio(df_terrazas)
    df_surface_barrio.to_csv(os.path.join(output_dir, 'Superficies_Agregadas.csv'), index=False)
    logging.info("Dataset Superficies_Agregadas guardado.")
    
    # Parte c: Contar licencias por distrito
    df_licencias_distrito = count_licencias_by_distrito(df_licencias)
    df_licencias_distrito.to_csv(os.path.join(output_dir, 'Licencias_Por_Distrito.csv'), index=False)
    logging.info("Dataset Licencias_Por_Distrito guardado.")

    # Parte d: Filtrar terrazas grandes y agrupar por distrito y barrio
    df_large_terrazas = filter_large_terrazas(df_terrazas)
    if not df_large_terrazas.empty:
        df_large_terrazas.to_csv(os.path.join(output_dir, 'Terrazas_Grandes.csv'), index=False)
        logging.info("Dataset Terrazas_Grandes guardado.")

    logging.info("Tarea 3 completada.")
    print("Tarea 3 completada y guardada en 'datasets'.")
    
    return df_integrated, df_surface_barrio, df_licencias_distrito, df_large_terrazas
