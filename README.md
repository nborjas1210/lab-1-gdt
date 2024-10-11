# Laboratorio de Pipeline ETL: `lab-1-gdt`

## Descripción

Este proyecto desarrolla un pipeline de ETL (Extracción, Transformación y Carga) para procesar y analizar datos, organizando la información en un Data Warehouse utilizando los modelos de Kimball e Inmon.

## Estructura del Proyecto

- **`datasets/`**: Contiene archivos de datos de entrada y salida.
- **`tasks/`**: 
  - **`extraction/`**: Extracción de datos desde archivos CSV y JSON.
  - **`transformation/`**: Limpieza, normalización y transformación de datos.
  - **`integration/`**: Integración de datos mediante operaciones de JOIN y agregaciones.
  - **`concatenation/`**: Concatenación de múltiples datasets.
  - **`loading/`**: Modelado dimensional y en tercera forma normal, carga de datos a PostgreSQL.
- **`config/`**: Contiene configuraciones de conexión a la base de datos.
- **`logs/`**: Archivos de log que documentan la ejecución y errores del pipeline.
- **`main_etl.py`**: Script principal que ejecuta el pipeline completo.
  
## Requisitos

1. **Python 3.8+** 
2. **Entorno Virtual**: Recomiendo crear un entorno virtual para instalar dependencias sin conflictos:
   ```bash
   python -m venv my_env
   source my_env/bin/activate  # Linux/macOS
   my_env\Scripts\activate     # Windows
