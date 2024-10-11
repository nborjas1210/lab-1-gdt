# dimensional_modeling.py
import psycopg2
import logging
from config.db_config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

def connect_db():
    """Establecer conexión a la base de datos PostgreSQL."""
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        connection.autocommit = True
        logging.info("Conectado a la base de datos PostgreSQL.")
        return connection
    except Exception as error:
        logging.error(f"Error al conectar a la base de datos: {error}")
        return None

def create_dimUbicacion_table(cursor):
    """Crear tabla de Dimensión Ubicación."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimUbicacion (
                id_local INT PRIMARY KEY,
                id_distrito_local_x INT,
                id_barrio_local_x INT,
                id_tipo_acceso_local_x INT,
                desc_distrito_local_x INT,
                desc_barrio_local_x INT,
                cod_postal INT,
                coordenada_x_local_x DECIMAL,
                coordenada_y_local_x DECIMAL,
                desc_tipo_acceso_local_x VARCHAR(100),
                desc_situacion_local_x VARCHAR(100),
                secuencial_local_PC_x INT
            );
        """)
        logging.info("Tabla dimUbicacion creada correctamente.")
    except Exception as error:
        logging.error(f"Error al crear la tabla dimUbicacion: {error}")

def create_dimTerraza_table(cursor):
    """Crear tabla de Dimensión Terraza."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimTerraza (
                id_Terraza INT PRIMARY KEY,
                Nombre_Terraza VARCHAR(100),
                Hora_Ini_LJ_ES TIME,
                Hora_Fin_LJ_ES TIME,
                Hora_Ini_LJ_RA TIME,
                Hora_Fin_LJ_RA TIME,
                Hora_Ini_VS_ES TIME,
                Hora_Fin_VS_ES TIME,
                Hora_Ini_VS_RA TIME,
                Hora_Fin_VS_RA TIME
            );
        """)
        logging.info("Tabla dimTerraza creada correctamente.")
    except Exception as error:
        logging.error(f"Error al crear la tabla dimTerraza: {error}")

def create_dimEdificio_table(cursor):
    """Crear tabla de Dimensión Edificio."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimEdificio (
                id_edificio INT PRIMARY KEY,
                id_clase_ndp_edificio_x INT,
                id_vial_edificio_x INT,
                clase_vial_edificio_x VARCHAR(45),
                desc_vial_edificio_x VARCHAR(75),
                nom_edificio_x VARCHAR(55),
                num_edificio_x VARCHAR(45)
            );
        """)
        logging.info("Tabla dimEdificio creada correctamente.")
    except Exception as error:
        logging.error(f"Error al crear la tabla dimEdificio: {error}")

def create_dimLicencia_table(cursor):
    """Crear tabla de Dimensión Licencia."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimLicencia (
                id_licencia INT PRIMARY KEY,
                id_tipo_licencia INT, 
                id_tipo_situacion_licencia INT,
                desc_tipo_situacion_licencia VARCHAR(75),
                desc_tipo_licencia VARCHAR(75),
                Fecha_Dec_Lic DATE,
                ref_licencia VARCHAR(100)
            );
        """)
        logging.info("Tabla dimLicencia creada correctamente.")
    except Exception as error:
        logging.error(f"Error al crear la tabla dimLicencia: {error}")

def create_dimFecha_table(cursor):
    """Crear tabla de Dimensión Fecha."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimFecha (
                id_fecha INT PRIMARY KEY,
                fecha DATE,
                anio INT,
                mes INT,
                trimestre INT,
                dia INT,
                semana INT,
                semana_del_anio INT,
                dia_de_mes INT
            );
        """)
        logging.info("Tabla dimFecha creada correctamente.")
    except Exception as error:
        logging.error(f"Error al crear la tabla dimFecha: {error}")

def create_hechoLugar_table(cursor):
    """Crear tabla de Hecho Lugar."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hechoLugar (
                id_lugar INT PRIMARY KEY,
                terraza_id INT,
                licencia_id INT,
                ubicacion_id INT,
                edificio_id INT,
                fecha_id INT,
                fecha DATE,
                superficie_to INT,
                FOREIGN KEY (licencia_id) REFERENCES dimLicencia(id_licencia),
                FOREIGN KEY (terraza_id) REFERENCES dimTerraza(id_Terraza),
                FOREIGN KEY (edificio_id) REFERENCES dimEdificio(id_edificio),
                FOREIGN KEY (fecha_id) REFERENCES dimFecha(id_fecha),
                FOREIGN KEY (ubicacion_id) REFERENCES dimUbicacion(id_local)
            );
        """)
        logging.info("Tabla hechoLugar creada correctamente.")
    except Exception as error:
        logging.error(f"Error al crear la tabla hechoLugar: {error}")

def create_dimensional_tables():
    """Función principal para crear todas las tablas dimensionales."""
    connection = connect_db()
    if connection is None:
        logging.error("No se pudo conectar a la base de datos. Abortando creación de tablas.")
        return

    try:
        cursor = connection.cursor()
        create_dimUbicacion_table(cursor)
        create_dimTerraza_table(cursor)
        create_dimEdificio_table(cursor)
        create_dimLicencia_table(cursor)
        create_dimFecha_table(cursor)
        create_hechoLugar_table(cursor)
    except Exception as error:
        logging.error(f"Error general en la creación de tablas dimensionales: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            logging.info("Conexión cerrada con la base de datos PostgreSQL.")

# Ejecutar si se llama directamente
if __name__ == "__main__":
    create_dimensional_tables()
