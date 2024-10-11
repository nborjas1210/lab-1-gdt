# inmon_modeling.py
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

def create_terraza_tables(cursor):
    """Crear tablas relacionadas con Terraza."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimPeriodoTerraza (
                id SERIAL PRIMARY KEY,
                des_periodo_terraza VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS dimSituacionTerraza (
                id SERIAL PRIMARY KEY,
                desc_situacion_terraza VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS dimComplementoTerraza (
                id SERIAL PRIMARY KEY,
                complemento VARCHAR(75),
                cantidad INT
            );

            CREATE TABLE IF NOT EXISTS dimTipoServicioTerraza (
                id SERIAL PRIMARY KEY,
                tipo_servicio VARCHAR(75),
                hora_inicio TIME,
                hora_fin TIME,
                terraza_id INT
            );

            CREATE TABLE IF NOT EXISTS dimTerraza (
                id SERIAL PRIMARY KEY,
                nro_terraza INT,
                nombreTerraza VARCHAR(100),
                periodo_terraza_id INT,
                situacion_terrza_id INT,
                complemento_terraza_id INT,
                tipo_servicio_terraza INT,
                FOREIGN KEY (periodo_terraza_id) REFERENCES dimPeriodoTerraza(id),
                FOREIGN KEY (situacion_terrza_id) REFERENCES dimSituacionTerraza(id),
                FOREIGN KEY (complemento_terraza_id) REFERENCES dimComplementoTerraza(id),
                FOREIGN KEY (tipo_servicio_terraza) REFERENCES dimTipoServicioTerraza(id)
            );
        """)
        logging.info("Tablas de Terraza creadas correctamente.")
    except Exception as error:
        logging.error(f"Error al crear tablas de Terraza: {error}")

def create_licencia_tables(cursor):
    """Crear tablas relacionadas con Licencia."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimSituacionLicencia (
                id SERIAL PRIMARY KEY,
                situacion VARCHAR(50) NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dimTipoLicencia (
                id SERIAL PRIMARY KEY,
                tipo VARCHAR(75)
            );

            CREATE TABLE IF NOT EXISTS dimFechaDecLic (
                id SERIAL PRIMARY KEY,
                fecha DATE,
                anio INT,
                mes INT,
                trimestre INT,
                dia INT,
                semana INT,
                semana_del_anio INT,
                dia_de_mes INT
            );

            CREATE TABLE IF NOT EXISTS hechoSeguimientoLicencia (
                id SERIAL PRIMARY KEY,
                situacion_licencia_id INT,
                tipo_licencia_id INT,
                terraza_id INT,
                fechaDecLic_id INT,
                FOREIGN KEY (situacion_licencia_id) REFERENCES dimSituacionLicencia(id),
                FOREIGN KEY (tipo_licencia_id) REFERENCES dimTipoLicencia(id),
                FOREIGN KEY (terraza_id) REFERENCES dimTerraza(id),
                FOREIGN KEY (fechaDecLic_id) REFERENCES dimFechaDecLic(id)
            );
        """)
        logging.info("Tablas de Licencia creadas correctamente.")
    except Exception as error:
        logging.error(f"Error al crear tablas de Licencia: {error}")

def create_ubicacion_tables(cursor):
    """Crear tablas relacionadas con Ubicación."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimDistrito (
                id SERIAL PRIMARY KEY,
                distrito VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS dimTipoVia (
                id SERIAL PRIMARY KEY,
                tipo VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS dimBarrio (
                id SERIAL PRIMARY KEY,
                barrio VARCHAR(75),
                dim_distrito_id INT,
                FOREIGN KEY (dim_distrito_id) REFERENCES dimDistrito(id)
            );

            CREATE TABLE IF NOT EXISTS dimEdificio (
                id SERIAL PRIMARY KEY,
                edificio VARCHAR(75),
                dim_barrio_id INT,
                dim_tipo_via_id INT,
                FOREIGN KEY (dim_barrio_id) REFERENCES dimBarrio(id),
                FOREIGN KEY (dim_tipo_via_id) REFERENCES dimTipoVia(id)
            );

            CREATE TABLE IF NOT EXISTS hechoUbicacion (
                id SERIAL PRIMARY KEY,
                dim_edificio_id INT,
                dim_terraza_id INT,
                superficie_to DECIMAL(7,2),
                FOREIGN KEY (dim_edificio_id) REFERENCES dimEdificio(id),
                FOREIGN KEY (dim_terraza_id) REFERENCES dimTerraza(id)
            );
        """)
        logging.info("Tablas de Ubicación creadas correctamente.")
    except Exception as error:
        logging.error(f"Error al crear tablas de Ubicación: {error}")

def create_inmon_tables():
    """Función principal para crear todas las tablas Inmon."""
    connection = connect_db()
    if connection is None:
        logging.error("No se pudo conectar a la base de datos. Abortando creación de tablas.")
        return

    try:
        cursor = connection.cursor()
        create_terraza_tables(cursor)
        create_licencia_tables(cursor)
        create_ubicacion_tables(cursor)
    except Exception as error:
        logging.error(f"Error general en la creación de tablas Inmon: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            logging.info("Conexión cerrada con la base de datos PostgreSQL.")

# Ejecutar si se llama directamente
if __name__ == "__main__":
    create_inmon_tables()
