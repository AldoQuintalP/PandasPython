import pandas as pd
import zipfile
import os
import re
import json
from datetime import datetime
import psycopg2
from psycopg2 import Error
import logging

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Leer configuración desde el archivo JSON
try:
    with open('config.json', 'r', encoding='utf-8') as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    logging.error("No se encontró el archivo de configuración 'config.json'.")
    exit()
except json.JSONDecodeError:
    logging.error("El archivo de configuración 'config.json' no es un JSON válido.")
    exit()

# Configuración
workng_dir = config.get('workng_dir')
sandbx = config.get('sandbx')
reportes = config.get('reportes', [])
db_config = config.get('db', {})
columnas_esperadas = config.get('columnas_esperadas', {})

if not workng_dir or not sandbx or not reportes or not db_config:
    logging.error("Configuración incompleta en 'config.json'.")
    exit()

# Función para conectar a la base de datos PostgreSQL
def conectar_db(host, usuario, contrasena, base_de_datos):
    try:
        conexion = psycopg2.connect(
            host=host,
            user=usuario,
            password=contrasena,
            database=base_de_datos
        )
        logging.info("Conexión a la base de datos establecida.")
        return conexion
    except Error as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        return None

# Función para ejecutar una consulta SQL
def ejecutar_consulta(conexion, consulta):
    try:
        cursor = conexion.cursor()
        cursor.execute(consulta)
        conexion.commit()
        logging.info("Consulta ejecutada con éxito.")
    except Error as e:
        logging.error(f"Error al ejecutar la consulta: {e}")

# Función para limpiar el contenido de una carpeta
def limpiar_carpeta(carpeta):
    for archivo in os.listdir(carpeta):
        ruta_archivo = os.path.join(carpeta, archivo)
        try:
            if os.path.isfile(ruta_archivo):
                os.remove(ruta_archivo)
            elif os.path.isdir(ruta_archivo):
                os.rmdir(ruta_archivo)  # Para carpetas vacías
        except Exception as e:
            logging.error(f"No se pudo eliminar {ruta_archivo}: {e}")

# Limpiar la carpeta Sandbx
limpiar_carpeta(sandbx)

# Encontrar el archivo ZIP en la carpeta de trabajo
def encontrar_zip(carpeta):
    for archivo in os.listdir(carpeta):
        if archivo.endswith('.zip'):
            return os.path.join(carpeta, archivo)
    raise FileNotFoundError("No se encontró ningún archivo ZIP en la carpeta de trabajo.")

# Extraer nombre del archivo ZIP
def extraer_info_zip(nombre_zip):
    nombre_sin_ext = os.path.splitext(os.path.basename(nombre_zip))[0]
    cliente = nombre_sin_ext[:4]
    sucursal = nombre_sin_ext[4:6]
    fecha_zip = nombre_sin_ext[6:]
    fecha_actual = datetime.now().strftime('%d/%m/%Y')
    with open(os.path.join(sandbx, 'Client.txt'), 'w', encoding='utf-8') as f:
        f.write(cliente)
    with open(os.path.join(sandbx, 'Branch.txt'), 'w', encoding='utf-8') as f:
        f.write(sucursal)
    with open(os.path.join(sandbx, 'Fecha.txt'), 'w', encoding='utf-8') as f:
        f.write(fecha_actual)
    return cliente, sucursal, fecha_actual

# Obtener la información
try:
    workng = encontrar_zip(workng_dir)
    cliente, sucursal, fecha_actual = extraer_info_zip(workng)
    cliente = cliente.lstrip('0')
except FileNotFoundError as e:
    logging.error(e)
    exit()

# Verificar existencia y permisos del archivo ZIP
if not os.path.isfile(workng):
    logging.error(f"El archivo ZIP no existe en la ruta especificada: {workng}")
    exit()
elif not os.access(workng, os.R_OK):
    logging.error(f"Permiso denegado para leer el archivo ZIP: {workng}")
    exit()

# Crear la carpeta Sandbx si no existe
if not os.path.isdir(sandbx):
    try:
        os.makedirs(sandbx, exist_ok=True)
        logging.info(f"Carpeta creada: {sandbx}")
    except PermissionError:
        logging.error(f"Permiso denegado para crear la carpeta: {sandbx}")
        exit()
    except Exception as e:
        logging.error(f"Se produjo un error al crear la carpeta: {e}")
        exit()

# Intentar descomprimir el archivo ZIP
try:
    with zipfile.ZipFile(workng, 'r') as zip_ref:
        zip_ref.extractall(sandbx)
        logging.info(f"Archivo descomprimido en {sandbx}")
except FileNotFoundError:
    logging.error(f"El archivo ZIP no se encontró en la ruta especificada: {workng}")
except PermissionError:
    logging.error(f"Permiso denegado para acceder al archivo ZIP o a la carpeta de destino.")
except Exception as e:
    logging.error(f"Se produjo un error al descomprimir el archivo: {e}")

# Función para filtrar solo letras de un nombre de archivo
def filtrar_letras(nombre):
    return re.sub(r'[^a-zA-Z]', '', nombre)

# Renombrar los archivos extraídos
for archivo in os.listdir(sandbx):
    for reporte in reportes:
        if archivo.startswith(reporte):
            nuevo_nombre_base = filtrar_letras(archivo)
            nuevo_nombre = f"{reporte}{sucursal}.txt"
            ruta_antigua = os.path.join(sandbx, archivo)
            ruta_nueva = os.path.join(sandbx, nuevo_nombre)
            os.rename(ruta_antigua, ruta_nueva)
            logging.info(f"Archivo renombrado de {archivo} a {nuevo_nombre}")

# Función para guardar consultas SQL en un archivo
def guardar_sql_dump(nombre_archivo, consultas, version_servidor):
    encabezado = (
        "-- PostgreSQL dump\n"
        "--\n"
        f"-- Host: localhost    Database: {db_config.get('base_de_datos', 'Desconocida')}\n"
        "-- ------------------------------------------------------\n"
        f"-- Server version {version_servidor}\n\n"
    )
    try:
        with open(nombre_archivo, 'w', encoding='utf-8') as f:
            f.write(encabezado)
            for consulta in consultas:
                f.write(consulta + '\n')
        logging.info(f"Archivo SQL dump generado: {nombre_archivo}")
    except Exception as e:
        logging.error(f"Se produjo un error al guardar el archivo SQL dump: {e}")

# Conectar a la base de datos
conexion = conectar_db(db_config.get('host', ''), db_config.get('usuario', ''), db_config.get('contrasena', ''), db_config.get('base_de_datos', ''))

# Función para obtener la versión del servidor PostgreSQL
def obtener_version_servidor(conexion):
    try:
        cursor = conexion.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        logging.info(f"Versión del servidor PostgreSQL: {version}")
        return version
    except Error as e:
        logging.error(f"Error al obtener la versión del servidor: {e}")
        return "Desconocida"

# Obtener la versión del servidor
if conexion:
    version_servidor = obtener_version_servidor(conexion)
else:
    version_servidor = "Desconocida"

# Iterar sobre cada reporte y realizar las operaciones de creación de tabla e inserción
for reporte in reportes:
    nombre_tabla = f"{reporte}{sucursal}"
    ruta_archivo = os.path.join(sandbx, f'{reporte}{sucursal}.txt')

    # Verificar existencia del archivo TXT
    if not os.path.isfile(ruta_archivo):
        logging.warning(f"El archivo TXT no se encontró en la ruta especificada: {ruta_archivo}. Omitiendo este reporte.")
        continue

    # Intentar leer el archivo con diferentes codificaciones
    codificaciones = ['utf-8', 'ISO-8859-1', 'latin1', 'Windows-1252']

    raw_data = None
    for codificacion in codificaciones:
        try:
            with open(ruta_archivo, 'r', encoding=codificacion) as file:
                raw_data = file.read()
            logging.info(f"Archivo {ruta_archivo} leído con éxito usando la codificación: {codificacion}")
            break
        except UnicodeDecodeError as e:
            logging.error(f"Error con la codificación {codificacion}: {e}")
        except FileNotFoundError:
            logging.error(f"El archivo TXT no se encontró en la ruta especificada: {ruta_archivo}")
            break
        except PermissionError:
            logging.error(f"Permiso denegado para leer el archivo TXT: {ruta_archivo}")
            break

    if raw_data is None:
        logging.error(f"No se pudo leer el archivo TXT {ruta_archivo} con ninguna de las codificaciones probadas.")
        continue

    # Procesar el contenido del archivo TXT
    data = [row.split('|') for row in raw_data.strip().split('\n')]

    # Usar encabezados esperados del archivo de configuración
    encabezados_esperados = columnas_esperadas.get(reporte, [])
    headers = encabezados_esperados
    rows = data

    # Asegurarse de que los nombres de las columnas sean únicos agregando sufijos
    def renombrar_columnas(headers):
        seen = {}
        new_headers = []
        for header in headers:
            if header in seen:
                seen[header] += 1
                new_header = f"{header}_{seen[header]}"
            else:
                seen[header] = 0
                new_header = header
            new_headers.append(new_header)
        return new_headers

    headers = renombrar_columnas(headers)

    # Ajustar el número de columnas en las filas
    max_columns = len(headers)
    adjusted_rows = []
    for row in rows:
        if len(row) < max_columns:
            row.extend([''] * (max_columns - len(row)))  # Completar con valores vacíos
        elif len(row) > max_columns:
            row = row[:max_columns]  # Truncar si hay más columnas de las esperadas
        adjusted_rows.append(row)

    # Crear el DataFrame con las columnas leídas del archivo
    df = pd.DataFrame(adjusted_rows, columns=headers)

    # Añadir columnas Client, Branch, Date
    df.insert(0, 'Client', cliente)
    df.insert(1, 'Branch', sucursal)
    df.insert(2, 'Date', fecha_actual)

    # Limpiar datos (si es necesario)
    df.fillna('', inplace=True)  # Rellenar valores nulos con cadenas vacías

    # Inferir el tipo de datos de cada columna
    def inferir_tipo_dato(serie):
        try:
            columna_clean = serie.replace(',', '').replace('', '0')  
            pd.to_numeric(columna_clean, errors='raise')
            if serie.str.contains(r'\.').any():
                return 'DECIMAL(10,2)'
            elif serie.astype(float).max() > 2147483647:  
                return 'BIGINT'
            return 'INTEGER'
        except (ValueError, TypeError):
            if serie.str.len().max() > 255:
                return 'TEXT'
            return 'VARCHAR(255)'

    # Comparar las columnas actuales con las esperadas
    columnas = set(df.columns)
    columnas_esperadas_reporte = set(columnas_esperadas.get(reporte, []))

    if not columnas_esperadas_reporte.intersection(columnas):
        logging.info(f"No se genera SQL dump para {nombre_tabla}. Las columnas no coinciden con las esperadas.")
        continue

    # Crear la consulta SQL para crear la tabla
    create_table_query = f"CREATE TABLE IF NOT EXISTS {nombre_tabla} (\n"
    create_table_query += "    Client VARCHAR(255),\n"
    create_table_query += "    Branch VARCHAR(255),\n"
    create_table_query += "    Date DATE,\n"
    for columna in df.columns[3:]:
        tipo_dato = inferir_tipo_dato(df[columna])
        create_table_query += f"    {columna} {tipo_dato},\n"
    create_table_query = create_table_query.rstrip(',\n') + "\n);"

    # Añadir ENGINE y CHARSET a la consulta SQL
    create_table_query += "\n-- ENGINE=InnoDB CHARSET=utf8mb4\n"

    # Crear la consulta SQL para eliminar la tabla si existe
    drop_query = f"DROP TABLE IF EXISTS {nombre_tabla};"

    # Crear la consulta SQL para insertar los datos
    insert_query = f"INSERT INTO {nombre_tabla} ({', '.join(df.columns)}) VALUES "
    values_list = df.apply(lambda x: tuple(x), axis=1).tolist()
    values_query = ', '.join([str(v) for v in values_list])
    insert_query += values_query + ";"

    # Guardar las consultas SQL en un archivo .sql.dump
    consultas = [
        f"-- Table structure for table {nombre_tabla}",
        drop_query,
        create_table_query,
        f"-- Dumping data for table {nombre_tabla}",
        insert_query
    ]
    archivo_sql = os.path.join(sandbx, f"{nombre_tabla}.sql.dump")
    guardar_sql_dump(archivo_sql, consultas, version_servidor)
    
    if conexion:
        ejecutar_consulta(conexion, drop_query)
        ejecutar_consulta(conexion, create_table_query)
        ejecutar_consulta(conexion, insert_query)

# Cerrar la conexión a la base de datos
if conexion:
    conexion.close()
    logging.info("Conexión a la base de datos cerrada.")
