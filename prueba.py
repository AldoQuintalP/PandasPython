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

# Variables globales
workng_dir = None
sandbx = None
reportes = None
db_config = None
columnas_esperadas = {}

def cargar_reportes(cliente_numero):
    ruta_config = os.path.join('CLIENTS', cliente_numero, 'Config', 'config.json')
    
    with open(ruta_config, 'r') as file:
        config_data = json.load(file)
    
    global reportes
    reportes = []
    for registro in config_data['registros']:
        dms_data = registro.get('dms', {})
        for reportes_lista in dms_data.values():
            reportes.extend(reportes_lista)
    
    return reportes

def obtener_columnas_esperadas(dms_name, reporte_name):
    dms_path = os.path.join('CLIENTS', 'dms', f'{dms_name}.json')
    
    if not os.path.exists(dms_path):
        logging.error(f"No se encontró el archivo {dms_path} para el DMS {dms_name}.")
        return None

    with open(dms_path, 'r') as file:
        dms_data = json.load(file)
    
    columnas_esperadas = dms_data.get('columnas_esperadas', {}).get(reporte_name, {}).get('columnas', [])
    
    if not columnas_esperadas:
        logging.warning(f"No se encontraron columnas esperadas para el reporte {reporte_name} en el DMS {dms_name}.")
    
    return columnas_esperadas

def encontrar_zip(carpeta):
    for archivo in os.listdir(carpeta):
        if archivo.endswith('.zip'):
            return os.path.join(carpeta, archivo)
    raise FileNotFoundError("No se encontró ningún archivo ZIP en la carpeta de trabajo.")

# Función para inferir el tipo de dato de una columna
def inferir_tipo_dato(serie):
    """
    Esta función intenta inferir el tipo de dato SQL basado en el contenido de la columna de un DataFrame.
    Por simplicidad, aquí siempre devuelve 'VARCHAR(255)', pero podría ser extendida para detectar tipos
    como INTEGER, FLOAT, etc.
    """
    # Aquí podrías implementar lógica para detectar diferentes tipos de datos
    return 'VARCHAR(255)'


# Extraer información del archivo ZIP
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

def procesar_archivo_zip():
    global workng_dir, sandbx, db_config, columnas_esperadas
    
    try:
        with open('database.json', 'r', encoding='utf-8') as config_file:
            config = json.load(config_file)
    except FileNotFoundError:
        logging.error("No se encontró el archivo de configuración 'database.json'.")
        exit()
    except json.JSONDecodeError:
        logging.error("El archivo de configuración 'database.json' no es un JSON válido.")
        exit()

    # Extraer la configuración de la base de datos
    db_config = {
        'host': config.get('host'),
        'usuario': config.get('usuario'),
        'contrasena': config.get('contrasena'),
        'base_de_datos': config.get('base_de_datos')
    }
    
    workng_dir = config.get('workng_dir')
    sandbx = config.get('sandbx')
    
    for file_name in os.listdir(workng_dir):
        if file_name.endswith('.zip'):
            nombre_zip = file_name
            break
    else:
        print("No se encontraron archivos .zip en el directorio.")
        return
    
    cliente_numero = nombre_zip[0:4].lstrip('0')
    cargar_reportes(cliente_numero)
    
    ruta_config = os.path.join('CLIENTS', cliente_numero, 'Config', 'config.json')
    with open(ruta_config, 'r') as file:
        config_data = json.load(file)

    for registro in config_data['registros']:
        dms_data = registro.get('dms', {})
        for dms_name, reportes_lista in dms_data.items():
            for reporte_name in reportes_lista:
                columnas = obtener_columnas_esperadas(dms_name, reporte_name)
                columnas_esperadas[reporte_name] = columnas

    print(f'Reportes : {reportes}')
    print(f"Configuración de la base de datos: {db_config}")
    print(f"Columnas esperadas para el cliente {cliente_numero}: {columnas_esperadas}")

    if not workng_dir or not sandbx or not reportes or not db_config:
        logging.error("Configuración incompleta en 'config.json'.")
        exit()

    # Implementación de las operaciones del Python B

    # Obtener la información
    try:
        workng = encontrar_zip(workng_dir)
        print(f'Encontrar el zip : {workng}')
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

    # Conectar a la base de datos
    conexion = conectar_db(db_config.get('host', ''), db_config.get('usuario', ''), db_config.get('contrasena', ''), db_config.get('base_de_datos', ''))

    # Obtener la versión del servidor PostgreSQL
    if conexion:
        version_servidor = obtener_version_servidor(conexion)
    else:
        version_servidor = "Desconocida"

    # Iterar sobre cada reporte y realizar las operaciones de creación de tabla e inserción
    for reporte in reportes:
        print(f'Sucursal: {sucursal}')
        print(f'Reporte: {filtrar_letras(reporte)}')
        nombre_tabla = f"{filtrar_letras(reporte)}{sucursal}"
        ruta_archivo = os.path.join(sandbx, f'{filtrar_letras(reporte)}{sucursal}.txt')
        print(f'Ruta_aarchivo: {ruta_archivo}')

        # Verificar existencia del archivo TXT
        if not os.path.isfile(ruta_archivo):
            logging.warning(f"El archivo TXT no se encontró en la ruta especificada: {ruta_archivo}. Omitiendo este reporte.")
            continue

        # Intentar leer el archivo con diferentes codificaciones
        codificaciones = ['utf-8', 'ISO-8859-1', 'latin1', 'Windows-1252']

        raw_data = None
        for codificacion in codificaciones:
            try:
                with open(ruta_archivo, 'r', encoding=codificacion) as f:
                    raw_data = f.read()
                break
            except UnicodeDecodeError:
                continue

        if raw_data is None:
            logging.error(f"No se pudo leer el archivo TXT {ruta_archivo} con ninguna de las codificaciones probadas.")
            continue

        # Se limpia el reporte de la basura
        raw_data_clean = limpiar_encabezado(raw_data)

        # Procesar el contenido del archivo TXT
        data = [row.split('|') for row in raw_data_clean.strip().split('\n')]

        # Usar encabezados esperados del archivo de configuración
        encabezados_esperados = columnas_esperadas.get(reporte, [])
        headers = encabezados_esperados
        rows = data

        # Comparar las columnas actuales con las esperadas
        columnas = data[0]
        columnas_esperadas_reporte = set(columnas_esperadas.get(reporte, []))
        # Verificar si al menos una columna coincide
        if columnas_esperadas_reporte.intersection(columnas):
            rows = data[1:] 
            logging.info(f"Al menos una columna de {nombre_tabla} coincide con las columnas esperadas en la configuración.")
        else:
            logging.info(f"El documento {nombre_tabla} no trae columnas.")

        # Asegurarse de que los nombres de las columnas sean únicos agregando sufijos
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
    print(f'Carpeta: {carpeta}')
    for archivo in os.listdir(carpeta):
        ruta_archivo = os.path.join(carpeta, archivo)
        try:
            if os.path.isfile(ruta_archivo):
                os.remove(ruta_archivo)
            elif os.path.isdir(ruta_archivo):
                os.rmdir(ruta_archivo)  # Para carpetas vacías
        except Exception as e:
            logging.error(f"No se pudo eliminar {ruta_archivo}: {e}")

# Función para limpiar encabezados basura de los reportes
def limpiar_encabezado(reporte):
    lines = reporte.strip().splitlines()
    for i, line in enumerate(lines):
        if '|' in line:
            return '\n'.join(lines[i:])
    return ''

# Función para filtrar solo letras de un nombre de archivo
def filtrar_letras(nombre):
    return re.sub(r'[^a-zA-Z]', '', nombre)

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

# Función para renombrar columnas
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

# Ejemplo de uso
procesar_archivo_zip()


