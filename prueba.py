import pandas as pd
import zipfile
import os
import re
import json
from datetime import datetime
import psycopg2
from psycopg2 import Error
import logging
import subprocess
from funcionesExternas import *



# Variables globales
workng_dir = None
sandbx = None
reportes = None
db_config = None
columnas_esperadas = {}
reportes_selec = []
dms_name = None

def configurar_logging(cliente_numero):
    # Crear la carpeta de logs si no existe
    logs_dir = os.path.join('CLIENTS', cliente_numero, 'Logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Crear el nombre del archivo de log basado en la fecha actual
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(logs_dir, f'log_{fecha_actual}.txt')
    
    # Crear un manejador de archivo
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # Crear un manejador de consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Configurar el formato del logging
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Obtener el logger raíz
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Limpiar cualquier manejador previo para evitar duplicados
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Añadir los manejadores al logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


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
    
    reportes = [''.join(filter(str.isalpha, reporte)) for reporte in reportes]
    
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


def extraer_columnas_valores_longitudes(consulta):
    print(f'Consulta a evaluar en extraer: {consulta}')
    try:
        # Validar que la consulta comience con 'INSERT INTO'
        if not consulta.strip().upper().startswith('INSERT INTO'):
            raise ValueError("La consulta no comienza con 'INSERT INTO'. Asegúrate de que la consulta sea correcta.")
        
        # Ajustar para tolerar más espacios y variaciones en la estructura de la consulta
        match_columnas = re.search(r'INSERT INTO\s+\w+\s*\((.*?)\)\s*VALUES', consulta, re.DOTALL)
        print(f'............... match_ {match_columnas}')
        match_valores = re.search(r'VALUES\s*\((.*?)\)\s*', consulta, re.DOTALL)
        print(f'March_vaores: {match_valores}')

        if not match_columnas or not match_valores:
            raise ValueError("No se pudieron extraer las columnas o valores de la consulta.")

        # Extraer y limpiar columnas
        columnas = [col.strip() for col in match_columnas.group(1).split(',')]

        # Extraer y limpiar valores
        valores = [val.strip().strip("'") for val in match_valores.group(1).split(',')]

        # Placeholder para las longitudes máximas
        longitudes_maximas = [25] * len(columnas)  # Placeholder, ajusta esto según tu esquema real

        return columnas, valores, longitudes_maximas
    
    except ValueError as e:
        print(f"Error: {e}")
        return [], [], []  # Retornar listas vacías como valor por defecto

# Función para inferir el tipo de dato de una columna
def inferir_tipo_dato(nombre_columna, dms_name, reporte_name):
    """
    Esta función devuelve el tipo de dato almacenado en la llave 'tipo' del JSON del DMS para la columna especificada.
    Si el tipo de dato es 'character varying', también incluye la longitud especificada en la llave 'length'.
    """
    # Obtener el data_type del DMS para la columna
    dms_path = os.path.join('CLIENTS', 'dms', f'{dms_name}.json')
    
    if not os.path.exists(dms_path):
        logging.error(f"No se encontró el archivo {dms_path} para el DMS {dms_name}.")
        return 'VARCHAR(255)'  # Devolver un tipo por defecto en caso de error

    with open(dms_path, 'r') as file:
        dms_data = json.load(file)
    
    # Acceder al tipo de dato de la columna en el JSON del DMS
    columna_info = dms_data.get('columnas_esperadas', {}).get(reporte_name, {}).get('data_types', {}).get(nombre_columna, {})
    tipo_dato = columna_info.get('tipo', 'VARCHAR(255)')
    
    # Si el tipo de dato es 'character varying', incluir la longitud
    if tipo_dato == 'character varying':
        longitud = columna_info.get('length', 255)  # Asume 255 si no se especifica una longitud
        tipo_dato = f'{tipo_dato}({longitud})'
    
    return tipo_dato


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
    global workng_dir, sandbx, db_config, columnas_esperadas, reportes_selec, dms_name

    
    
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
    limpiar_carpeta(sandbx)
    
    for file_name in os.listdir(workng_dir):
        if file_name.endswith('.zip'):
            nombre_zip = file_name
            break
            
    else:
        print("No se encontraron archivos .zip en el directorio.")
        return
    
    cliente_numero = nombre_zip[0:4].lstrip('0')
    
    #Cargan los logs
    configurar_logging(cliente_numero)
    logging.info("<--------------- Inicia el proceso --------------->")
    
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


    reportes_selec = list(columnas_esperadas.keys())
    print(f'Reportes : {reportes}')
    print(f"Configuración de la base de datos: {db_config}")
    print(f"Columnas esperadas para el cliente {cliente_numero}: {columnas_esperadas}")
    print(f'Reportes select: {reportes_selec}')


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

    # Limpiamos números de la variable reportes
    # Cleaning the list
    

    # Renombrar los archivos extraídos
    for archivo in os.listdir(sandbx):
        print(f'Archivo : {archivo}')
        
        for reporte in reportes:
            if archivo.startswith(reporte):
                nuevo_nombre_base = filtrar_letras(archivo)
                nuevo_nombre = f"{reporte}{sucursal}.txt"
                ruta_antigua = os.path.join(sandbx, archivo)
                ruta_nueva = os.path.join(sandbx, nuevo_nombre)
                os.rename(ruta_antigua, ruta_nueva)
                logging.info(f"Archivo renombrado de {archivo} a {nuevo_nombre}")


    try:
        # Conectar a la base de datos
        conexion = conectar_db(db_config.get('host', ''), db_config.get('usuario', ''), db_config.get('contrasena', ''), db_config.get('base_de_datos', ''))

        # Obtener la versión del servidor PostgreSQL
        if conexion:
            version_servidor = obtener_version_servidor(conexion)
        else:
            version_servidor = "Desconocida"

        # Iterar sobre cada reporte y realizar las operaciones de creación de tabla e inserción
        print(reportes)
        for reporte in reportes:
            print(f'Sucursal: {sucursal}')
        
            for item in reportes_selec:
                if reporte in item and reporte == ''.join([i for i in item if not i.isdigit()]):
                    reporte = item
                    break
            print(f'Reporte ? : {reporte}')
            #Tomamos el nombre del dms
            dms_name = obtener_dms_por_reporte(reporte, config_data)
            print(f'dms_name......: {dms_name}')

            nombre_tabla = f"{filtrar_letras(reporte)}{sucursal}"
            print(f'Nombre tabla: {nombre_tabla}')
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
            print(f'Encabezados esperados : {encabezados_esperados}')
            headers = encabezados_esperados
            rows = data
            #print(f'rows : ......... {rows}')
            

            # Comparar las columnas actuales con las esperadas
            columnas = data[0]
            print(f'Columnas reporte: {columnas}')
            columnas_esperadas_reporte = set(columnas_esperadas.get(reporte, []))
            print(f'Columnas esperadas: {columnas_esperadas_reporte}')
            # Verificar si al menos una columna coincide
            if columnas_esperadas_reporte.intersection(columnas):
                rows = data[1:] 
                logging.info(f"Al menos una columna de {nombre_tabla} coincide con las columnas esperadas en la configuración.")
            else:
                logging.info(f"El documento {nombre_tabla} no trae columnas.")

            # Asegurarse de que los nombres de las columnas sean únicos agregando sufijos
            headers = renombrar_columnas(headers)
            print(f'Headers: {headers}')

            # Ajustar el número de columnas en las filas
            max_columns = len(headers)
            adjusted_rows = []
            for row in rows:
                if len(row) < max_columns:
                    row.extend([''] * (max_columns - len(row)))  # Completar con valores vacíos
                elif len(row) > max_columns:
                    row = row[:max_columns]  # Truncar si hay más columnas de las esperadas
                adjusted_rows.append(row)

            # Paso 1: Filtrar los campos que contienen '(computed)'
            campos_computed = [campo for campo in headers if '(computed)' in campo]
            encabezados2 = [campo for campo in headers if '(computed)' not in campo]
            print(f'Campos Computed: {campos_computed}')
            print(f'Encabeza2: {encabezados2}')

            # Paso 2: Verificar y ajustar el número de columnas en adjusted_rows
            adjusted_rows = [row[:len(encabezados2)] for row in adjusted_rows]

            # Paso 3: Crear el DataFrame sin los campos calculados
            df = pd.DataFrame(adjusted_rows, columns=encabezados2)
            # Convertir las fechas en el DataFrame antes de la inserción
            df = convertir_fechas_df(df, dms_name, reporte)

            # Paso 4: Aplicar las fórmulas a todas las columnas que tengan fórmulas en encabezados2
            aplicar_formulas(df, dms_name, reporte)

            # Paso 5: Aplicar las fórmulas para las columnas calculadas (computed)
            print(f'Campos_computed: {campos_computed}')
            for campo_calculado in campos_computed:
                formula = obtener_formula(dms_name, reporte, campo_calculado)
                print(f'Formula campo calculado: {formula}')
                if formula:
                    try:
                        print(df.columns)
                        for col in df.columns:
                            formula = formula.replace(col, f"df['{col}']")
                        print(f'Formula ajustada: {formula}')
                        
                        # Identificar las columnas en la fórmula
                        columnas_en_formula = re.findall(r"df\['(.*?)'\]", formula)
                        
                        # Convertir las columnas a tipo numérico, manejando errores y NaNs
                        for col in columnas_en_formula:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            if df[col].isnull().any():
                                logging.warning(f"Valores no numéricos en la columna '{col}' fueron convertidos a 0.")
                                df[col] = df[col].fillna(0)
                        
                        # Evaluar la fórmula
                        df[campo_calculado] = eval(formula)
                        logging.info(f"Fórmula aplicada a la columna calculada '{campo_calculado}'.")

                        # Renombrar la columna quitando '(computed)'
                        nuevo_nombre = campo_calculado.replace(' (computed)', '')
                        df.rename(columns={campo_calculado: nuevo_nombre}, inplace=True)
                        logging.info(f"El encabezado de la columna '{campo_calculado}' fue cambiado a '{nuevo_nombre}'.")

                    except Exception as e:
                        logging.error(f"Error al aplicar la fórmula en la columna calculada '{campo_calculado}': {e}")

            # Paso 6: Reordenar las columnas para respetar la posición original de las columnas calculadas
            for campo_calculado in campos_computed:
                nuevo_nombre = campo_calculado.replace('(computed)', '').strip()
                if nuevo_nombre in df.columns:
                    pos_original = encabezados_esperados.index(campo_calculado)
                    encabezados_esperados[pos_original] = nuevo_nombre  # Actualizar el nombre sin '(computed)'

            # Asegurar que las columnas estén en el orden esperado
            df = df[encabezados_esperados]

            # Obtener los nuevos encabezados después de aplicar las fórmulas y reordenar
            nuevos_encabezados = df.columns.to_list()
            print(f'Nuevos Encabezados: {nuevos_encabezados}')

        
            
            # Añadir columnas Client, Branch, Date
            df.insert(0, 'Client', cliente)
            df.insert(1, 'Branch', sucursal)
            df.insert(2, 'Date', fecha_actual)

            # Limpiar datos (si es necesario)
            df.fillna('', inplace=True)  # Rellenar valores nulos con cadenas vacías

            # Crear la consulta SQL para crear la tabla
            create_table_query = f"CREATE TABLE IF NOT EXISTS {nombre_tabla} (\n"
            create_table_query += "    Client character varying(255),\n"
            create_table_query += "    Branch character varying(255),\n"
            create_table_query += "    Date character varying(20),\n"
            
            for a in nuevos_encabezados:
                tipo_dato = inferir_tipo_dato(a, dms_name, reporte)
                print(f'Reporte: {reporte} columna : {a} .. {tipo_dato}')
                create_table_query += f"    {a} {tipo_dato},\n"
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
                # Verificar si alguna columna excede la longitud permitida antes de insertar
                longitudes_maximas = obtener_column_lengths(archivo_sql)
                print(f'Longitudes_maximas _ {longitudes_maximas}')
                
                ejecutar_consulta(conexion, drop_query)
                ejecutar_consulta(conexion, create_table_query)
                ejecutar_consulta_insert(conexion, insert_query, df=df, max_lengths=longitudes_maximas, nombre_tabla=nombre_tabla, version_servidor=version_servidor, archivo_sql=archivo_sql, drop_query=drop_query,create_table_query=create_table_query)

                


    finally:
        # Asegurarse de cerrar la conexión al final
        if conexion:
            conexion.close()
            logging.info("<--------------- Conexion a la base de datos cerrada. --------------->")
    


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
        logging.error(f"Error al ejecutar la consulta: {consulta} - {e}")


def ejecutar_consulta_insert(conexion, consulta, df=None, max_lengths=None, nombre_tabla=None, archivo_sql=None, version_servidor=None, drop_query=None, create_table_query=None):
    """
    Ejecuta una consulta SQL y maneja los errores relacionados con la longitud máxima de las columnas. 
    Si la consulta falla debido a un valor que excede la longitud máxima permitida en una columna, 
    ajusta los valores en el DataFrame y vuelve a intentar la inserción.
    Si el nuevo intento de inserción tiene éxito, se guarda el nuevo INSERT en el archivo .sql.dump.

    :param conexion: Objeto de conexión a la base de datos.
    :param consulta: Cadena de texto que contiene la consulta SQL a ejecutar.
    :param df: DataFrame que contiene los datos a insertar. (Opcional)
    :param max_lengths: Diccionario con las columnas como claves y sus longitudes máximas permitidas como valores. (Opcional)
    :param nombre_tabla: Nombre de la tabla en la base de datos donde se realizará la inserción. (Opcional)
    :param archivo_sql: Ruta del archivo .sql.dump donde se guardarán las consultas. (Opcional)
    :param version_servidor: Versión del servidor PostgreSQL. Necesario para la cabecera del archivo .sql.dump. (Opcional)
    :return: None
    """
    if df is not None and df.empty:
        logging.warning("El DataFrame está vacío, no se realizará ninguna operación.")
        return  # Salir de la función si el DataFrame está vacío

    try:
        cursor = conexion.cursor()
        cursor.execute(consulta)
        conexion.commit()
        logging.info("Consulta ejecutada con éxito.")
    except Error as e:
        logging.error(f"Error al ejecutar la consulta: {e}")
        logging.error(f"Consulta que falló: {consulta}")
        # Realiza un rollback para liberar la transacción
        conexion.rollback()

        # Proceder a ajustar el dataframe y reintentar la inserción
        if df is not None and max_lengths is not None:
            logging.info("Ajustando el dataframe y reintentando la inserción...")

            try:
                # Truncar los valores que exceden la longitud máxima permitida
                for col, max_len in max_lengths.items():
                    if col in df.columns and max_len is not None:
                        df[col] = df[col].apply(lambda x: x[:max_len] if isinstance(x, str) else x)
                
                # Verificar si después del ajuste el DataFrame sigue teniendo datos
                if df.empty:
                    logging.warning("El DataFrame está vacío después del ajuste, no se realizará ninguna operación.")
                    return  # Salir de la función si el DataFrame está vacío después del ajuste
                
                # Generar una nueva consulta INSERT con los datos ajustados
                columns = ', '.join(df.columns)
                values = ', '.join(str(tuple(row)) for row in df.values)
                new_insert_query = f"INSERT INTO {nombre_tabla} ({columns}) VALUES {values};"

                cursor.execute(new_insert_query)
                conexion.commit()
                logging.info("Inserción reintentada con éxito después del ajuste.")
                
                # Si se proporciona un archivo SQL, guardar solo el nuevo insert en el archivo
                if archivo_sql and version_servidor:
                    consultas = [
                        f"-- Table structure for table {nombre_tabla}",
                        drop_query,
                        create_table_query,
                        f"-- Dumping data for table {nombre_tabla} después de ajustar los datos",
                        new_insert_query
                    ]
                    guardar_sql_dump(archivo_sql, consultas, version_servidor)
            except KeyError as ke:
                logging.error(f"Error de columna en el DataFrame: {ke}")
            except Exception as e2:
                logging.error(f"Error al ejecutar la consulta después del ajuste: {e2}")
                conexion.rollback()  # Asegurarse de hacer rollback también aquí
        else:
            logging.error("El dataframe o las longitudes máximas son None, no se puede proceder con el ajuste.")
            conexion.rollback()  # Realiza un rollback para liberar la transacción






def generar_query_verificacion_longitudes(columnas, valores, longitudes_maximas):
    """
    Genera una consulta SQL para verificar si algún valor excede la longitud máxima permitida en una columna.

    :param columnas: Lista de nombres de las columnas de la tabla.
    :param valores: Lista de valores que se intentan insertar.
    :param longitudes_maximas: Diccionario con las columnas y sus longitudes máximas permitidas.
    :return: Consulta SQL como cadena de texto.
    """
    query = []
    
    for columna, valor in zip(columnas, valores):
        longitud_max = longitudes_maximas.get(columna)
        if longitud_max and valor:  # Evita las columnas con valores vacíos
            condicion = f"SELECT '{columna}' AS column_name, {longitud_max} AS character_maximum_length, LENGTH('{valor}') AS actual_length WHERE LENGTH('{valor}') > {longitud_max}"
            query.append(condicion)
    
    query_sql = " UNION ALL ".join(query)
    
    return query_sql


def extraer_longitudes_maximas(archivo_sql):
    """
    Esta función extrae las longitudes máximas para las columnas que tienen un tamaño definido
    (como VARCHAR, CHAR, etc.) en el archivo SQL dump, e ignora tipos como DATE y DOUBLE PRECISION.
    
    :param archivo_sql: Ruta del archivo SQL dump.
    :return: Diccionario con las columnas como claves y las longitudes máximas como valores.
    """
    longitudes_maximas = {}

    try:
        with open(archivo_sql, 'r', encoding='utf-8') as file:
            contenido = file.read()
            
        # Expresión regular para capturar columnas con longitudes definidas
        matches = re.findall(r'(\w+)\s+(VARCHAR|character varying)\((\d+)\)', contenido, re.IGNORECASE)
        

        # Crear un diccionario con las longitudes máximas solo para VARCHAR y CHAR
        for match in matches:
            columna, tipo, longitud = match
            longitudes_maximas[columna] = int(longitud)

    except FileNotFoundError:
        logging.error(f"No se encontró el archivo SQL: {archivo_sql}")
    except Exception as e:
        logging.error(f"Error al extraer longitudes máximas del archivo SQL: {e}")

    return longitudes_maximas


def verificar_longitudes_y_ajustar(fila, column_lengths):
    """
    Verifica si alguna columna de la fila excede la longitud máxima permitida y ajusta la fila si es necesario.

    :param fila: Una lista que representa una fila de datos.
    :param column_lengths: Un diccionario donde las claves son los nombres de las columnas y los valores son las longitudes máximas.
    :return: Una lista de errores, donde cada error es una cadena que describe la columna que excedió la longitud.
    """
    errores = []
    columnas = list(column_lengths.keys())
    print(f'Columnas esperadas: {columnas}')
    print(f'Fila original: {fila}')

    # Verificar si el número de columnas coincide con el número esperado
    if len(fila) < len(columnas):
        print("El número de columnas en la fila es menor que el número de columnas esperado.")
        fila = fila + [''] * (len(columnas) - len(fila))  # Rellenar con valores vacíos para coincidir con las columnas
        print(f'Fila ajustada con valores vacíos: {fila}')
    elif len(fila) > len(columnas):
        print("El número de columnas en la fila es mayor que el número de columnas esperado.")
        return ["Número de columnas excede el esperado."]

    # Iterar sobre los valores de la fila y los nombres de columnas en paralelo
    for col_name, valor in zip(columnas, fila):
        max_length = column_lengths.get(col_name)
        
        # Verificar si el valor no es None y si tiene una longitud que se pueda medir
        if valor is not None and isinstance(valor, str):
            # Comprobar si la longitud del valor excede la longitud máxima permitida
            if max_length is not None and len(valor) > max_length:
                error_msg = f"Error en columna '{col_name}' con valor '{valor}' y longitud máxima {max_length}"
                errores.append(error_msg)
                print(error_msg)
            else:
                print(f"Revisando columna '{col_name}' con valor '{valor}' y longitud máxima {max_length}")
        else:
            print(f"Columna '{col_name}' con valor no verificable (valor: {valor})")

    return errores



def obtener_column_lengths(archivo_sql):
    """
    Extrae las longitudes de las columnas que están dentro de los paréntesis en un archivo SQL de creación de tablas,
    excluyendo las columnas de tipo double precision y DATE.

    :param archivo_sql: Ruta al archivo SQL que contiene la declaración de creación de la tabla.
    :return: Un diccionario con los nombres de las columnas como claves y sus longitudes máximas como valores.
    """
    column_lengths = {}
    inside_parentheses = False
    with open(archivo_sql, 'r') as file:
        for line in file:
            # Detectar el inicio de las definiciones de columnas (dentro de paréntesis)
            if '(' in line:
                inside_parentheses = True
            elif ')' in line:
                inside_parentheses = False

            if inside_parentheses:
                # Buscar líneas que definan columnas, manejando diferentes formatos de definición
                match = re.search(r'(\w+\$?)\s+(character varying\((\d+)\)|\w+\s*\(?\d*\)?)', line, re.IGNORECASE)
                if match:
                    column_name = match.group(1)
                    column_type = match.group(2)
                    
                    # Filtrar palabras clave que no son columnas y tipos double precision y DATE
                    if column_name.upper() not in ["CREATE", "INSERT"] and \
                       "double precision" not in column_type.lower() and \
                       "date" not in column_type.lower():
                        # Verificar si el tipo de columna incluye una longitud específica
                        length_match = match.group(3)
                        if length_match:
                            column_lengths[column_name] = int(length_match)
                        else:
                            column_lengths[column_name] = None  # Para otros tipos de datos sin longitud fija
    
    return column_lengths




def limpiar_column_lengths(column_lengths):
    """
    Elimina las claves no deseadas ('DROP', 'CREATE', 'INSERT') del diccionario de longitudes de columnas.

    :param column_lengths: Diccionario con las columnas como claves y sus longitudes máximas como valores.
    :return: Diccionario limpio sin las claves no deseadas.
    """
    # Claves no deseadas
    claves_no_deseadas = ['DROP', 'CREATE', 'INSERT']

    # Eliminar las claves no deseadas del diccionario
    for clave in claves_no_deseadas:
        if clave in column_lengths:
            del column_lengths[clave]

    return column_lengths

def extraer_datos_insert(archivo_sql):
    """
    Extrae los datos de las consultas INSERT INTO desde un archivo .sql.dump y los organiza en una lista de tuplas.

    :param archivo_sql: Ruta al archivo .sql.dump.
    :return: Lista de tuplas con los datos extraídos.
    """
    with open(archivo_sql, 'r', encoding='utf-8') as file:
        contenido = file.read()

    # Buscar todas las consultas INSERT INTO
    insert_matches = re.findall(r"INSERT INTO \w+ \([^)]+\) VALUES (.+);", contenido, re.DOTALL)

    # Lista para almacenar los datos extraídos
    datos = []

    # Procesar cada consulta INSERT INTO encontrada
    for match in insert_matches:
        # Eliminar paréntesis iniciales y finales y dividir las filas
        filas = match.strip().strip('()').split("),(")
        for fila in filas:
            # Dividir los valores de la fila y limpiar los espacios en blanco y comillas
            valores = [val.strip().strip("'") for val in fila.split(",")]
            datos.append(tuple(valores))
    
    return datos


def limpiar_y_dividir_datos(datos, column_lengths, columnas_varying_list):
    """
    Esta función toma una lista de datos que contiene elementos incorrectamente combinados y los separa correctamente
    en tuplas, considerando solo las columnas que son de tipo character varying según el diccionario column_lengths.

    :param datos: Lista de datos donde los elementos están mal combinados.
    :param column_lengths: Un diccionario donde las claves son los nombres de las columnas y los valores son las longitudes máximas.
    :param columnas_varying_list: Lista de columnas que son de tipo character varying.
    :return: Lista de tuplas correctamente formadas con solo las columnas de tipo character varying.
    """
    # Filtrar columnas que son character varying y no tienen valor None
    columnas_varying = [col for col in columnas_varying_list if column_lengths.get(col) is not None]
    num_columnas_varying = len(columnas_varying)

    datos_limpios = []
    temp = []

    for item in datos:
        if isinstance(item, str):
            # Limpia el string y separa en elementos, tratando de quitar los paréntesis y comillas adicionales
            elementos = item.replace("')", "").replace("('", "").replace("'", "").split(", ")
            temp.extend(elementos)
        elif isinstance(item, tuple):
            temp.extend(list(item))

        # Procesar los datos si ya se tiene el número correcto de columnas
        while len(temp) >= num_columnas_varying:
            fila = temp[:num_columnas_varying]
            datos_limpios.append(tuple(fila))
            temp = temp[num_columnas_varying:]

    # Verificar si hay elementos residuales no utilizados
    if temp:
        print("Advertencia: Quedaron elementos sin procesar, pueden estar mal estructurados:", temp)
        # Si hay elementos residuales, asegúrate de agregarlos a la última fila correctamente
        if len(datos_limpios) > 0:
            ultima_fila = list(datos_limpios[-1]) + temp
            datos_limpios[-1] = tuple(ultima_fila[:num_columnas_varying])

    return datos_limpios

def obtener_columnas_varying_ordenadas(column_lengths):
    """
    Esta función toma un diccionario `column_lengths` y retorna una lista de nombres de columnas
    que son de tipo character varying (es decir, aquellas que no tienen valor `None`), 
    manteniendo el orden original.

    :param column_lengths: Diccionario con nombres de columnas y sus longitudes.
    :return: Lista de nombres de columnas que son de tipo character varying en orden.
    """
    columnas_varying_ordenadas = [col for col, length in column_lengths.items() if length is not None]
    return columnas_varying_ordenadas


def clean_and_split_list(raw_list, column_lengths):
    cleaned_list = []

    for item in raw_list:
        if isinstance(item, tuple):
            # Convertir cada elemento del tuple en una cadena y limpiar
            cleaned_item = [i.strip("()',") for i in item]
            cleaned_list.extend(cleaned_item)
        else:
            # En caso de que no sea tuple (por si acaso), limpiar directamente
            cleaned_item = item.strip("()',")
            cleaned_list.append(cleaned_item)
    
    # Determinar el número de columnas basado en la longitud del diccionario
    num_columns = len(column_lengths)

    # Agrupar los elementos en registros con el número de columnas determinado
    grouped_list = [cleaned_list[i:i + num_columns] for i in range(0, len(cleaned_list), num_columns)]
    
    return grouped_list




def ajustar_tamano_columna(conexion, nombre_tabla, columna, nuevo_tamano):
    """
    Ajusta el tamaño de una columna en la base de datos para permitir que valores más largos se inserten.

    :param conexion: La conexión a la base de datos PostgreSQL.
    :param nombre_tabla: El nombre de la tabla donde se encuentra la columna.
    :param columna: El nombre de la columna cuyo tamaño se desea ajustar.
    :param nuevo_tamano: El nuevo tamaño máximo para la columna.
    """
    try:
        cursor = conexion.cursor()
        alter_query = f"ALTER TABLE {nombre_tabla} ALTER COLUMN {columna} TYPE character varying({nuevo_tamano});"
        cursor.execute(alter_query)
        conexion.commit()
        logging.info(f"Tamaño de la columna '{columna}' en la tabla '{nombre_tabla}' ajustado a {nuevo_tamano}.")
    except Error as e:
        logging.error(f"Error al ajustar el tamaño de la columna '{columna}' en la tabla '{nombre_tabla}': {e}")
        conexion.rollback()

def verificar_y_ajustar_columnas(conexion, nombre_tabla, filas, column_lengths):
    """
    Verifica y ajusta las columnas que exceden su tamaño máximo permitido, aumentando su tamaño en +10.

    :param conexion: La conexión a la base de datos PostgreSQL.
    :param nombre_tabla: El nombre de la tabla en la base de datos.
    :param filas: Lista de filas de datos a verificar.
    :param column_lengths: Un diccionario con los nombres de las columnas y sus longitudes máximas.
    """
    for fila in filas:
        errores = verificar_longitudes_y_ajustar(fila, column_lengths)
        if errores:
            for error in errores:
                # Extraer el nombre de la columna y el tamaño actual desde el mensaje de error
                match = re.search(r"Error en columna '(.+?)' con valor '.+?' y longitud máxima (\d+)", error)
                if match:
                    columna = match.group(1)
                    longitud_maxima_str = match.group(2)
                    
                    # Validar que la longitud máxima es un número antes de convertirla a entero
                    if longitud_maxima_str.isdigit():
                        tamano_actual = int(longitud_maxima_str)
                        nuevo_tamano = tamano_actual + 50
                        # Ajustar el tamaño de la columna
                        ajustar_tamano_columna(conexion, nombre_tabla, columna, nuevo_tamano)
                    else:
                        logging.error(f"Error: la longitud máxima '{longitud_maxima_str}' no es un número válido.")
                else:
                    logging.error(f"No se pudo extraer correctamente el tamaño de la columna desde el mensaje de error: {error}")



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

def obtener_formula(dms_name, reporte_name, campo):
    """
    Esta función busca y retorna la fórmula correspondiente a un campo específico dentro de un reporte
    de un DMS en particular.

    :param dms_name: Nombre del DMS.
    :param reporte_name: Nombre del reporte dentro del DMS.
    :param campo: El nombre de la columna o campo para el cual se quiere obtener la fórmula.
    :return: La fórmula correspondiente al campo si existe, de lo contrario, None.
    """
    # Ruta al archivo JSON del DMS
    dms_path = os.path.join('CLIENTS', 'dms', f'{dms_name}.json')
    
    if not os.path.exists(dms_path):
        logging.error(f"No se encontró el archivo {dms_path} para el DMS {dms_name}.")
        return None

    try:
        with open(dms_path, 'r') as file:
            dms_data = json.load(file)
    except json.JSONDecodeError as e:
        logging.error(f"Error al leer el archivo JSON {dms_path}: {e}")
        return None
    
    # Acceder a las fórmulas en el JSON del DMS para el reporte y campo específicos
    formula = dms_data.get('columnas_esperadas', {}).get(reporte_name, {}).get('formulas', {}).get(campo, None)
    
    return formula

def aplicar_formulas(df, dms_name, reporte):
    """
    Aplica fórmulas a las columnas calculadas en el DataFrame.

    :param df: El DataFrame al que se aplicarán las fórmulas.
    :param dms_name: El nombre del DMS utilizado para obtener las fórmulas.
    :param reporte: El nombre del reporte que se está procesando.
    """
    for columna in df.columns:
        # Obtener la fórmula asociada a la columna
        formula = obtener_formula(dms_name, reporte, columna)
        if formula:
            try:
                # Verificar si la fórmula contiene 'LimpiaTexto' o 'LimpiaEmail' para aplicar la función correspondiente
                if 'LimpiaTexto' in formula:
                    df[columna] = df[columna].apply(LimpiaTexto)
                elif 'LimpiaEmail' in formula:
                    df[columna] = df[columna].apply(LimpiaEmail)
                else:
                    # Convertir las columnas que participan en la fórmula a un tipo numérico
                    columnas_en_formula = re.findall(r"df\['(.*?)'\]", formula)
                    for col in columnas_en_formula:
                        if df[col].dtype == 'object':
                            logging.info(f"Convirtiendo la columna '{col}' a numérico.")
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    # Evaluar cualquier otra fórmula que no sea de limpieza de texto
                    df[columna] = eval(formula)

                logging.info(f"Fórmula '{formula}' aplicada a la columna '{columna}' en el reporte '{reporte}'.")

            except Exception as e:
                logging.error(f"Error al aplicar la fórmula en la columna calculada '{columna}': {e}")
                logging.error(f"Fórmula: {formula}")
        else:
            logging.info(f"No hay fórmula para la columna '{columna}' en el reporte '{reporte}'.")



def convertir_fechas_df(df, dms_name, reporte_name):
    """
    Esta función busca las columnas de tipo DATE en el DataFrame y las convierte al formato 'yyyy-mm-dd'.
    """
    # Obtener las columnas que son de tipo DATE en el DMS
    dms_path = os.path.join('CLIENTS', 'dms', f'{dms_name}.json')
    
    if not os.path.exists(dms_path):
        logging.error(f"No se encontró el archivo {dms_path} para el DMS {dms_name}.")
        return df

    with open(dms_path, 'r') as file:
        dms_data = json.load(file)
    
    # Acceder al tipo de dato de la columna en el JSON del DMS
    columnas_tipo_date = [
        col for col, info in dms_data.get('columnas_esperadas', {}).get(reporte_name, {}).get('data_types', {}).items()
        if info.get('tipo') == 'DATE'
    ]
    
    # Convertir las columnas de tipo DATE al formato 'yyyy-mm-dd'
    for col in columnas_tipo_date:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
            except Exception as e:
                logging.error(f"Error al convertir la columna {col} a formato de fecha: {e}")
    
    return df


def obtener_dms_por_reporte(reporte, config_data):
    """
    Función para obtener el DMS basado en un reporte específico.

    :param reporte: El reporte que se está buscando (ejemplo: 'REFSER01').
    :param config_data: El diccionario que contiene la información del JSON de configuración.
    :return: El DMS correspondiente si se encuentra, de lo contrario None.
    """
    # Recorremos los registros dentro de la configuración
    for registro in config_data.get('registros', []):
        dms_data = registro.get('dms', {})
        # Recorremos las claves y listas de reportes dentro de dms
        for dms, reportes_lista in dms_data.items():
            # Verificamos si el reporte está en la lista correspondiente
            if reporte in reportes_lista:
                return dms  # Retornamos el dms correspondiente si encontramos el reporte
    return None


procesar_archivo_zip()


