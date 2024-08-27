import json
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def cargar_reportes(cliente_numero):
    ruta_config = os.path.join('CLIENTS', cliente_numero, 'Config', 'config.json')
    
    with open(ruta_config, 'r') as file:
        config_data = json.load(file)
    
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

def procesar_archivo_zip():
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
    
    for file_name in os.listdir(workng_dir):
        if file_name.endswith('.zip'):
            nombre_zip = file_name
            break
    else:
        print("No se encontraron archivos .zip en el directorio.")
        return
    
    cliente_numero = nombre_zip[0:4].lstrip('0')
    reportes = cargar_reportes(cliente_numero)
    
    columnas_esperadas_dict = {}
    
    ruta_config = os.path.join('CLIENTS', cliente_numero, 'Config', 'config.json')
    with open(ruta_config, 'r') as file:
        config_data = json.load(file)

    for registro in config_data['registros']:
        dms_data = registro.get('dms', {})
        for dms_name, reportes_lista in dms_data.items():
            for reporte_name in reportes_lista:
                columnas = obtener_columnas_esperadas(dms_name, reporte_name)
                columnas_esperadas_dict[reporte_name] = columnas

    print(f'Reportes : {reportes}')
    print(f"Configuración de la base de datos: {db_config}")
    print(f"Columnas esperadas para el cliente {cliente_numero}: {columnas_esperadas_dict}")

# Ejemplo de uso
procesar_archivo_zip()
