import os
import json
import logging

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

# Ejemplo de uso
dms_name = 'BProm'
reporte_name = 'MACC01'
campo = 'Descripcion'

formula = obtener_formula(dms_name, reporte_name, campo)
if formula:
    print(f"La fórmula para el campo '{campo}' en el reporte '{reporte_name}' es: {formula}")
else:
    print(f"No se encontró fórmula para el campo '{campo}' en el reporte '{reporte_name}'.")
