from unidecode import unidecode
import unicodedata
from pyspark.sql import functions as F
import pandas as pd
import re
from datetime import datetime
import numpy as np


def LimpiaTexto(texto):
    """
    Limpia el texto dado eliminando caracteres especiales, reemplazando letras con acentos,
    y quitando comas u otros caracteres innecesarios.
    Funciona tanto con cadenas individuales como con pandas Series.

    :param texto: El texto o pandas Series a limpiar.
    :return: El texto limpio o pandas Series con texto limpio.
    """
    if isinstance(texto, pd.Series):
        return texto.apply(LimpiaTexto)  # Aplicar la función recursivamente a cada elemento de la Series

    if texto is not None and isinstance(texto, str):
        # Convertir a mayúsculas
        temp = texto.upper()

        # Eliminar caracteres especiales antes y después del texto
        temp = temp.strip("""-+*/._:,;{}[]&lt;&gt;^`¨~´¡!¿?\'()=&amp;%$#º°ª¬@¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
        
        # Reemplazar caracteres no deseados por espacios o eliminar completamente si son irrelevantes
        temp = re.sub(r'[^A-Z0-9 ]', '', temp)  # Solo mantener letras, números y espacios

        # Eliminar acentos de letras Ej: Á -> A, Ñ -> N, etc.
        final = unidecode(temp)
        
        # Eliminar dobles espacios en caso de que se generen
        final = re.sub(r'\s+', ' ', final).strip()

    else:
        final = str(texto) if texto is not None else ""  # Convertir a string si no es None

    return final

def FormulaMargen(ventas, costos):
    # Condición 1: Si ventas es igual a 0 o costos es igual a 0
    if ventas == 0 or costos == 0:
        return 0.0
    # Condición 2: Si ventas es menor que 0
    elif ventas < 0:
        return ((ventas / (costos)- 1) * -100)
    # Condición 3: Si ninguna de las anteriores es cierta
    else:
        return ((ventas / (costos)- 1) * 100)
    
def FormulaOrdenes(ventas):
    # Si Ventas es igual a 0
    if ventas == 0:
        return 0
    # Si Ventas es mayor que 0
    elif ventas > 0:
        return 1
    # Si Ventas es menor que 0
    else:
        return -1

def FormulaUtilidad(ventas, costos):
    # Si Costos o Ventas es igual a 0
    if costos == 0 or ventas == 0:
        return 0
    # Si no, devolver la diferencia entre Ventas y Costos
    else:
        return ventas - costos
    

def Ctod(fecha_str, orden):
    """
    Convierte una cadena de fecha a un objeto datetime según el formato dado en 'orden'.
    
    Args:
    fecha_str (str): Fecha en formato de cadena (ejemplo: '25/12/2023')
    orden (str): El formato en el que está la fecha (ejemplo: 'd/m/y', 'm/d/y', etc.)
    
    Returns:
    datetime: Objeto datetime representando la fecha
    """
    
    # Reemplazar las letras 'd', 'm', 'y' por los códigos de formato de strptime
    formato = orden.replace('d', '%d').replace('m', '%m').replace('y', '%Y')

    try:
        # Convertir la cadena de fecha al objeto datetime usando el formato
        return datetime.strptime(fecha_str, formato)
    except ValueError as e:
        # Si hay un error al convertir, devolver un error detallado
        raise ValueError(f"Error al convertir la fecha '{fecha_str}' con el formato '{orden}': {e}")

    

def LimpiaCodigos(texto):
    #Se Eliminan caracteres especiales antes y despues del texto
    temp = texto.upper()
    temp = temp.strip("""-+*/._:,;{}[]&lt;&gt;^`¨~´¡!¿?\'()=&amp;%$#º°ª¬@¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
    #Se reemplazan caracteres especiales por letras
    temp_rep = temp.upper().replace("Ã‘","N").replace("Ã‰","E").replace("Ã“","O").replace("Ãº","U").replace("Ã­","I").replace("Ã±","N")
    #Se eliminan acentos de letras Ej: Ã -> A
    final = unidecode(temp_rep)
    return final

# def LimpiaCodigosv1(texto):
#     # Convierte el texto a mayúsculas
#     temp = texto.upper()
#     # Elimina caracteres especiales antes y después del texto
#     temp = temp.strip("""-+*/._:,;{}[]<>^`¨~´¡!¿?\'()=&%$#º°ª¬@¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
#     # Reemplaza caracteres especiales por letras
#     temp_rep = temp.replace("Ã‘","N").replace("Ã‰","E").replace("Ã“","O").replace("Ãº","U").replace("Ã­","I").replace("Ã±","N")
#     # Elimina acentos de letras
#     final = unidecode(temp_rep)
#     return final

# def LimpiaTextov1(texto):
#     if texto is not None:
#         # Convertir a mayúsculas y quitar acentos
#         temp = unidecode(texto.upper())
#         # Eliminar caracteres especiales antes y después del texto
#         temp = temp.strip("""-+*/._:,;{}[]<>&^`¨~´¡!¿?'()=&%$#º°ª¬@¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
#         # Reemplazar caracteres especiales por letras
#         temp_rep = (
#             temp.replace("Ã‘","N")
#                 .replace("Ã‰","E")
#                 .replace("Ã“","O")
#                 .replace("Ãº","U")
#                 .replace("Ã­","I")
#                 .replace("Ã±","N")
#                 .replace("‰","A")
#                 .replace("¢","O")
#                 .replace("Ã³","O")
#                 .replace("/"," ")
#                 .replace("-"," ")
#                 .replace("_"," ")
#                 .replace(".","")
#                 .replace(",","")
#         )
#         # Eliminar acentos de letras
#         final = unidecode(temp_rep)
#     else:
#         final = ""

#     return final

# def LimpiaTextov2(texto):
#     if texto is not None:
#         # Convertir a mayúsculas y normalizar Unicode
#         temp = unicodedata.normalize('NFKD', texto.upper())
#         # Eliminar caracteres especiales antes y después del texto
#         temp = temp.strip("""-+*/._:,;{}[]<>&^`¨~´¡!¿?'()=&%$#º°ª¬@¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
#         # Reemplazar caracteres especiales por letras
#         temp_rep = (
#             temp.replace("Ñ","N")
#                 .replace("É","E")
#                 .replace("Ó","O")
#                 .replace("Ú","U")
#                 .replace("Í","I")
#                 .replace("Ñ","N")
#                 .replace("‰","A")
#                 .replace("¢","O")
#                 .replace("Ó","O")  # Tratar específicamente el carácter 'ó'
#                 .replace("/"," ")
#                 .replace("-"," ")
#                 .replace("_"," ")
#                 .replace(".","")
#                 .replace(",","")
#         )
#         # Eliminar acentos de letras
#         final = ''.join(char for char in temp_rep if unicodedata.category(char) != 'Mn')
#     else:
#         final = ""

#     return final

def ventasNetas(venta, costo):
    """
    Aplica las reglas de negocio para la columna VentasNetas sobre columnas de pandas.
    
    Args:
    venta (pd.Series): Columna de ventas.
    costo (pd.Series): Columna de costos.
    
    Returns:
    pd.Series: Resultado de la fórmula aplicada.
    """
    return np.where(
        (venta == 0) | (costo == 0),  # Si venta o costo es 0
        0,
        np.where(
            (venta < 0) | (costo < 0),  # Si venta o costo es menor que 0
            -1,
            1  # Si no cumple las condiciones anteriores, retorna 1
        )
    )


def LimpiaEmail(texto):
    # Convertir a mayúsculas y aplicar unidecode para eliminar acentos
    temp = unidecode(texto.upper())
    
    # Eliminar caracteres especiales antes y después del texto
    temp = temp.strip("""-+*/._:,;{}[]&lt;&gt;^`¨~´¡!¿?'()=&amp;%$#º°ª¬¢©®«»±£¤¥§¯µ¶·¸ÆæÇçØß÷Œ×ƒ½¼¾ðÐ¦þ\t¹²³“‘" """)
    
    # Reemplazar caracteres especiales por letras o espacios
    temp = temp.replace("N~", "N").replace("E~", "E").replace("O~", "O").replace("U~", "U")\
               .replace("I~", "I").replace("/", " ").replace("-", " ").replace("_", " ")

    # Eliminar cualquier carácter adicional no alfanumérico permitido en correos electrónicos
    final = re.sub(r'[^A-Z0-9@._-]', '', temp)
    
    return final


def ttelefono(columna_telefono):
    return F.when(F.length(columna_telefono) < 2, "").otherwise(
        F.translate(
            F.upper(
                F.when(F.instr(columna_telefono, ",") != 0, F.split(columna_telefono, ",", 2).getItem(1))
                .otherwise(
                    F.when(F.instr(columna_telefono, ";") != 0, F.split(columna_telefono, ";", 2).getItem(1))
                    .otherwise(
                        F.when(F.instr(columna_telefono, "/") != 0, F.split(columna_telefono, "/", 2).getItem(1))
                        .otherwise(
                            F.when(F.instr(columna_telefono, "Y") != 0, F.split(columna_telefono, "Y", 2).getItem(1))
                            .otherwise(
                                F.when(F.instr(columna_telefono, "EXT") != 0, F.split(columna_telefono, "EXT", 2).getItem(1))
                                .otherwise(columna_telefono)
                            )
                        )
                    )
                )
            ),
            "ABCDEFGHIJKLMNÑOPQRSTUVWXYZ",
            ""
        ))

def tcelular(columna_celular):
    return F.when(F.length(columna_celular) < 2, "").otherwise(
        F.translate(
            F.upper(
                F.when(F.instr(columna_celular, ",") != 0, F.split(columna_celular, ",", 2).getItem(1))
                .otherwise(
                    F.when(F.instr(columna_celular, ";") != 0, F.split(columna_celular, ";", 2).getItem(1))
                    .otherwise(
                        F.when(F.instr(columna_celular, "/") != 0, F.split(columna_celular, "/", 2).getItem(1))
                        .otherwise(
                            F.when(F.instr(columna_celular, "Y") != 0, F.split(columna_celular, "Y", 2).getItem(1))
                            .otherwise(
                                F.when(F.instr(columna_celular, "EXT") != 0, F.split(columna_celular, "EXT", 2).getItem(1))
                                .otherwise(columna_celular)
                            )
                        )
                    )
                )
            ),
            "ABCDEFGHIJKLMNÑOPQRSTUVWXYZ",
            ""
        ))