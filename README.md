Proyecto de Procesamiento de Datos
Este proyecto procesa archivos ZIP que contienen datos en formato TXT, renombra los archivos, y los carga en una base de datos PostgreSQL. El proyecto utiliza un archivo de configuración para ajustar las rutas y los parámetros de la base de datos.

Requisitos
Python 3.x
PostgreSQL
Configuración del Entorno

Crea un entorno virtual para aislar las dependencias del proyecto:
python -m venv venv

Activar el Entorno Virtual
En Windows:
venv\Scripts\activate

Instalar Dependencias
Instala las dependencias del proyecto usando el archivo requirements.txt:
pip install -r requirements.txt

Configurar el Archivo config.json

Notas
Asegúrate de tener permisos adecuados para acceder y modificar los directorios y archivos especificados en config.json.
La configuración de la base de datos debe coincidir con los detalles de tu instalación de PostgreSQL.

Contribuciones
Las contribuciones son bienvenidas. Si encuentras algún problema o tienes sugerencias para mejorar el proyecto, por favor abre un issue o envía un pull request.