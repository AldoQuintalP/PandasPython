from flask import Flask, render_template, request, jsonify
import json
import os
import uuid  # Importar la biblioteca para generar UUIDs

app = Flask(__name__)

def cargar_config():
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    # Asegurarse de que cada reporte tenga la estructura adecuada
    for reporte, data in config.get('columnas_esperadas', {}).items():
        if isinstance(data, list):
            config['columnas_esperadas'][reporte] = {
                'columnas': data,
                'formulas': {}
            }
    return config


# Ruta para cargar la configuración
@app.route('/')
def index():
    config = cargar_config()
    return render_template('index.html', config=config)

# Ruta para guardar la configuración principal
@app.route('/', methods=['POST'])
def save_config():
    try:
        data = request.form
        config = cargar_config()

        # Actualizar configuraciones principales
        config['workng_dir'] = data.get('workng_dir', '')
        config['sandbx'] = data.get('sandbx', '')
        config['reportes'] = [rep.strip() for rep in data.get('reportes', '').split(',') if rep.strip()]

        # Validar y actualizar columnas esperadas
        columnas_esperadas = {}
        for key in data.keys():
            if key.startswith('columnas_'):
                reporte = key.split('_', 1)[1]
                columnas = [col.strip() for col in data.get(key, '').split(',') if col.strip()]
                if reporte in columnas_esperadas:
                    return jsonify({'success': False, 'error': f'El reporte "{reporte}" ya está agregado.'})
                columnas_esperadas[reporte] = {
                    'columnas': columnas,
                    'formulas': {}
                }
        
        config['columnas_esperadas'] = columnas_esperadas

        # Guardar configuración
        with open('config.json', 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'success': False, 'error': 'No se pudo guardar la configuración.'})

# Ruta para guardar la configuración de la base de datos
@app.route('/save-db-config', methods=['POST'])
def save_db_config():
    try:
        data = request.form
        config = cargar_config()

        config['db'] = {
            'host': data.get('db_host', ''),
            'usuario': data.get('db_usuario', ''),
            'contrasena': data.get('db_contrasena', ''),
            'base_de_datos': data.get('db_base_datos', '')
        }

        with open('config.json', 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'success': False, 'error': 'No se pudo guardar la configuración de la base de datos.'})

# Ruta para agregar un nuevo reporte
@app.route('/add-reporte', methods=['POST'])
def add_reporte():
    try:
        nombre = request.form.get('nombre')
        columnas = request.form.get('columnas', '').split(',')
        columnas = [col.strip() for col in columnas if col.strip()]

        if not nombre:
            return jsonify({'success': False, 'error': 'El nombre del reporte no puede estar vacío.'})

        config = cargar_config()

        if nombre in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte ya existe.'})

        config['columnas_esperadas'][nombre] = {
            'columnas': columnas,
            'formulas': {}
        }

        # Añadir el nombre del reporte a la lista de reportes
        if nombre not in config['reportes']:
            config['reportes'].append(nombre)

        with open('config.json', 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'success': False, 'error': 'No se pudo agregar el reporte.'})

# Ruta para eliminar un reporte
@app.route('/delete-reporte', methods=['POST'])
def delete_reporte():
    try:
        reporte = request.form.get('reporte')

        if not reporte:
            return jsonify({'success': False, 'error': 'El reporte no puede estar vacío.'})

        config = cargar_config()

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

        # Eliminar el reporte de columnas_esperadas
        del config['columnas_esperadas'][reporte]

        # Eliminar el nombre del reporte de la lista de reportes
        if reporte in config['reportes']:
            config['reportes'].remove(reporte)

        with open('config.json', 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'success': False, 'error': 'No se pudo eliminar el reporte.'})


@app.route('/edit_reporte/<reporte>', methods=['GET'])
def edit_reporte(reporte):
    config = cargar_config()
    columnas_info = config['columnas_esperadas'].get(reporte, {'columnas': [], 'formulas': {}})
    
    # Asegurarse de que columnas_info sea un diccionario
    if isinstance(columnas_info, list):
        columnas_info = {'columnas': columnas_info, 'formulas': {}}

    columnas = columnas_info.get('columnas', [])
    formulas = columnas_info.get('formulas', {})

    return jsonify({'success': True, 'columnas': columnas, 'formulas': formulas})

@app.route('/save-order', methods=['POST'])
def save_order():
    try:
        reporte = request.form.get('reporte')
        columnas_ordenadas = request.form.get('columnas')

        if not reporte or not columnas_ordenadas:
            return jsonify({'success': False, 'error': 'Datos incompletos.'})

        try:
            columnas_ordenadas = json.loads(columnas_ordenadas)
        except json.JSONDecodeError:
            return jsonify({'success': False, 'error': 'El formato de columnas es inválido.'})

        config = cargar_config()

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe en la configuración.'})

        # Asegurarse de que la entrada es un diccionario con 'columnas' y 'formulas'
        if isinstance(config['columnas_esperadas'][reporte], list):
            config['columnas_esperadas'][reporte] = {
                'columnas': config['columnas_esperadas'][reporte],
                'formulas': {}
            }

        config['columnas_esperadas'][reporte]['columnas'] = columnas_ordenadas

        with open('config.json', 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error al guardar el orden de columnas: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/clientes')
def clientes():
    try:
        folder_path = os.path.join(os.getcwd(), 'CLIENTS')
        files = os.listdir(folder_path)
        return render_template('clientes.html', files=files)
    except Exception as e:
        print(f"Error al listar archivos en CLIENTS: {e}")
        return render_template('clientes.html', files=[], error="No se pudo cargar el contenido de la carpeta CLIENTS.")

@app.route('/guardar_cliente', methods=['POST'])
def guardar_cliente():
    try:
        data = request.get_json()
        client_name = data.get('client_name')

        if not client_name:
            return jsonify({'success': False, 'error': 'El nombre del cliente es necesario.'})

        registros = data.get('registros', [])

        client_folder = os.path.join(os.getcwd(), 'CLIENTS', client_name, 'Config')
        if not os.path.exists(client_folder):
            os.makedirs(client_folder)

        client_config_path = os.path.join(client_folder, 'config.json')

        if os.path.exists(client_config_path):
            with open(client_config_path, 'r') as f:
                existing_config = json.load(f)
                existing_branches = {reg['branch'] for reg in existing_config.get('registros', [])}
        else:
            existing_config = {'registros': []}
            existing_branches = set()

        nuevos_registros = [registro for registro in registros if registro['branch'] not in existing_branches]

        # Verificar si hay nuevos registros para agregar
        if not nuevos_registros:
            return jsonify({'success': False, 'error': 'No hay nuevos registros para agregar.'})

        # Agregar los nuevos registros a los existentes
        existing_config['registros'].extend(nuevos_registros)

        # Guardar la configuración actualizada en el archivo config.json
        with open(client_config_path, 'w') as f:
            json.dump(existing_config, f, indent=4)

        return jsonify({'success': True, 'added_records': nuevos_registros})

    except Exception as e:
        print(f"Error al guardar los datos del cliente: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/cliente_detalles/<client_name>', methods=['GET'])
def cliente_detalles(client_name):
    try:
        client_config_path = os.path.join(os.getcwd(), 'CLIENTS', client_name, 'Config', 'config.json')
        
        if os.path.exists(client_config_path):
            with open(client_config_path, 'r') as f:
                client_config = json.load(f)
        else:
            client_config = {}

        return jsonify({'success': True, 'data': client_config})

    except Exception as e:
        print(f"Error al cargar los datos del cliente: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/eliminar_registro', methods=['POST'])
def eliminar_registro():
    try:
        data = request.get_json()
        client_name = data.get('client_name')
        branch_to_delete = data.get('branch')

        if not client_name or not branch_to_delete:
            return jsonify({'success': False, 'error': 'El nombre del cliente y el branch son necesarios.'})

        client_folder = os.path.join(os.getcwd(), 'CLIENTS', client_name, 'Config')
        client_config_path = os.path.join(client_folder, 'config.json')

        if os.path.exists(client_config_path):
            with open(client_config_path, 'r') as f:
                existing_config = json.load(f)

            registros_actualizados = [reg for reg in existing_config.get('registros', []) if reg['branch'] != branch_to_delete]

            with open(client_config_path, 'w') as f:
                json.dump({'registros': registros_actualizados}, f, indent=4)

            return jsonify({'success': True})

        else:
            return jsonify({'success': False, 'error': 'El archivo de configuración no existe.'})

    except Exception as e:
        print(f"Error al eliminar el registro del cliente: {e}")
        return jsonify({'success': False, 'error': 'Error al eliminar el registro.'})

@app.route('/actualizar_registro', methods=['POST'])
def actualizar_registro():
    try:
        data = request.json
        registro_actualizado = data['registro']
        client_name = data['client_name']

        registro_id = registro_actualizado.get('id')
        if not registro_id:
            registro_id = str(uuid.uuid4())
            registro_actualizado['id'] = registro_id

        client_config_path = os.path.join(os.getcwd(), 'CLIENTS', client_name, 'Config', 'config.json')

        if os.path.exists(client_config_path):
            with open(client_config_path, 'r') as f:
                client_config = json.load(f)

            for registro in client_config.get('registros', []):
                if registro['branch'] == registro_actualizado['branch'] and registro['id'] != registro_id:
                    return jsonify({'success': False, 'error': 'Branch ya existe en otro registro.'})

            registro_encontrado = None
            for i, registro in enumerate(client_config.get('registros', [])):
                if registro.get('id') == registro_id:
                    registro_encontrado = i
                    break

            if registro_encontrado is not None:
                client_config['registros'][registro_encontrado] = registro_actualizado
            else:
                client_config['registros'].append(registro_actualizado)

            with open(client_config_path, 'w') as f:
                json.dump(client_config, f, indent=4)

            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Archivo de configuración no encontrado'})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'success': False, 'error': 'No se pudo actualizar el registro.'})

# Editar Formula de las columnas
@app.route('/save-formula', methods=['POST'])
def save_formula():
    try:
        reporte = request.form.get('reporte')
        columna = request.form.get('columna')
        formula = request.form.get('formula')

        if not reporte or not columna:
            return jsonify({'success': False, 'error': 'Reporte y columna son necesarios.'})

        config = cargar_config()

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

        # Asegurarse de que el reporte esté en formato dict
        if isinstance(config['columnas_esperadas'][reporte], list):
            config['columnas_esperadas'][reporte] = {
                'columnas': config['columnas_esperadas'][reporte],
                'formulas': {}
            }

        config['columnas_esperadas'][reporte]['formulas'][columna] = formula

        with open('config.json', 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error al guardar la fórmula: {e}")
        return jsonify({'success': False, 'error': 'No se pudo guardar la fórmula.'})
    
@app.route('/delete-formula', methods=['POST'])
def delete_formula():
    try:
        reporte = request.form.get('reporte')
        columna = request.form.get('columna')

        if not reporte or not columna:
            return jsonify({'success': False, 'error': 'Reporte y columna son necesarios.'})

        config = cargar_config()

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

        # Asegurarse de que el reporte esté en formato dict
        if isinstance(config['columnas_esperadas'][reporte], list):
            return jsonify({'success': False, 'error': 'El formato del reporte no es válido.'})

        if columna in config['columnas_esperadas'][reporte]['formulas']:
            del config['columnas_esperadas'][reporte]['formulas'][columna]

            with open('config.json', 'w') as f:
                json.dump(config, f, indent=4)

            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'La fórmula no existe.'})

    except Exception as e:
        print(f"Error al eliminar la fórmula: {e}")
        return jsonify({'success': False, 'error': 'No se pudo eliminar la fórmula.'})


if __name__ == '__main__':
    app.run(debug=True)
