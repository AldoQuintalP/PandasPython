from flask import Flask, render_template, request, jsonify
import json
import os
import uuid  # Importar la biblioteca para generar UUIDs

app = Flask(__name__)

# Ruta para cargar la configuración
@app.route('/')
def index():
    with open('config.json', 'r') as f:
        config = json.load(f)
    return render_template('index.html', config=config)

# Ruta para guardar la configuración principal
@app.route('/', methods=['POST'])
def save_config():
    try:
        data = request.form
        # Cargar configuración existente
        with open('config.json', 'r') as f:
            config = json.load(f)

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
                columnas_esperadas[reporte] = columnas
        
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
        with open('config.json', 'r') as f:
            config = json.load(f)

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

        with open('config.json', 'r') as f:
            config = json.load(f)

        if nombre in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte ya existe.'})

        config['columnas_esperadas'][nombre] = columnas

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

        with open('config.json', 'r') as f:
            config = json.load(f)

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
    
def obtener_columnas(reporte):
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        
        # Obtener las columnas para el reporte específico
        columnas = config.get('columnas_esperadas', {}).get(reporte, [])
        return columnas
    except Exception as e:
        print(f"Error al obtener las columnas: {e}")
        return []

@app.route('/edit_reporte/<reporte>', methods=['GET'])
def edit_reporte(reporte):
    # Lógica para obtener datos del reporte y renderizar la plantilla
    columnas = obtener_columnas(reporte)  # Implementa esta función según tus necesidades
    return render_template('edit_reporte.html', reporte_nombre=reporte, columnas=columnas)

@app.route('/save-order', methods=['POST'])
def save_order():
    try:
        # Obtener los datos enviados desde el frontend
        reporte = request.form.get('reporte')
        columnas_ordenadas = request.form.get('columnas')

        if not reporte or not columnas_ordenadas:
            return jsonify({'success': False, 'error': 'Datos incompletos.'})

        # Intentar decodificar el JSON
        try:
            columnas_ordenadas = json.loads(columnas_ordenadas)
        except json.JSONDecodeError:
            return jsonify({'success': False, 'error': 'El formato de columnas es inválido.'})

        # Leer el archivo de configuración
        with open('config.json', 'r') as f:
            config = json.load(f)

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe en la configuración.'})

        columnas_originales = config['columnas_esperadas'][reporte]

        # Verificar que todas las columnas ordenadas están en las columnas originales
        if set(columnas_ordenadas) != set(columnas_originales):
            return jsonify({'success': False, 'error': 'El nuevo orden contiene columnas no válidas.'})

        # Actualizar el orden de las columnas para el reporte específico
        config['columnas_esperadas'][reporte] = columnas_ordenadas

        # Guardar la configuración actualizada
        with open('config.json', 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        # Imprimir el error en la consola para depuración
        print(f"Error al guardar el orden de columnas: {e}")
        return jsonify({'success': False, 'error': str(e)})
    
@app.route('/clientes')
def clientes():
    try:
        # Ruta a la carpeta CLIENTS
        folder_path = os.path.join(os.getcwd(), 'CLIENTS')
        
        # Obtener la lista de archivos y directorios
        files = os.listdir(folder_path)
        
        return render_template('clientes.html', files=files)
    except Exception as e:
        print(f"Error al listar archivos en CLIENTS: {e}")
        return render_template('clientes.html', files=[], error="No se pudo cargar el contenido de la carpeta CLIENTS.")

@app.route('/guardar_cliente', methods=['POST'])
def guardar_cliente():
    try:
        # Recibir los datos enviados en el cuerpo de la petición
        data = request.get_json()
        client_name = data.get('client_name')

        if not client_name:
            return jsonify({'success': False, 'error': 'El nombre del cliente es necesario.'})

        registros = data.get('registros', [])

        # Crear la ruta de la carpeta del cliente
        client_folder = os.path.join(os.getcwd(), 'CLIENTS', client_name, 'Config')
        
        # Crear la carpeta si no existe
        if not os.path.exists(client_folder):
            os.makedirs(client_folder)

        # Ruta al archivo de configuración del cliente
        client_config_path = os.path.join(client_folder, 'config.json')

        # Leer la configuración existente
        if os.path.exists(client_config_path):
            with open(client_config_path, 'r') as f:
                existing_config = json.load(f)
                existing_branches = {reg['branch'] for reg in existing_config.get('registros', [])}
        else:
            existing_config = {'registros': []}
            existing_branches = set()

        # Filtrar los registros para agregar solo los nuevos
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
        # Ruta al archivo de configuración del cliente
        client_config_path = os.path.join(os.getcwd(), 'CLIENTS', client_name, 'Config', 'config.json')
        
        if os.path.exists(client_config_path):
            # Leer la configuración existente
            with open(client_config_path, 'r') as f:
                client_config = json.load(f)
        else:
            client_config = {}

        return jsonify({'success': True, 'data': client_config})

    except Exception as e:
        print(f"Error al cargar los datos del cliente: {e}")
        return jsonify({'success': False, 'error': str(e)})
    
# Elimina registros
@app.route('/eliminar_registro', methods=['POST'])
def eliminar_registro():
    try:
        data = request.get_json()
        client_name = data.get('client_name')
        branch_to_delete = data.get('branch')

        if not client_name or not branch_to_delete:
            return jsonify({'success': False, 'error': 'El nombre del cliente y el branch son necesarios.'})

        # Ruta al archivo de configuración del cliente
        client_folder = os.path.join(os.getcwd(), 'CLIENTS', client_name, 'Config')
        client_config_path = os.path.join(client_folder, 'config.json')

        # Leer la configuración existente
        if os.path.exists(client_config_path):
            with open(client_config_path, 'r') as f:
                existing_config = json.load(f)

            # Filtrar los registros para eliminar el branch específico
            registros_actualizados = [reg for reg in existing_config.get('registros', []) if reg['branch'] != branch_to_delete]

            # Guardar la configuración actualizada en el archivo config.json
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

        # Verificar si el registro tiene un id, si no, generarlo
        registro_id = registro_actualizado.get('id')
        if not registro_id:
            registro_id = str(uuid.uuid4())
            registro_actualizado['id'] = registro_id

        print(f"Registro recibido: {registro_actualizado}")
        print(f"Cliente: {client_name}")

        # Leer el archivo de configuración del cliente específico
        client_config_path = os.path.join(os.getcwd(), 'CLIENTS', client_name, 'Config', 'config.json')

        if os.path.exists(client_config_path):
            with open(client_config_path, 'r') as f:
                client_config = json.load(f)

            # Validar que no haya otro registro con el mismo branch
            for registro in client_config.get('registros', []):
                if registro['branch'] == registro_actualizado['branch'] and registro['id'] != registro_id:
                    return jsonify({'success': False, 'error': 'Branch ya existe en otro registro.'})

            # Buscar el registro por ID para actualizarlo o añadirlo si no existe
            registro_encontrado = None
            for i, registro in enumerate(client_config.get('registros', [])):
                if registro.get('id') == registro_id:
                    registro_encontrado = i
                    break

            if registro_encontrado is not None:
                # Actualizar el registro encontrado
                client_config['registros'][registro_encontrado] = registro_actualizado
            else:
                # Añadir el nuevo registro si no se encontró
                client_config['registros'].append(registro_actualizado)

            # Guardar la configuración actualizada
            with open(client_config_path, 'w') as f:
                json.dump(client_config, f, indent=4)

            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Archivo de configuración no encontrado'})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'success': False, 'error': 'No se pudo actualizar el registro.'})

if __name__ == '__main__':
    app.run(debug=True)
