from flask import Flask, render_template, request, jsonify
import json

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

if __name__ == '__main__':
    app.run(debug=True)
