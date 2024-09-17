from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import json
import os
import uuid
from ext import db  # Importar db desde ext.py
from forms import RegistrationForm, LoginForm
from models import User  # Import the User model after db is initialized
import subprocess
import ast
import logging
import psycopg2
import re
from datetime import datetime
import time



# Configura Flask con la ruta para archivos estáticos
app = Flask(__name__, static_url_path='/static')
#logging.basicConfig(filename='debug.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')


def get_database_connection():
    db_config = cargar_db_config()
    if db_config is None:
        raise ValueError("No se pudo cargar la configuración de la base de datos.")
    
    return psycopg2.connect(
        host=db_config['host'],
        user=db_config['usuario'],
        password=db_config['contrasena'],
        database=db_config['base_de_datos']
    )

# Cargar la configuración desde un archivo JSON específico
def cargar_config(tab_name):
    config_path = os.path.join('CLIENTS', 'dms', f'{tab_name}.json')
    
    # Si el archivo no existe, retornar un error claro
    if not os.path.exists(config_path):
        print(f"El archivo {config_path} no existe.")  # Para fines de depuración
        return None
    
    # Si el archivo existe, cargar su contenido
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    return config

# Cargar la configuración de la base de datos desde database.json
def cargar_db_config():
    db_config_path = 'database.json'
    
    # Verificar si el archivo database.json existe
    if not os.path.exists(db_config_path):
        # Crear una estructura básica si el archivo no existe
        db_config = {
            "host": "",
            "usuario": "",
            "contrasena": "",
            "base_de_datos": ""
        }
        with open(db_config_path, 'w') as f:
            json.dump(db_config, f, indent=4)
    else:
        # Si el archivo existe, cargar su contenido
        with open(db_config_path, 'r') as f:
            db_config = json.load(f)

    return db_config



# Cargar la configuración de la base de datos desde config.json
db_config = cargar_db_config()
app.config['SECRET_KEY'] = 'your_secret_key'
app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{db_config.get('usuario')}:{db_config.get('contrasena')}@{db_config.get('host')}/{db_config.get('base_de_datos')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Inicializar db con la aplicación
db.init_app(app)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

@app.route('/register', methods=['GET', 'POST'])
def register():
    form = RegistrationForm()
    
    if form.validate_on_submit():
        hashed_password = generate_password_hash(form.password.data, method='pbkdf2:sha256')
        
        new_user = User(username=form.username.data, email=form.email.data, password=hashed_password)
        
        try:
            db.session.add(new_user)
            db.session.commit()
            flash('Registro exitoso', 'success')  # Flash de éxito
            return render_template('register.html', form=form, success=True)
        except Exception as e:
            db.session.rollback()
            flash('Ocurrió un error durante el registro', 'danger')
    
    return render_template('register.html', form=form)

@app.route('/login', methods=['GET', 'POST'])
def login():
    print("Login route called")  # Debug statement
    
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        print(f"Email: {email}, Password: {password}")  # Debugging output
        
        user = User.query.filter_by(email=email).first()
        
        if user:
            print(f"User found: {user.email}")  # Debugging output
            if check_password_hash(user.password, password):
                login_user(user)
                print("Login successful")  # Debugging output
                flash('Inicio de sesión exitoso', category='success')
                return redirect(url_for('index'))
            else:
                print("Password incorrect")  # Debugging output
                flash('Correo o contraseña incorrectos', category='danger')
        else:
            print("User not found")  # Debugging output
            flash('Correo o contraseña incorrectos', category='danger')
        
        return redirect(url_for('login'))
    
    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Has cerrado sesión', category='info')
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    tabs = cargar_tabs()
    tab_data = {}

    dms_folder = os.path.join(os.getcwd(), 'CLIENTS', 'dms')

    for tab in tabs:
        tab_path = os.path.join(dms_folder, f"{tab}.json")
        if os.path.exists(tab_path):
            with open(tab_path, 'r') as f:
                tab_data[tab] = json.load(f)
        else:
            tab_data[tab] = {"reportes": []}

    return render_template('index.html', tabs=tabs, tab_data=tab_data)

@app.route('/', methods=['POST'])
@login_required
def save_config():
    try:
        data = request.form
        config = cargar_config()

        config['reportes'] = [rep.strip() for rep in data.get('reportes', '').split(',') if rep.strip()]

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

        with open('config.json', 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'success': False, 'error': 'No se pudo guardar la configuración.'})


@app.route('/save-db-config', methods=['POST'])
@login_required
def save_db_config():
    try:
        data = request.form

        # Crear la configuración a partir de los datos del formulario
        db_config = {
            'host': data.get('db_host', ''),
            'usuario': data.get('db_user', ''),
            'contrasena': data.get('db_contrasena', ''),
            'base_de_datos': data.get('db_base_datos', ''),
            'workng_dir': data.get('workng_dir', ''),
            'sandbx': data.get('sandbx', '')
        }

        # Ruta del archivo database.json
        db_config_path = 'database.json'

        # Verificar si el archivo existe, si no, crearlo
        if not os.path.exists(db_config_path):
            with open(db_config_path, 'w') as f:
                json.dump({}, f)  # Crear el archivo vacío si no existe

        # Guardar la configuración en el archivo database.json
        with open(db_config_path, 'w') as f:
            json.dump(db_config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'success': False, 'error': 'No se pudo guardar la configuración de la base de datos.'})


@app.route('/add-reporte', methods=['POST'])
@login_required
def add_reporte():
    try:
        tab_name = request.form.get('tab_name')
        nombre_reporte = request.form.get('nombre')
        columnas = request.form.get('columnas', '').split(',')
        columnas = [col.strip() for col in columnas if col.strip()]

        if not tab_name or not nombre_reporte:
            return jsonify({'success': False, 'error': 'El nombre del reporte y la pestaña son necesarios.'})

        # Ruta al archivo JSON de la pestaña correspondiente
        dms_folder = os.path.join(os.getcwd(), 'CLIENTS', 'dms')
        tab_path = os.path.join(dms_folder, f"{tab_name}.json")

        if not os.path.exists(tab_path):
            return jsonify({'success': False, 'error': 'La pestaña especificada no existe.'})

        # Cargar el contenido del archivo JSON
        with open(tab_path, 'r') as f:
            config = json.load(f)

        # Verificar si el reporte ya existe
        if nombre_reporte in config.get('reportes', []):
            return jsonify({'success': False, 'error': 'El reporte ya existe en esta pestaña.'})

        # Agregar el nuevo reporte al JSON
        config['reportes'].append(nombre_reporte)
        config['columnas_esperadas'][nombre_reporte] = {
            "columnas": columnas,
            "formulas": {},
            "data_types": {}  # Se agrega la nueva llave "data_types"
        }

        # Guardar el JSON actualizado
        with open(tab_path, 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error al guardar el reporte: {e}")
        return jsonify({'success': False, 'error': 'No se pudo guardar el reporte.'})


@app.route('/delete-reporte', methods=['POST'])
@login_required
def delete_reporte():
    try:
        tab_name = request.form.get('tab_name')
        reporte = request.form.get('reporte')

        if not tab_name or not reporte:
            return jsonify({'success': False, 'error': 'El nombre de la pestaña y el reporte son necesarios.'})

        # Cargar la configuración para la pestaña especificada
        config = cargar_config(tab_name)

        if reporte not in config['reportes']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

        # Eliminar el reporte de la configuración
        config['reportes'].remove(reporte)
        del config['columnas_esperadas'][reporte]

        # Guardar la configuración actualizada
        config_path = os.path.join('CLIENTS', 'dms', f'{tab_name}.json')
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error al eliminar el reporte: {e}")
        return jsonify({'success': False, 'error': 'No se pudo eliminar el reporte.'})


@app.route('/edit_reporte/<reporte>', methods=['GET'])
@login_required
def edit_reporte(reporte):
    tab_name = request.args.get('tab_name')
    dms_folder = os.path.join(os.getcwd(), 'CLIENTS', 'dms')
    tab_path = os.path.join(dms_folder, f"{tab_name}.json")

    if not os.path.exists(tab_path):
        return jsonify({'success': False, 'error': 'El archivo de la pestaña especificada no existe.'})

    with open(tab_path, 'r') as f:
        config = json.load(f)

    columnas_info = config['columnas_esperadas'].get(reporte, {'columnas': [], 'formulas': {}, 'data_types': {}})
    columnas = columnas_info.get('columnas', [])
    data_types = columnas_info.get('data_types', {})
    formulas = columnas_info.get('formulas', {})  # Asegurarse de obtener las fórmulas

    # Validar cuáles columnas no tienen un tipo de dato asignado
    columnas_sin_tipo_dato = [col for col in columnas if col not in data_types]

    return jsonify({
        'success': True,
        'columnas': columnas,
        'formulas': formulas,  # Asegurarse de que las fórmulas se devuelvan en la respuesta
        'data_types': data_types,
        'columnas_sin_tipo_dato': columnas_sin_tipo_dato
    })


@app.route('/save-order', methods=['POST'])
@login_required
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
@login_required
def clientes():
    try:
        folder_path = os.path.join(os.getcwd(), 'CLIENTS')
        files = os.listdir(folder_path)
        return render_template('clientes.html', files=files)
    except Exception as e:
        print(f"Error al listar archivos en CLIENTS: {e}")
        return render_template('clientes.html', files=[], error="No se pudo cargar el contenido de la carpeta CLIENTS.")

@app.route('/guardar_cliente', methods=['POST'])
@login_required
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

        # Si existe una configuración previa, cargarla
        if os.path.exists(client_config_path):
            with open(client_config_path, 'r') as f:
                existing_config = json.load(f)
        else:
            existing_config = {'registros': []}

        # Añadir los nuevos registros a la configuración existente
        existing_config['registros'].extend(registros)

        # Guardar la configuración actualizada
        with open(client_config_path, 'w') as f:
            json.dump(existing_config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error al guardar los datos del cliente: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/cliente_detalles/<client_name>', methods=['GET'])
@login_required
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
@login_required
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
@login_required
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

@app.route('/save-formula', methods=['POST'])
@login_required
def save_formula():
    try:
        # Obtener los datos del request como JSON
        data = request.get_json()
        tab_name = data.get('name')  # Obtener el nombre de la pestaña
        reporte = data.get('reporte')
        columna = data.get('columna')
        formula = data.get('formula')

        print(f"Tab Name: {tab_name}, Reporte: {reporte}, Columna: {columna}")

        if not tab_name or not reporte or not columna:
            return jsonify({'success': False, 'error': 'Pestaña, reporte y columna son necesarios.'})

        # Cargar la configuración para la pestaña especificada
        config = cargar_config(tab_name)

        if config is None:
            return jsonify({'success': False, 'error': 'No se pudo cargar la configuración para la pestaña especificada.'})

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

        # Actualizar o agregar la fórmula en la columna correspondiente
        config['columnas_esperadas'][reporte]['formulas'][columna] = formula

        # Guardar la configuración actualizada en el archivo correspondiente
        config_path = os.path.join('CLIENTS', 'dms', f'{tab_name}.json')
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error al guardar la fórmula: {e}")
        return jsonify({'success': False, 'error': 'No se pudo guardar la fórmula.'})

    
@app.route('/delete-formula', methods=['POST'])
def delete_formula():
    try:
        tab_name = request.form.get('name')  # Obtener el nombre de la pestaña
        reporte = request.form.get('reporte')
        columna = request.form.get('columna')

        if not tab_name or not reporte or not columna:
            return jsonify({'success': False, 'error': 'Pestaña, reporte y columna son necesarios.'})

        # Cargar la configuración para la pestaña especificada
        config = cargar_config(tab_name)

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

        # Asegurarse de que el reporte esté en formato dict
        if isinstance(config['columnas_esperadas'][reporte], list):
            return jsonify({'success': False, 'error': 'El formato del reporte no es válido.'})

        if columna in config['columnas_esperadas'][reporte]['formulas']:
            del config['columnas_esperadas'][reporte]['formulas'][columna]

            # Guardar la configuración actualizada
            config_path = os.path.join('CLIENTS', 'dms', f'{tab_name}.json')
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=4)

            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'La fórmula no existe.'})

    except Exception as e:
        print(f"Error al eliminar la fórmula: {e}")
        return jsonify({'success': False, 'error': 'No se pudo eliminar la fórmula.'})


    
@app.route('/database', methods=['GET'])
@login_required
def database():
    # Verificar si database.json existe y cargar su contenido
    db_config_path = 'database.json'
    if os.path.exists(db_config_path):
        with open(db_config_path, 'r') as f:
            db_config = json.load(f)
    else:
        db_config = {}

    return render_template('database.html', config=db_config)


# Ruta al archivo donde se guardan las pestañas
tabs_file = os.path.join(os.getcwd(), 'CLIENTS', 'dms', 'tabs.json')

def cargar_tabs():
    if os.path.exists(tabs_file):
        with open(tabs_file, 'r') as f:
            return json.load(f)
    return []

def guardar_tab(tab_name):
    tabs = cargar_tabs()
    if tab_name not in tabs:
        tabs.append(tab_name)
        with open(tabs_file, 'w') as f:
            json.dump(tabs, f, indent=4)

@app.route('/add_tab', methods=['POST'])
@login_required
def add_tab():
    try:
        tab_name = request.form.get('tab_name')
        if not tab_name:
            return jsonify({'success': False, 'error': 'El nombre de la pestaña es necesario.'})
        
        # Ruta al archivo tabs.json que almacena las pestañas
        tabs_file = os.path.join(os.getcwd(), 'CLIENTS', 'dms', 'tabs.json')
        
        # Crear la carpeta CLIENTS/dms si no existe
        dms_folder = os.path.join(os.getcwd(), 'CLIENTS', 'dms')
        if not os.path.exists(dms_folder):
            os.makedirs(dms_folder)
        
        # Cargar las pestañas existentes desde tabs.json
        if os.path.exists(tabs_file):
            with open(tabs_file, 'r') as f:
                tabs = json.load(f)
        else:
            tabs = []

        # Verificar si la pestaña ya existe
        if tab_name in tabs:
            return jsonify({'success': False, 'error': 'La pestaña ya existe.'})

        # Agregar la nueva pestaña a la lista y guardar en tabs.json
        tabs.append(tab_name)
        with open(tabs_file, 'w') as f:
            json.dump(tabs, f, indent=4)

        # Crear un archivo JSON con la estructura inicial para la nueva pestaña, incluyendo el nombre
        tab_path = os.path.join(dms_folder, f"{tab_name}.json")
        initial_data = {
            "name": tab_name,  # Agregar la llave 'name' con el nombre de la pestaña
            "reportes": [],
            "columnas_esperadas": {}
        }

        with open(tab_path, 'w') as f:
            json.dump(initial_data, f, indent=4)
        
        return jsonify({'success': True})
    
    except Exception as e:
        print(f"Error al crear la nueva pestaña: {e}")
        return jsonify({'success': False, 'error': 'No se pudo crear la nueva pestaña.'})

@app.route('/editar-columna', methods=['POST'])
@login_required
def editar_columna():
    try:
        tab_name = request.form.get('tab_name')
        reporte = request.form.get('reporte')
        columna = request.form.get('columna')
        nueva_columna = request.form.get('nueva_columna')

        if not tab_name or not reporte or not columna or not nueva_columna:
            return jsonify({'success': False, 'error': 'Pestaña, reporte, y columna son necesarios.'})

        config = cargar_config(tab_name)

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

        columnas = config['columnas_esperadas'][reporte]['columnas']
        if columna in columnas:
            columnas[columnas.index(columna)] = nueva_columna
            with open(f'CLIENTS/dms/{tab_name}.json', 'w') as f:
                json.dump(config, f, indent=4)
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'La columna no existe.'})

    except Exception as e:
        print(f"Error al editar la columna: {e}")
        return jsonify({'success': False, 'error': 'No se pudo editar la columna.'})

@app.route('/eliminar-columna', methods=['POST'])
@login_required
def eliminar_columna():
    try:
        tab_name = request.form.get('tab_name')
        reporte = request.form.get('reporte')
        columna = request.form.get('columna')

        if not tab_name or not reporte or not columna:
            return jsonify({'success': False, 'error': 'Pestaña, reporte y columna son necesarios.'})

        config = cargar_config(tab_name)

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

        columnas = config['columnas_esperadas'][reporte]['columnas']
        if columna in columnas:
            columnas.remove(columna)
            with open(f'CLIENTS/dms/{tab_name}.json', 'w') as f:
                json.dump(config, f, indent=4)
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'La columna no existe.'})

    except Exception as e:
        print(f"Error al eliminar la columna: {e}")
        return jsonify({'success': False, 'error': 'No se pudo eliminar la columna.'})
    
@app.route('/guardar-orden-columnas', methods=['POST'])
@login_required
def guardar_orden_columnas():
    try:
        # Parsear el cuerpo de la solicitud como JSON
        data = request.get_json()

        tab_name = data.get('name')  # Obtener el nombre de la pestaña (DMS)
        print(f'Tab_name: {tab_name}')
        reporte = data.get('reportes')[0]  # Obtener el primer (y único) reporte
        print(f'Reporte: {reporte}')
        columnas = data.get('columnas_esperadas', {}).get(reporte, {}).get('columnas', [])
        print(f'Columnas: {columnas}')

        # Validar que los datos clave estén presentes
        if not tab_name or not reporte or not columnas:
            return jsonify({'success': False, 'error': 'Faltan datos para guardar el orden de columnas.'})

        # Cargar la configuración actual del archivo JSON correspondiente
        config = cargar_config(tab_name)
        if config is None:
            return jsonify({'success': False, 'error': 'No se pudo cargar la configuración para la pestaña especificada.'})

        # Verificar que el reporte exista en la configuración
        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe en la configuración.'})

        # Actualizar las columnas en la configuración
        config['columnas_esperadas'][reporte]['columnas'] = columnas

        # Guardar la nueva configuración en el archivo JSON
        config_path = os.path.join('CLIENTS', 'dms', f'{tab_name}.json')
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})
    except Exception as e:
        print(f"Error al guardar el orden de columnas: {e}")
        return jsonify({'success': False, 'error': str(e)})

    
@app.route('/add-columna', methods=['POST'])
@login_required
def add_columna():
    try:
        tab_name = request.form.get('tab_name')
        reporte = request.form.get('reporte')
        nueva_columna = request.form.get('nueva_columna')

        if not tab_name or not reporte or not nueva_columna:
            return jsonify({'success': False, 'error': 'Pestaña, reporte, y columna son necesarios.'})

        config = cargar_config(tab_name)

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

        columnas = config['columnas_esperadas'][reporte]['columnas']
        if nueva_columna in columnas:
            return jsonify({'success': False, 'error': 'La columna ya existe.'})

        columnas.append(nueva_columna)
        with open(f'CLIENTS/dms/{tab_name}.json', 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error al agregar la columna: {e}")
        return jsonify({'success': False, 'error': 'No se pudo agregar la columna.'})

@app.route('/duplicar-reporte', methods=['POST'])
@login_required
def duplicar_reporte():
    try:
        tab_name = request.form.get('tab_name')
        reporte = request.form.get('reporte')
        nuevo_reporte = request.form.get('nuevo_reporte')
        dms_destino = request.form.get('dms_destino')

        if not tab_name or not reporte or not nuevo_reporte or not dms_destino:
            return jsonify({"success": False, "error": "Faltan parámetros"}), 400

        # Ruta del archivo JSON del DMS origen
        origen_json_file_path = f"CLIENTS/dms/{tab_name}.json"
        
        # Ruta del archivo JSON del DMS destino
        destino_json_file_path = f"CLIENTS/dms/{dms_destino}.json"

        # Verifica que ambos archivos JSON existan
        if not os.path.exists(origen_json_file_path):
            return jsonify({"success": False, "error": "Archivo de configuración de origen no encontrado"}), 404

        if not os.path.exists(destino_json_file_path):
            return jsonify({"success": False, "error": "Archivo de configuración de destino no encontrado"}), 404

        # Cargar la configuración del DMS origen
        with open(origen_json_file_path, 'r') as file:
            origen_config_data = json.load(file)

        # Verifica si el reporte original existe
        if reporte not in origen_config_data.get('reportes', []):
            return jsonify({"success": False, "error": "El reporte original no existe"}), 404

        # Cargar la configuración del DMS destino
        with open(destino_json_file_path, 'r') as file:
            destino_config_data = json.load(file)

        # Verifica si el nuevo reporte ya existe en el DMS destino
        if nuevo_reporte in destino_config_data.get('reportes', []):
            return jsonify({"success": False, "error": "El nuevo nombre de reporte ya existe en el DMS destino"}), 400

        # Duplicar el reporte en el DMS destino
        destino_config_data['reportes'].append(nuevo_reporte)
        destino_config_data['columnas_esperadas'][nuevo_reporte] = origen_config_data['columnas_esperadas'][reporte]

        # Guardar los cambios en el archivo JSON del DMS destino
        with open(destino_json_file_path, 'w') as file:
            json.dump(destino_config_data, file, indent=4)

        return jsonify({"success": True}), 200

    except Exception as e:
        # Imprimir el error en la consola para depuración
        print(f"Error al duplicar el reporte: {e}")
        return jsonify({"success": False, "error": f"Error interno: {str(e)}"}), 500

    
@app.route('/get_tabs_json', methods=['GET'])
def get_tabs_json():
    try:
        tabs_path = os.path.join('CLIENTS', 'dms', 'tabs.json')
        if os.path.exists(tabs_path):
            with open(tabs_path, 'r') as file:
                tabs_data = json.load(file)
            return jsonify(tabs_data)
        else:
            return jsonify({"error": "Archivo tabs.json no encontrado."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/get_reportes_json', methods=['GET'])
def get_reportes_json():
    dms_name = request.args.get('dms')
    if not dms_name:
        return jsonify({"error": "DMS no especificado"}), 400
    
    filepath = os.path.join('CLIENTS', 'dms', f'{dms_name}.json')
    
    if not os.path.exists(filepath):
        return jsonify({"error": "Archivo no encontrado"}), 404
    
    try:
        with open(filepath, 'r') as json_file:
            data = json.load(json_file)
            return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/guardar_registro', methods=['POST'])
def guardar_registro():
    new_record = request.json
    try:
        with open('clientes.json', 'r+') as file:
            data = json.load(file)
            data['registros'].append(new_record)
            file.seek(0)
            json.dump(data, file, indent=4)
        return jsonify(success=True)
    except Exception as e:
        print(f"Error: {e}")
        return jsonify(success=False, error=str(e))


@app.route('/obtener_registro', methods=['GET'])
@login_required
def obtener_registro():
    try:
        # Obtener los parámetros de la solicitud GET
        client_name = request.args.get('client_name')
        branch_name = request.args.get('branch_name')
        branch = request.args.get('branch')

        # Asegurarse de que los parámetros requeridos están presentes
        if not client_name or not branch_name or not branch:
            return jsonify(success=False, error="Faltan parámetros requeridos"), 400

        # Construir la ruta al archivo config.json del cliente
        client_config_path = os.path.join(os.getcwd(), 'CLIENTS', client_name, 'Config', 'config.json')

        # Verificar si el archivo config.json existe
        if not os.path.exists(client_config_path):
            return jsonify(success=False, error="Archivo de configuración no encontrado"), 404

        # Cargar el contenido del archivo config.json
        with open(client_config_path, 'r') as f:
            client_config = json.load(f)

        # Buscar el registro que coincide con la sucursal y branch especificados
        for registro in client_config.get('registros', []):
            if registro['sucursal'] == branch_name and registro['branch'] == branch:
                return jsonify(success=True, registro=registro)

        # Si no se encuentra el registro, devolver un error
        return jsonify(success=False, error="Registro no encontrado"), 404

    except Exception as e:
        # Imprimir el error en la consola para ayudar en la depuración
        print(f"Error en /obtener_registro: {str(e)}")
        return jsonify(success=False, error="Error interno del servidor"), 500
    
@app.route('/eliminar_dms', methods=['POST'])
@login_required
def eliminar_dms():
    try:
        data = request.get_json()
        client_name = data.get('client_name')
        branch_name = data.get('branch_name')
        branch = data.get('branch')
        dms_to_delete = data.get('dms')

        client_config_path = os.path.join(os.getcwd(), 'CLIENTS', client_name, 'Config', 'config.json')

        if os.path.exists(client_config_path):
            with open(client_config_path, 'r') as f:
                client_config = json.load(f)

            for registro in client_config.get('registros', []):
                if registro['branch'] == branch and registro['sucursal'] == branch_name:
                    if dms_to_delete in registro['dms']:
                        del registro['dms'][dms_to_delete]  # Eliminar el DMS y sus reportes

            with open(client_config_path, 'w') as f:
                json.dump(client_config, f, indent=4)

            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Archivo de configuración no encontrado'})

    except Exception as e:
        print(f"Error al eliminar el DMS y sus reportes: {e}")
        return jsonify({'success': False, 'error': 'No se pudo eliminar el DMS y sus reportes.'})
    

@app.route('/home')
@login_required
def home():
    return render_template('home.html')


@app.route('/search_client_folder')
@login_required
def search_client_folder():
    client_code = request.args.get('client')
    client_number = request.args.get('number')
    client_folder = os.path.join('CLIENTS', f'{client_code}{client_number}')

    if os.path.exists(client_folder):
        return jsonify({"exists": True})
    else:
        return jsonify({"exists": False})
    

@app.route('/validar_cliente', methods=['POST'])
def validar_cliente():
    data = request.get_json()
    client_number = data.get('clientNumber')[0:4]  # Los primeros 4 caracteres
    branch_code = data.get('clientNumber')[4:6]  # Los siguientes 2 caracteres
    
    # Eliminar ceros a la izquierda del número de cliente
    client_number = client_number.lstrip('0')

    # Buscar la carpeta del cliente
    client_folder = os.path.join('CLIENTS', client_number)

    if os.path.exists(client_folder):
        config_path = os.path.join(client_folder, 'Config', 'config.json')
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_data = json.load(f)
                branch_exists = False
                reportes = []

                # Verificar si el branch existe
                for registro in config_data.get('registros', []):
                    if registro['branch'] == branch_code:
                        branch_exists = True
                        # Obtener los reportes asociados al DMS
                        for dms, reportes_list in registro.get('dms', {}).items():
                            # Ajustar el nombre del reporte aquí
                            reportes.extend([f"{reporte}{branch_code}" if "SERVTC" in reporte else reporte for reporte in reportes_list])
                        break

                return jsonify({
                    'clientExists': True, 
                    'branchExists': branch_exists, 
                    'branchCode': branch_code,
                    'reportes': reportes  # Devolver el nombre correcto del reporte
                })



@app.route('/upload_file', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify(success=False), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify(success=False), 400

    if file and file.filename.endswith('.zip'):
        filename = secure_filename(file.filename)
        db_config = cargar_db_config()
        workng_dir = db_config.get('workng_dir', '')

        if not os.path.exists(workng_dir):
            return jsonify(success=False, error='Directorio de trabajo no encontrado'), 400

        file.save(os.path.join(workng_dir, filename))
        return jsonify(success=True), 200
    else:
        return jsonify(success=False), 400


@app.route('/filtrar_reportes/<tab_name>', methods=['GET'])
def filtrar_reportes(tab_name):
    query = request.args.get('query', '').lower()
    
    # Cargar el archivo JSON correspondiente a la pestaña
    file_path = os.path.join(os.path.join(os.getcwd(), 'clients', 'dms'), f'{tab_name}.json')
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    # Filtrar reportes basados en la query
    reportes_filtrados = [reporte for reporte in data['reportes'] if query in reporte.lower()]
    
    return jsonify(reportes_filtrados)


@app.route('/agregar_tipo_dato', methods=['POST'])
@login_required
def agregar_tipo_datos():
    try:
        # Obtener los datos del formulario
        tipo = request.form.get('tipo')
        descripcion = request.form.get('descripcion')

        if not tipo or not descripcion:
            return jsonify({'success': False, 'error': 'Tipo y descripción son obligatorios.'})

        # Cargar la configuración de la base de datos desde database.json
        db_config = cargar_db_config()

        # Verificar si el tipo de dato ya existe
        tipos_datos = db_config.get('tipos_datos', [])
        if any(dato['tipo'] == tipo for dato in tipos_datos):
            return jsonify({'success': False, 'error': 'El tipo de datos ya existe.'})

        # Agregar el nuevo tipo de dato
        tipos_datos.append({'tipo': tipo, 'descripcion': descripcion})
        db_config['tipos_datos'] = tipos_datos

        # Guardar los cambios en el archivo database.json
        with open('database.json', 'w') as f:
            json.dump(db_config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error al agregar el tipo de datos: {e}")
        return jsonify({'success': False, 'error': 'No se pudo agregar el tipo de datos.'})


@app.route('/guardar_tipo_dato', methods=['POST'])
@login_required
def guardar_tipo_dato():
    try:
        # Obtener los datos del request
        data = request.get_json()
        tipo = data.get('tipo')
        descripcion = data.get('descripcion')

        # Verifica si tipo y descripción están presentes
        if not tipo or not descripcion:
            return jsonify({'success': False, 'error': 'Tipo y descripción son obligatorios.'}), 400

        # Ruta del archivo database.json
        db_config_path = 'database.json'
        
        # Cargar la configuración actual desde el archivo database.json
        if os.path.exists(db_config_path):
            with open(db_config_path, 'r') as f:
                db_config = json.load(f)
        else:
            return jsonify({'success': False, 'error': 'El archivo de configuración no existe.'}), 404

        # Verificar si la llave "tipos_datos" existe, sino, crearla
        if 'tipos_datos' not in db_config:
            db_config['tipos_datos'] = []

        # Agregar el nuevo tipo y descripción al array de "tipos_datos"
        db_config['tipos_datos'].append({'tipo': tipo, 'descripcion': descripcion})

        # Guardar la configuración actualizada en el archivo database.json
        with open(db_config_path, 'w') as f:
            json.dump(db_config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error al guardar el tipo de dato: {e}")
        return jsonify({'success': False, 'error': 'No se pudo guardar el tipo de dato.'}), 500


@app.route('/eliminar_tipo_dato', methods=['POST'])
@login_required
def eliminar_tipo_dato():
    try:
        data = request.get_json()
        tipo = data.get('tipo')

        if not tipo:
            return jsonify({'success': False, 'error': 'Tipo es obligatorio.'}), 400

        db_config_path = 'database.json'

        # Cargar la configuración actual desde el archivo database.json
        if os.path.exists(db_config_path):
            with open(db_config_path, 'r') as f:
                db_config = json.load(f)
        else:
            return jsonify({'success': False, 'error': 'El archivo de configuración no existe.'}), 404

        # Filtrar los tipos de datos para eliminar el que se desea
        tipos_datos = db_config.get('tipos_datos', [])
        db_config['tipos_datos'] = [td for td in tipos_datos if td['tipo'] != tipo]

        # Guardar la configuración actualizada en el archivo database.json
        with open(db_config_path, 'w') as f:
            json.dump(db_config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error al eliminar el tipo de dato: {e}")
        return jsonify({'success': False, 'error': 'No se pudo eliminar el tipo de dato.'}), 500


@app.route('/editar_tipo_dato', methods=['POST'])
@login_required
def editar_tipo_dato():
    try:
        data = request.get_json()
        old_tipo = data.get('oldTipo')
        new_tipo = data.get('newTipo')
        new_descripcion = data.get('newDescripcion')

        if not old_tipo or not new_tipo or not new_descripcion:
            return jsonify({'success': False, 'error': 'Todos los campos son obligatorios.'}), 400

        db_config_path = 'database.json'

        # Cargar la configuración actual desde el archivo database.json
        if os.path.exists(db_config_path):
            with open(db_config_path, 'r') as f:
                db_config = json.load(f)
        else:
            return jsonify({'success': False, 'error': 'El archivo de configuración no existe.'}), 404

        # Actualizar el tipo de dato
        tipos_datos = db_config.get('tipos_datos', [])
        for td in tipos_datos:
            if td['tipo'] == old_tipo:
                td['tipo'] = new_tipo
                td['descripcion'] = new_descripcion
                break

        # Guardar la configuración actualizada en el archivo database.json
        with open(db_config_path, 'w') as f:
            json.dump(db_config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error al editar el tipo de dato: {e}")
        return jsonify({'success': False, 'error': 'No se pudo editar el tipo de dato.'}), 500


@app.route('/get_tipos_datos', methods=['GET'])
@login_required
def get_tipos_datos():
    try:
        with open('database.json', 'r') as file:
            db_config = json.load(file)
            return jsonify(db_config.get('tipos_datos', []))
    except Exception as e:
        print(f"Error al cargar tipos de datos: {e}")
        return jsonify({'success': False, 'error': 'No se pudo cargar los tipos de datos.'}), 500



@app.route('/save_columns', methods=['POST'])
@login_required
def save_columns():
    try:
        data = request.json
        print(data)
        dms_name = data.get('dms_name')
        reporte = data.get('reporte')
        data_types = data.get('data_types')

        if not dms_name or not reporte or not data_types:
            return jsonify({'success': False, 'error': 'Faltan datos para guardar las columnas.'})

        # Ruta al archivo JSON del DMS
        dms_path = os.path.join('CLIENTS', 'dms', f'{dms_name}.json')

        if not os.path.exists(dms_path):
            return jsonify({'success': False, 'error': 'El archivo del DMS no existe.'})

        # Cargar el contenido del archivo JSON
        with open(dms_path, 'r') as file:
            dms_data = json.load(file)

        # Actualizar el reporte con los tipos de datos en "data_types"
        if reporte in dms_data['reportes']:
            dms_data['columnas_esperadas'][reporte]['data_types'] = data_types

            # Guardar los cambios en el archivo JSON
            with open(dms_path, 'w') as file:
                json.dump(dms_data, file, indent=4)

            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'El reporte no existe en el DMS.'})

    except Exception as e:
        print(f"Error al guardar las columnas: {e}")
        return jsonify({'success': False, 'error': 'No se pudieron guardar las columnas.'})

@app.route('/configuracion', methods=['GET'])
@login_required
def configuracion():
    active_tab = request.args.get('active_tab', None)
    tabs = cargar_tabs()
    
    return render_template('configuracion.html', tabs=tabs, active_tab=active_tab)


@app.route('/upload_and_execute', methods=['POST'])
def upload_and_execute():
    file = request.files['file']
    print(f'file: {file}')
    workng_dir = db_config.get('workng_dir', '')
    
    if not file or not file.filename.endswith('.zip'):
        return jsonify(success=False, error="No se ha proporcionado un archivo .zip válido.")
    
    # Mover el archivo al directorio workng_dir
    try:
        file_path = os.path.join(workng_dir, file.filename)
        file.save(file_path)
        
        # Ejecutar el script prueba.py
        # Ruta al script de activación del entorno virtual en Windows
        activate_env = os.path.join('.venv', 'Scripts', 'activate.bat')

        # Comando para ejecutar el script de activación seguido del script Python
        command = f'{activate_env} && python prueba.py'

        # Ejecutar el comando en un subprocess
        result = subprocess.run(command, capture_output=True, text=True, shell=True)
        
        if result.returncode == 0:
            return jsonify(success=True, output=result.stdout)
        else:
            return jsonify(success=False, error=result.stderr)
    
    except Exception as e:
        return jsonify(success=False, error=str(e))



def get_functions_from_file(file_path):
    functions = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            tree = ast.parse(file.read())
            for node in tree.body:
                if isinstance(node, ast.FunctionDef):
                    functions.append(node.name)
        print(f"Funciones extraídas del archivo {file_path}: {functions}")
    except Exception as e:
        print(f"Error al leer el archivo {file_path}: {e}")
    return functions

@app.route('/get_functions', methods=['GET'])
def get_functions():
    file_path = 'funcionesExternas.py'  # Ruta al archivo de funciones
    functions = get_functions_from_file(file_path)
    if not functions:
        print("No se encontraron funciones en el archivo o el archivo no se pudo leer.")
    return jsonify(functions)


@app.route('/get_formulas', methods=['GET'])
@login_required
def get_formulas():
    tab_name = request.args.get('tab_name')
    reporte = request.args.get('reporte')

    if not tab_name or not reporte:
        return jsonify({'success': False, 'error': 'Pestaña y reporte son necesarios.'})

    config = cargar_config(tab_name)

    if reporte not in config['columnas_esperadas']:
        return jsonify({'success': False, 'error': 'El reporte no existe.'})

    # Obtener las columnas que tienen fórmulas
    formulas = config['columnas_esperadas'][reporte].get('formulas', {})

    return jsonify({'success': True, 'formulas': formulas})


@app.route('/obtener_reporte', methods=['GET'])
def obtener_reporte():
    nombre_reporte = request.args.get('nombreReporte')
    print(f'Nombre Reporte: {nombre_reporte}')
    nombre_reporte = re.sub(r'\d+', '', nombre_reporte)
    branch_code = request.args.get('branch_code')  # Obtener el branch_code
    
    print(f'Sucursal: {branch_code}')

    # Ruta del archivo SQL dump
    sql_dump_path = os.path.join('3-Sandbx', f'{nombre_reporte+branch_code}.sql.dump')
    print(f'sql_dump_path: {sql_dump_path}')

    if os.path.exists(sql_dump_path):
        with open(sql_dump_path, 'r') as f:
            sql_dump = f.read()

        # Procesar el SQL y generar la tabla HTML
        tabla_html = procesar_sql_y_generar_tabla(sql_dump, nombre_reporte + branch_code)
        return tabla_html
    else:
        return "<p>Error: No se encontró el archivo SQL para el reporte solicitado.</p>", 404


def procesar_sql_y_generar_tabla(sql_dump, table_name):
    connection = None
    try:
        # Conectar a la base de datos
        connection = get_database_connection()
        cursor = connection.cursor()

        # Ejecutar el SQL dump (esto incluye los INSERTS)
        cursor.execute(sql_dump)
        connection.commit()  # Asegurarse de que las inserciones se guarden

        # Ejecutar una consulta SELECT para obtener los datos de la tabla después de los INSERTS
        select_query = f"SELECT * FROM {table_name}"
        cursor.execute(select_query)

        # Obtener los resultados
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]

        # Generar el HTML de la tabla
        table_html = '<table id="reporteTabla" class="table table-striped">'
        
        # Thead
        table_html += '<thead><tr>' + ''.join([f'<th>{header}</th>' for header in headers]) + '</tr></thead>'
        
        # Tfoot para los filtros - lo generamos vacío aquí
        table_html += '<tfoot><tr>' + ''.join([f'<th><select><option value="">Filtrar por {header}</option></select></th>' for header in headers]) + '</tr></tfoot>'
        
        # Tbody
        table_html += '<tbody>'
        for row in rows:
            table_html += '<tr>' + ''.join([f'<td>{cell}</td>' for cell in row]) + '</tr>'
        table_html += '</tbody></table>'

        return table_html
    except Exception as e:
        logging.error(f"Error al procesar el SQL y generar la tabla: {e}")
        return f"<p>Error: {e}</p>"
    finally:
        if connection:
            connection.close()




@app.route('/obtener_headers/<reporte>', methods=['GET'])
def obtener_headers(reporte):
    # Ruta del archivo SQL dump
    sql_dump_path = os.path.join('3-Sandbx', f'{reporte}.sql.dump')
    
    if os.path.exists(sql_dump_path):
        with open(sql_dump_path, 'r') as f:
            sql_dump = f.read()
        
        # Buscar la sección CREATE TABLE y extraer las columnas
        headers = extraer_headers(sql_dump)
        
        if headers:
            return jsonify({'headers': headers})
        else:
            return jsonify({'error': 'No se encontraron columnas en el archivo SQL dump.'}), 404
    else:
        return jsonify({'error': f'No se encontró el archivo SQL para el reporte {reporte}.'}), 404

def extraer_headers(sql_dump):
    # Buscar la sección CREATE TABLE
    create_table_match = re.search(r'CREATE TABLE.*?\((.*?)\);', sql_dump, re.S)
    
    if create_table_match:
        # Extraer los nombres de las columnas
        columns_definition = create_table_match.group(1)
        headers = []
        
        for line in columns_definition.splitlines():
            # Ignorar líneas vacías y líneas que no definen columnas
            if line.strip() and not line.strip().startswith('--'):
                # Extraer el nombre de la columna antes de la primera palabra clave (tipo de dato)
                column_name = re.match(r'\s*([^\s]+)', line.strip())
                if column_name:
                    headers.append(column_name.group(1))
        
        return headers
    return None


@app.route('/leer_logs/<cliente>', methods=['GET'])
def leer_logs(cliente):
    try:
        # Eliminar ceros a la izquierda del número del cliente
        cliente = str(cliente).lstrip('0')
        print(f'Cliente: {cliente}')
        
        # Obtener la fecha actual en formato aaaa-mm-dd
        fecha_actual = datetime.now().strftime('%Y-%m-%d')
        log_file_path = os.path.join('CLIENTS', cliente, 'Logs', f'log_{fecha_actual}.txt')
        print(f'Log_filepath: {log_file_path}')

        # Verificar si el archivo de logs existe antes de leerlo
        if not os.path.exists(log_file_path):
            print(f'Archivo de logs no encontrado: {log_file_path}')
            return jsonify({'logs': 'Archivo de logs no encontrado.'}), 404

        # Intentar leer el archivo de logs con diferentes codificaciones
        logs = None
        codificaciones = ['utf-8', 'ISO-8859-1', 'latin1', 'Windows-1252']
        for codificacion in codificaciones:
            try:
                with open(log_file_path, 'r', encoding=codificacion) as log_file:
                    logs = log_file.read()
                    print(f'Archivo leído con la codificación {codificacion}')
                    break  # Salir del bucle si la lectura fue exitosa
            except UnicodeDecodeError:
                print(f'Error al leer con la codificación {codificacion}, probando otra...')
                continue

        # Si logs sigue siendo None, significa que no se pudo leer con ninguna codificación
        if logs is None:
            return jsonify({'logs': 'Error al leer el archivo de logs con las codificaciones probadas.'}), 500

        return jsonify({'logs': logs})

    except FileNotFoundError:
        return jsonify({'logs': 'Archivo de logs no encontrado.'}), 404
    except Exception as e:
        print(f'Error inesperado: {str(e)}')
        return jsonify({'logs': f'Error inesperado: {str(e)}'}), 500
    

@app.route('/api/get-columnas', methods=['GET'])
def get_columnas():
    reporte = request.args.get('reporte')
    tab_name = request.args.get('tab_name')  # Obtener la pestaña (DMS) donde se encuentra el reporte

    if not reporte or not tab_name:
        return jsonify({'success': False, 'error': 'Pestaña y reporte son necesarios.'})

    config = cargar_config(tab_name)  # Función que carga la configuración del DMS correspondiente

    if reporte not in config['columnas_esperadas']:
        return jsonify({'success': False, 'error': 'El reporte no existe.'})

    # Obtener las columnas del reporte
    columnas = config['columnas_esperadas'][reporte].get('columnas', [])

    return jsonify({'success': True, 'columnas': columnas})



if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)
