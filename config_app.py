from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import json
import os
import uuid
from ext import db  # Importar db desde ext.py
from forms import RegistrationForm, LoginForm
from models import User  # Import the User model after db is initialized

app = Flask(__name__)

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
            "formulas": {}
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

    columnas_info = config['columnas_esperadas'].get(reporte, {'columnas': [], 'formulas': {}})
    return jsonify({'success': True, 'columnas': columnas_info['columnas'], 'formulas': columnas_info['formulas']})

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
        tab_name = request.form.get('name')  # Obtener el nombre de la pestaña desde el formulario
        print(f'tab_name: {tab_name}')
        reporte = request.form.get('reporte')
        columna = request.form.get('columna')
        formula = request.form.get('formula')

        if not tab_name or not reporte or not columna:
            return jsonify({'success': False, 'error': 'Pestaña, reporte y columna son necesarios.'})

        # Cargar la configuración para la pestaña especificada
        config = cargar_config(tab_name)

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
        tab_name = request.form.get('tab_name')
        reporte = request.form.get('reporte')
        columnas = request.form.get('columnas')

        if not tab_name or not reporte or not columnas:
            return jsonify({'success': False, 'error': 'Faltan datos para guardar el orden de columnas.'})

        columnas_ordenadas = json.loads(columnas)

        config = cargar_config(tab_name)
        if config is None:
            return jsonify({'success': False, 'error': 'No se pudo cargar la configuración para la pestaña especificada.'})

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe en la configuración.'})

        config['columnas_esperadas'][reporte]['columnas'] = columnas_ordenadas

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
def duplicar_reporte():
    try:
        tab_name = request.form.get('tab_name')
        reporte = request.form.get('reporte')
        nuevo_reporte = request.form.get('nuevo_reporte')

        if not tab_name or not reporte or not nuevo_reporte:
            return jsonify({"success": False, "error": "Faltan parámetros"}), 400

        # Ruta del archivo JSON correspondiente a la pestaña
        json_file_path = f"CLIENTS/dms/{tab_name}.json"

        # Verifica que el archivo JSON exista
        if not os.path.exists(json_file_path):
            return jsonify({"success": False, "error": "Archivo de configuración no encontrado"}), 404

        # Cargar la configuración actual
        with open(json_file_path, 'r') as file:
            config_data = json.load(file)

        # Verifica si el reporte original existe
        if reporte not in config_data.get('reportes', []):
            return jsonify({"success": False, "error": "El reporte original no existe"}), 404

        # Verifica si el nuevo reporte ya existe
        if nuevo_reporte in config_data.get('reportes', []):
            return jsonify({"success": False, "error": "El nuevo nombre de reporte ya existe"}), 400

        # Duplicar el reporte
        config_data['reportes'].append(nuevo_reporte)
        config_data['columnas_esperadas'][nuevo_reporte] = config_data['columnas_esperadas'][reporte]

        # Guardar los cambios en el archivo JSON
        with open(json_file_path, 'w') as file:
            json.dump(config_data, file, indent=4)

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



if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)
