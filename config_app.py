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

# Cargar la configuración desde config.json
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

# Cargar la configuración de la base de datos desde config.json
config = cargar_config()
db_config = config.get('db', {})
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
    config = cargar_config()
    return render_template('index.html', config=config)

@app.route('/', methods=['POST'])
@login_required
def save_config():
    try:
        data = request.form
        config = cargar_config()

        config['workng_dir'] = data.get('workng_dir', '')
        config['sandbx'] = data.get('sandbx', '')
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

@app.route('/add-reporte', methods=['POST'])
@login_required
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

        if nombre not in config['reportes']:
            config['reportes'].append(nombre)

        with open('config.json', 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'success': False, 'error': 'No se pudo agregar el reporte.'})

@app.route('/delete-reporte', methods=['POST'])
@login_required
def delete_reporte():
    try:
        reporte = request.form.get('reporte')

        if not reporte:
            return jsonify({'success': False, 'error': 'El reporte no puede estar vacío.'})

        config = cargar_config()

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

        del config['columnas_esperadas'][reporte]

        if reporte in config['reportes']:
            config['reportes'].remove(reporte)

        with open('config.json', 'w') as f:
            json.dump(config, f, indent=4)

        return jsonify({'success': True})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'success': False, 'error': 'No se pudo eliminar el reporte.'})

@app.route('/edit_reporte/<reporte>', methods=['GET'])
@login_required
def edit_reporte(reporte):
    config = cargar_config()
    columnas_info = config['columnas_esperadas'].get(reporte, {'columnas': [], 'formulas': {}})
    
    if isinstance(columnas_info, list):
        columnas_info = {'columnas': columnas_info, 'formulas': {}}

    columnas = columnas_info.get('columnas', [])
    formulas = columnas_info.get('formulas', {})

    return jsonify({'success': True, 'columnas': columnas, 'formulas': formulas})

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

        if os.path.exists(client_config_path):
            with open(client_config_path, 'r') as f:
                existing_config = json.load(f)
                existing_branches = {reg['branch'] for reg in existing_config.get('registros', [])}
        else:
            existing_config = {'registros': []}
            existing_branches = set()

        nuevos_registros = [registro for registro in registros if registro['branch'] not in existing_branches]

        if not nuevos_registros:
            return jsonify({'success': False, 'error': 'No hay nuevos registros para agregar.'})

        existing_config['registros'].extend(nuevos_registros)

        with open(client_config_path, 'w') as f:
            json.dump(existing_config, f, indent=4)

        return jsonify({'success': True, 'added_records': nuevos_registros})

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
        reporte = request.form.get('reporte')
        columna = request.form.get('columna')
        formula = request.form.get('formula')

        if not reporte or not columna:
            return jsonify({'success': False, 'error': 'Reporte y columna son necesarios.'})

        config = cargar_config()

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

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
@login_required
def delete_formula():
    try:
        reporte = request.form.get('reporte')
        columna = request.form.get('columna')

        if not reporte or not columna:
            return jsonify({'success': False, 'error': 'Reporte y columna son necesarios.'})

        config = cargar_config()

        if reporte not in config['columnas_esperadas']:
            return jsonify({'success': False, 'error': 'El reporte no existe.'})

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
    with app.app_context():
        db.create_all()
    app.run(debug=True)
