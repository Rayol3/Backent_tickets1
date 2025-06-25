from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import os
from decimal import Decimal
import pymysql

# Instalar PyMySQL como adaptador MySQL para SQLAlchemy
pymysql.install_as_MySQLdb()

app = Flask(__name__)

# Configuración de las bases de datos MariaDB
class Config:
    # Configuración de conexión a MariaDB
    MARIADB_CONFIG = {
        'host': os.getenv('MARIADB_HOST', 'localhost'),
        'port': int(os.getenv('MARIADB_PORT', 3306)),
        'user': os.getenv('MARIADB_USER', 'root'),
        'password': os.getenv('MARIADB_PASSWORD', '12345678'),
    }
    
    # Base de datos por sede
    DATABASES = {
        'sede1': f"mysql+pymysql://{MARIADB_CONFIG['user']}:{MARIADB_CONFIG['password']}@{MARIADB_CONFIG['host']}:{MARIADB_CONFIG['3307']}/conciertos_sede1?charset=utf8mb4",
        'sede2': f"mysql+pymysql://{MARIADB_CONFIG['user']}:{MARIADB_CONFIG['password']}@{MARIADB_CONFIG['host']}:{MARIADB_CONFIG['3308']}/conciertos_sede2?charset=utf8mb4",
        'sede3': f"mysql+pymysql://{MARIADB_CONFIG['user']}:{MARIADB_CONFIG['password']}@{MARIADB_CONFIG['host']}:{MARIADB_CONFIG['3309']}/conciertos_sede3?charset=utf8mb4"
    }

# Configuración global
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_pre_ping': True,
    'pool_recycle': 3600,
    'pool_timeout': 20,
    'max_overflow': 0
}

# Diccionario para almacenar las conexiones de BD
db_connections = {}

# Función para crear conexión a BD específica
def get_db_for_sede(sede_id):
    if sede_id not in db_connections:
        # Crear nueva instancia de SQLAlchemy para esta sede
        db = SQLAlchemy()
        
        # Configurar la URI específica para esta sede
        database_uri = Config.DATABASES.get(f'sede{sede_id}')
        if not database_uri:
            raise ValueError(f"No existe configuración para sede {sede_id}")
        
        # Crear configuración temporal para esta conexión
        temp_config = {
            'SQLALCHEMY_DATABASE_URI': database_uri,
            'SQLALCHEMY_TRACK_MODIFICATIONS': False,
            'SQLALCHEMY_ENGINE_OPTIONS': app.config['SQLALCHEMY_ENGINE_OPTIONS']
        }
        
        # Aplicar configuración y inicializar
        old_config = {}
        for key, value in temp_config.items():
            old_config[key] = app.config.get(key)
            app.config[key] = value
        
        with app.app_context():
            db.init_app(app)
            db_connections[sede_id] = db
        
        # Restaurar configuración
        for key, value in old_config.items():
            if value is not None:
                app.config[key] = value
            else:
                app.config.pop(key, None)
    
    return db_connections[sede_id]

# Modelo base que se usará para cada sede
class BaseModels:
    @staticmethod
    def create_models(db):
        class Sede(db.Model):
            __tablename__ = 'sedes'
            id_sede = db.Column(db.Integer, primary_key=True, autoincrement=True)
            nombre = db.Column(db.String(50), nullable=False, charset='utf8mb4', collation='utf8mb4_unicode_ci')
            
            # Relaciones
            conciertos = db.relationship('Concierto', backref='sede', lazy=True)
            
            def to_dict(self):
                return {
                    'id_sede': self.id_sede,
                    'nombre': self.nombre
                }

        class Concierto(db.Model):
            __tablename__ = 'conciertos'
            id_concierto = db.Column(db.Integer, primary_key=True, autoincrement=True)
            id_sede = db.Column(db.Integer, db.ForeignKey('sedes.id_sede'), nullable=False)
            artista = db.Column(db.String(100), nullable=False, charset='utf8mb4', collation='utf8mb4_unicode_ci')
            fecha = db.Column(db.DateTime, nullable=False)
            
            # Relaciones
            tickets = db.relationship('Ticket', backref='concierto', lazy=True)
            
            def to_dict(self):
                return {
                    'id_concierto': self.id_concierto,
                    'id_sede': self.id_sede,
                    'artista': self.artista,
                    'fecha': self.fecha.isoformat() if self.fecha else None
                }

        class Comprador(db.Model):
            __tablename__ = 'compradores'
            id_comprador = db.Column(db.Integer, primary_key=True, autoincrement=True)
            nombre = db.Column(db.String(100), nullable=False, charset='utf8mb4', collation='utf8mb4_unicode_ci')
            email = db.Column(db.String(100), nullable=False, unique=True)
            
            # Relaciones
            tickets = db.relationship('Ticket', backref='comprador', lazy=True)
            
            def to_dict(self):
                return {
                    'id_comprador': self.id_comprador,
                    'nombre': self.nombre,
                    'email': self.email
                }

        class Ticket(db.Model):
            __tablename__ = 'tickets'
            id_ticket = db.Column(db.Integer, primary_key=True, autoincrement=True)
            id_concierto = db.Column(db.Integer, db.ForeignKey('conciertos.id_concierto'), nullable=False)
            id_comprador = db.Column(db.Integer, db.ForeignKey('compradores.id_comprador'), nullable=False)
            asiento = db.Column(db.String(10), nullable=False)
            precio = db.Column(db.Decimal(8,2), nullable=False)
            fecha_compra = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
            
            # Índice único compuesto para evitar asientos duplicados por concierto
            __table_args__ = (
                db.UniqueConstraint('id_concierto', 'asiento', name='uq_concierto_asiento'),
            )
            
            def to_dict(self):
                return {
                    'id_ticket': self.id_ticket,
                    'id_concierto': self.id_concierto,
                    'id_comprador': self.id_comprador,
                    'asiento': self.asiento,
                    'precio': float(self.precio),
                    'fecha_compra': self.fecha_compra.isoformat()
                }

        return Sede, Concierto, Comprador, Ticket

# Función auxiliar para manejar errores de BD
def handle_db_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Log del error (en producción usar logging)
            print(f"Error en base de datos: {str(e)}")
            
            # Diferentes tipos de errores de MariaDB
            error_message = str(e).lower()
            
            if "duplicate entry" in error_message:
                return jsonify({
                    'success': False,
                    'error': 'Ya existe un registro con esos datos'
                }), 409
            elif "foreign key constraint" in error_message:
                return jsonify({
                    'success': False,
                    'error': 'Referencia inválida a datos relacionados'
                }), 400
            elif "connection" in error_message:
                return jsonify({
                    'success': False,
                    'error': 'Error de conexión a la base de datos'
                }), 503
            else:
                return jsonify({
                    'success': False,
                    'error': f'Error interno del servidor: {str(e)}'
                }), 500
    
    wrapper.__name__ = func.__name__
    return wrapper

# Endpoint para obtener sedes disponibles
@app.route('/api/sedes', methods=['GET'])
@handle_db_error
def get_sedes():
    """Obtiene todas las sedes disponibles"""
    sedes_info = []
    
    for sede_id in [1, 2, 3]:  # Asumiendo 3 sedes
        try:
            with app.app_context():
                app.config['SQLALCHEMY_DATABASE_URI'] = Config.DATABASES[f'sede{sede_id}']
                db = get_db_for_sede(sede_id)
                Sede, _, _, _ = BaseModels.create_models(db)
                
                # Obtener información de la sede
                sede = Sede.query.first()
                if sede:
                    sedes_info.append({
                        'id_sede': sede.id_sede,
                        'nombre': sede.nombre,
                        'database': f'conciertos_sede{sede_id}'
                    })
        except Exception as e:
            print(f"Error accediendo a sede {sede_id}: {e}")
            continue
    
    return jsonify({
        'success': True,
        'sedes': sedes_info
    })

# Endpoint para obtener conciertos de una sede específica
@app.route('/api/sede/<int:sede_id>/conciertos', methods=['GET'])
@handle_db_error
def get_conciertos_sede(sede_id):
    """Obtiene todos los conciertos de una sede específica"""
    with app.app_context():
        app.config['SQLALCHEMY_DATABASE_URI'] = Config.DATABASES[f'sede{sede_id}']
        db = get_db_for_sede(sede_id)
        _, Concierto, _, _ = BaseModels.create_models(db)
        
        # Filtros opcionales
        fecha_desde = request.args.get('fecha_desde')
        fecha_hasta = request.args.get('fecha_hasta')
        artista = request.args.get('artista')
        
        query = Concierto.query.filter_by(id_sede=sede_id)
        
        if fecha_desde:
            query = query.filter(Concierto.fecha >= datetime.fromisoformat(fecha_desde))
        if fecha_hasta:
            query = query.filter(Concierto.fecha <= datetime.fromisoformat(fecha_hasta))
        if artista:
            query = query.filter(Concierto.artista.like(f'%{artista}%'))
        
        conciertos = query.order_by(Concierto.fecha.asc()).all()
        conciertos_data = [concierto.to_dict() for concierto in conciertos]
        
        return jsonify({
            'success': True,
            'sede_id': sede_id,
            'conciertos': conciertos_data,
            'total': len(conciertos_data)
        })

# Endpoint principal para registrar ticket
@app.route('/api/sede/<int:sede_id>/ticket', methods=['POST'])
@handle_db_error
def registrar_ticket(sede_id):
    """Registra un nuevo ticket en la sede especificada"""
    data = request.get_json()
    
    # Validar datos requeridos
    required_fields = ['id_concierto', 'comprador', 'asiento', 'precio']
    for field in required_fields:
        if field not in data:
            return jsonify({
                'success': False,
                'error': f'Campo requerido faltante: {field}'
            }), 400
    
    # Validar estructura del comprador
    comprador_fields = ['nombre', 'email']
    for field in comprador_fields:
        if field not in data['comprador']:
            return jsonify({
                'success': False,
                'error': f'Campo requerido en comprador: {field}'
            }), 400
    
    with app.app_context():
        app.config['SQLALCHEMY_DATABASE_URI'] = Config.DATABASES[f'sede{sede_id}']
        db = get_db_for_sede(sede_id)
        Sede, Concierto, Comprador, Ticket = BaseModels.create_models(db)
        
        try:
            # Verificar que el concierto existe en esta sede
            concierto = Concierto.query.filter_by(
                id_concierto=data['id_concierto'],
                id_sede=sede_id
            ).first()
            
            if not concierto:
                return jsonify({
                    'success': False,
                    'error': 'El concierto no existe en esta sede'
                }), 404
            
            # Verificar que el asiento no esté ocupado
            asiento_ocupado = Ticket.query.filter_by(
                id_concierto=data['id_concierto'],
                asiento=data['asiento']
            ).first()
            
            if asiento_ocupado:
                return jsonify({
                    'success': False,
                    'error': f'El asiento {data["asiento"]} ya está ocupado para este concierto'
                }), 409
            
            # Buscar o crear comprador
            comprador_data = data['comprador']
            comprador = Comprador.query.filter_by(
                email=comprador_data['email']
            ).first()
            
            if not comprador:
                comprador = Comprador(
                    nombre=comprador_data['nombre'],
                    email=comprador_data['email']
                )
                db.session.add(comprador)
                db.session.flush()  # Para obtener el ID sin hacer commit
            
            # Crear el ticket
            ticket = Ticket(
                id_concierto=data['id_concierto'],
                id_comprador=comprador.id_comprador,
                asiento=data['asiento'],
                precio=Decimal(str(data['precio'])),
                fecha_compra=datetime.utcnow()
            )
            
            db.session.add(ticket)
            db.session.commit()
            
            # Respuesta exitosa con información completa
            return jsonify({
                'success': True,
                'message': 'Ticket registrado exitosamente',
                'ticket': {
                    'id_ticket': ticket.id_ticket,
                    'id_concierto': ticket.id_concierto,
                    'asiento': ticket.asiento,
                    'precio': float(ticket.precio),
                    'sede_id': sede_id,
                    'artista': concierto.artista,
                    'fecha_concierto': concierto.fecha.isoformat(),
                    'comprador': {
                        'id_comprador': comprador.id_comprador,
                        'nombre': comprador.nombre,
                        'email': comprador.email
                    },
                    'fecha_compra': ticket.fecha_compra.isoformat()
                }
            }), 201
            
        except Exception as e:
            db.session.rollback()
            raise e

# Endpoint para obtener tickets de una sede
@app.route('/api/sede/<int:sede_id>/tickets', methods=['GET'])
@handle_db_error
def get_tickets_sede(sede_id):
    """Obtiene todos los tickets de una sede específica"""
    with app.app_context():
        app.config['SQLALCHEMY_DATABASE_URI'] = Config.DATABASES[f'sede{sede_id}']
        db = get_db_for_sede(sede_id)
        _, Concierto, Comprador, Ticket = BaseModels.create_models(db)
        
        # Parámetros de filtrado
        concierto_id = request.args.get('concierto_id', type=int)
        comprador_email = request.args.get('comprador_email')
        fecha_desde = request.args.get('fecha_desde')
        fecha_hasta = request.args.get('fecha_hasta')
        
        # Query con joins para obtener información completa
        query = db.session.query(Ticket, Concierto, Comprador)\
            .join(Concierto, Ticket.id_concierto == Concierto.id_concierto)\
            .join(Comprador, Ticket.id_comprador == Comprador.id_comprador)
        
        # Aplicar filtros
        if concierto_id:
            query = query.filter(Ticket.id_concierto == concierto_id)
        if comprador_email:
            query = query.filter(Comprador.email.like(f'%{comprador_email}%'))
        if fecha_desde:
            query = query.filter(Ticket.fecha_compra >= datetime.fromisoformat(fecha_desde))
        if fecha_hasta:
            query = query.filter(Ticket.fecha_compra <= datetime.fromisoformat(fecha_hasta))
        
        tickets = query.order_by(Ticket.fecha_compra.desc()).all()
        
        tickets_data = []
        for ticket, concierto, comprador in tickets:
            tickets_data.append({
                'id_ticket': ticket.id_ticket,
                'asiento': ticket.asiento,
                'precio': float(ticket.precio),
                'fecha_compra': ticket.fecha_compra.isoformat(),
                'concierto': {
                    'id_concierto': concierto.id_concierto,
                    'artista': concierto.artista,
                    'fecha': concierto.fecha.isoformat()
                },
                'comprador': {
                    'id_comprador': comprador.id_comprador,
                    'nombre': comprador.nombre,
                    'email': comprador.email
                }
            })
        
        return jsonify({
            'success': True,
            'sede_id': sede_id,
            'tickets': tickets_data,
            'total': len(tickets_data)
        })

# Endpoint para verificar disponibilidad de asientos
@app.route('/api/sede/<int:sede_id>/concierto/<int:concierto_id>/asientos', methods=['GET'])
@handle_db_error
def get_asientos_disponibles(sede_id, concierto_id):
    """Obtiene información sobre asientos ocupados de un concierto"""
    with app.app_context():
        app.config['SQLALCHEMY_DATABASE_URI'] = Config.DATABASES[f'sede{sede_id}']
        db = get_db_for_sede(sede_id)
        _, Concierto, _, Ticket = BaseModels.create_models(db)
        
        # Verificar que el concierto existe
        concierto = Concierto.query.filter_by(
            id_concierto=concierto_id,
            id_sede=sede_id
        ).first()
        
        if not concierto:
            return jsonify({
                'success': False,
                'error': 'El concierto no existe en esta sede'
            }), 404
        
        # Obtener asientos ocupados
        asientos_ocupados = db.session.query(Ticket.asiento)\
            .filter_by(id_concierto=concierto_id)\
            .all()
        
        asientos_ocupados_list = [asiento[0] for asiento in asientos_ocupados]
        
        return jsonify({
            'success': True,
            'concierto': concierto.to_dict(),
            'asientos_ocupados': asientos_ocupados_list,
            'total_ocupados': len(asientos_ocupados_list)
        })

# Función para inicializar las bases de datos
def init_databases():
    """Inicializa las tablas en todas las bases de datos MariaDB"""
    import pymysql
    
    # Configuración de conexión
    connection_config = Config.MARIADB_CONFIG
    
    for sede_id in [1, 2, 3]:
        try:
            # Crear conexión directa para crear la base de datos si no existe
            connection = pymysql.connect(
                host=connection_config['host'],
                port=connection_config['port'],
                user=connection_config['user'],
                password=connection_config['password'],
                charset='utf8mb4'
            )
            
            with connection.cursor() as cursor:
                # Crear base de datos si no existe
                db_name = f'conciertos_sede{sede_id}'
                cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci')
                print(f"Base de datos {db_name} verificada/creada")
            
            connection.close()
            
            # Ahora crear las tablas usando SQLAlchemy
            with app.app_context():
                app.config['SQLALCHEMY_DATABASE_URI'] = Config.DATABASES[f'sede{sede_id}']
                
                db = SQLAlchemy()
                db.init_app(app)
                
                # Crear las tablas
                BaseModels.create_models(db)
                db.create_all()
                
                print(f"Tablas creadas para sede {sede_id}")
                
        except Exception as e:
            print(f"Error inicializando base de datos para sede {sede_id}: {e}")

if __name__ == '__main__':
    try:
        # Inicializar bases de datos
        init_databases()
        
        # Ejecutar la aplicación
        app.run(debug=True, host='0.0.0.0', port=5000)
    except Exception as e:
        print(f"Error iniciando la aplicación: {e}")
        print("Verifica que MariaDB esté ejecutándose y las credenciales sean correctas")