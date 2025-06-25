from flask import Flask, jsonify, request
from config import Config
from models import db, Concierto, Sede, Comprador, Ticket
from datetime import datetime
from sqlalchemy.exc import OperationalError, DisconnectionError
import pymysql
from pymysql.constants import CLIENT
from flask_cors import CORS
from urllib.parse import urlparse
from sqlalchemy import create_engine, text  # ✅ Asegúrate de importar `text`

from sqlalchemy.engine.url import make_url

def pick_live_database():
    for uri in Config.DATABASE_URIS:
        try:
            print(f"🔍 Probing URI: {uri}")
            parsed = make_url(uri)

            host = parsed.host
            port = parsed.port or 3306
            user = parsed.username
            password = parsed.password
            database = parsed.database

            # 🔍 Usa pymysql para validar con precisión
            conn = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                connect_timeout=3,
                client_flag=CLIENT.MULTI_STATEMENTS
            )
            conn.ping()
            conn.close()

            print(f"✅ Nodo activo confirmado: {uri}")
            return uri
        except Exception as e:
            print(f"❌ Falló URI {uri}: {e}")
    raise RuntimeError("❌ Ningún nodo disponible")

app = Flask(__name__)
CORS(app)
app.config.from_object(Config)

# Detectar nodo activo y usarlo
app.config['SQLALCHEMY_DATABASE_URI'] = pick_live_database()
db.init_app(app)
# Función para probar conexión individual
def test_db_connection(host, port, user, password, db_name):
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db_name,
            connect_timeout=5,
            client_flag=CLIENT.MULTI_STATEMENTS
        )
        conn.ping()
        conn.close()
        print(f"✅ Conexión exitosa a {host}:{port}")
        return True
    except Exception as e:
        print(f"❌ Error conectando a {host}:{port}: {str(e)}")
        return False

# Health Check
@app.route('/health', methods=['GET'])
def health_check():
    """Verifica el estado de las bases de datos"""
    status = {
        'db_3307': test_db_connection("25.40.94.84", 3307, "root", "root", "venta_tickets"),
        'db_3308': test_db_connection("25.40.94.84", 3308, "root", "root", "venta_tickets"),
        'db_3310': test_db_connection("25.40.94.84", 3310, "root", "root", "venta_tickets")
    }
    
    return jsonify({
        'status': 'healthy' if any(status.values()) else 'down',
        'databases': status
    }), 200 if any(status.values()) else 503



def execute_with_failover(query_func, model_name):
    try:
        return query_func()
    except (OperationalError, DisconnectionError) as e:
        print(f"⚠️ Error en {model_name}: {e}")
        try:
            print("🔄 Intentando failover...")
            new_uri = pick_live_database()

            # 🔁 Reconfigura la URI
            app.config['SQLALCHEMY_DATABASE_URI'] = new_uri
            db.session.remove()
            db.engine.dispose()


            # No vuelvas a inicializar db
            return query_func()
        except Exception as retry_error:
            print(f"❌ Failover fallido: {retry_error}")
            raise retry_error


# Endpoints 
@app.route('/conciertos', methods=['GET'])
def get_conciertos():
    """Obtiene todos los conciertos"""
    try:
        def query_conciertos():
            return Concierto.query.all()
        
        conciertos = execute_with_failover(query_conciertos, "conciertos")
        
        return jsonify({
            'status': 'success',
            'count': len(conciertos),
            'data': [{
                'id_concierto': c.id_concierto,
                'id_sede': c.id_sede,
                'artista': c.artista,
                'fecha': c.fecha.isoformat() if c.fecha else None
            } for c in conciertos]
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error al obtener conciertos: {str(e)}'
        }), 500


@app.route('/current-db', methods=['GET'])
def current_db():
    """Devuelve el host y puerto de la base de datos actualmente conectada"""
    try:
        uri = app.config['SQLALCHEMY_DATABASE_URI']
        parsed = urlparse(uri.replace('mysql+pymysql', 'mysql'))

        return jsonify({
            'status': 'success',
            'host': parsed.hostname,
            'port': parsed.port
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error al obtener URI activa: {str(e)}'
        }), 500


@app.route('/sedes', methods=['GET'])
def get_sedes():
    """Obtiene todas las sedes"""
    try:
        def query_sedes():
            return Sede.query.all()
        
        sedes = execute_with_failover(query_sedes, "sedes")
        
        return jsonify({
            'status': 'success',
            'count': len(sedes),
            'data': [{
                'id_sede': s.id_sede,
                'nombre': s.nombre
            } for s in sedes]
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error al obtener sedes: {str(e)}'
        }), 500

@app.route('/compradores', methods=['GET'])
def get_compradores():
    """Obtiene todos los compradores"""
    try:
        def query_compradores():
            return Comprador.query.all()
        
        compradores = execute_with_failover(query_compradores, "compradores")
        
        return jsonify({
            'status': 'success',
            'count': len(compradores),
            'data': [{
                'id_comprador': c.id_comprador,
                'nombre': c.nombre,
                'email': c.email
            } for c in compradores]
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error al obtener compradores: {str(e)}'
        }), 500

@app.route('/tickets', methods=['GET'])
def get_tickets():
    """Obtiene todos los tickets"""
    try:
        def query_tickets():
            return Ticket.query.all()
        
        tickets = execute_with_failover(query_tickets, "tickets")
        
        return jsonify({
            'status': 'success',
            'count': len(tickets),
            'data': [{
                'id_ticket': t.id_ticket,
                'id_concierto': t.id_concierto,
                'id_comprador': t.id_comprador,
                'asiento': t.asiento,
                'precio': float(t.precio) if t.precio else None,
                'fecha_compra': t.fecha_compra.isoformat() if t.fecha_compra else None
            } for t in tickets]
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error al obtener tickets: {str(e)}'
        }), 500

#Ticket post
@app.route('/tickets', methods=['POST'])
def create_ticket():
    """Crea un nuevo ticket"""
    try:
        # Obtener datos del request
        data = request.get_json()
        
        # Validar campos obligatorios
        required_fields = ['id_concierto', 'id_comprador', 'asiento', 'precio']
        if not all(field in data for field in required_fields):
            return jsonify({
                'status': 'error',
                'message': 'Faltan campos obligatorios: id_concierto, id_comprador, asiento, precio'
            }), 400
        
        # Crear nuevo ticket
        new_ticket = Ticket(
            id_concierto=data['id_concierto'],
            id_comprador=data['id_comprador'],
            asiento=data['asiento'],
            precio=data['precio'],
            fecha_compra=datetime.now()  # Fecha actual por defecto
        )
        
        # Guardar en la base de datos
        db.session.add(new_ticket)
        db.session.commit()
        
        return jsonify({
            'status': 'success',
            'message': 'Ticket creado exitosamente',
            'data': {
                'id_ticket': new_ticket.id_ticket,
                'id_concierto': new_ticket.id_concierto,
                'id_comprador': new_ticket.id_comprador,
                'asiento': new_ticket.asiento,
                'precio': float(new_ticket.precio) if new_ticket.precio else None,
                'fecha_compra': new_ticket.fecha_compra.isoformat() if new_ticket.fecha_compra else None
            }
        }), 201
    
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'status': 'error',
            'message': f'Error al crear ticket: {str(e)}'
        }), 500

# Endpoint para obtener un concierto específico con sus relaciones
@app.route('/conciertos/<int:id_concierto>', methods=['GET'])
def get_concierto_detalle(id_concierto):
    """Obtiene detalles de un concierto específico"""
    try:
        concierto = Concierto.query.get(id_concierto)
        if not concierto:
            return jsonify({
                'status': 'error',
                'message': 'Concierto no encontrado'
            }), 404
        
        # Obtener tickets del concierto
        tickets = Ticket.query.filter_by(id_concierto=id_concierto).all()
        
        return jsonify({
            'status': 'success',
            'data': {
                'concierto': {
                    'id_concierto': concierto.id_concierto,
                    'id_sede': concierto.id_sede,
                    'artista': concierto.artista,
                    'fecha': concierto.fecha.isoformat() if concierto.fecha else None
                },
                'tickets': [{
                    'id_ticket': t.id_ticket,
                    'id_comprador': t.id_comprador,
                    'asiento': t.asiento,
                    'precio': float(t.precio) if t.precio else None,
                    'fecha_compra': t.fecha_compra.isoformat() if t.fecha_compra else None
                } for t in tickets]
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error al obtener detalle del concierto: {str(e)}'
        }), 500

#Delete Ticket ID
@app.route('/tickets/<int:id_ticket>', methods=['DELETE'])
def delete_ticket(id_ticket):
    """Elimina un ticket por su ID"""
    try:
        # Buscar el ticket
        ticket = Ticket.query.get(id_ticket)
        
        if not ticket:
            return jsonify({
                'status': 'error',
                'message': f'Ticket con ID {id_ticket} no encontrado'
            }), 404
        
        # Eliminar el ticket
        db.session.delete(ticket)
        db.session.commit()
        
        return jsonify({
            'status': 'success',
            'message': f'Ticket con ID {id_ticket} eliminado correctamente'
        }), 200
    
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'status': 'error',
            'message': f'Error al eliminar ticket: {str(e)}'
        }), 500

# Endpoint para estadísticas
@app.route('/stats', methods=['GET'])
def get_stats():
    """Obtiene estadísticas generales"""
    try:
        stats = {
            'total_conciertos': Concierto.query.count(),
            'total_sedes': Sede.query.count(),
            'total_compradores': Comprador.query.count(),
            'total_tickets': Ticket.query.count()
        }
        
        return jsonify({
            'status': 'success',
            'data': stats
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error al obtener estadísticas: {str(e)}'
        }), 500

if __name__ == '__main__':
    print("🚀 Iniciando aplicación...")
    
    # Crear tablas si no existen
    with app.app_context():
        try:
            db.create_all()
            print("✅ Tablas creadas/verificadas correctamente")
        except Exception as e:
            print(f"⚠️  Error al crear tablas: {e}")
    
    # Prueba inicial de conexiones
    print("\n📡 Probando conectividad inicial...")
    test_db_connection("25.40.94.84", 3307, "root", "root", "venta_tickets")
    test_db_connection("25.40.94.84", 3308, "root", "root", "venta_tickets")
    test_db_connection("25.40.94.84", 3310, "root", "root", "venta_tickets")
    
    print("\n🌐 Iniciando servidor Flask...")
    app.run(debug=True, host='127.0.0.1', port=5000)