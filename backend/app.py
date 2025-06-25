from flask import Flask, jsonify, request
from config import Config
from models import db, Concierto, Sede, Comprador, Ticket
from datetime import datetime
from sqlalchemy.exc import OperationalError, DisconnectionError, SQLAlchemyError
import pymysql
from pymysql.constants import CLIENT
from flask_cors import CORS
from urllib.parse import urlparse
from sqlalchemy.engine.url import make_url
import time

#Librerias Paralelismo
from sqlalchemy import create_engine
import concurrent.futures
from sqlalchemy import text

app = Flask(__name__)
CORS(app)
app.config.from_object(Config)

# Estado global para almacenar la URI activa
current_active_uri = None
last_failover_time = 0
FAILOVER_COOLDOWN = 60  # 60 segundos de espera entre failovers

def probe_database(uri):
    """Verifica si una base de datos está disponible"""
    try:
        parsed = make_url(uri)
        conn = pymysql.connect(
            host=parsed.host,
            port=parsed.port or 3306,
            user=parsed.username,
            password=parsed.password,
            database=parsed.database,
            connect_timeout=3,
            client_flag=CLIENT.MULTI_STATEMENTS
        )
        conn.ping()
        conn.close()
        return True
    except Exception:
        return False

def pick_live_database():
    """Selecciona una base de datos disponible de la lista"""
    global last_failover_time
    
    # Verificar si estamos en periodo de cooldown
    if time.time() - last_failover_time < FAILOVER_COOLDOWN:
        print(f"⏳ En periodo de cooldown ({FAILOVER_COOLDOWN}s), usando URI actual")
        return current_active_uri
    
    # Probar todas las URIs en orden
    for uri in app.config['DATABASE_URIS']:
        if probe_database(uri):
            print(f"✅ Seleccionada URI activa: {uri}")
            return uri
    
    # Si ninguna está disponible, mantener la actual
    print("⚠️ No se encontraron bases de datos disponibles, usando última conocida")
    return current_active_uri

def get_current_engine():
    """Obtiene el motor de base de datos actual"""
    return db.get_engine(app)

def reconfigure_database(uri):
    """Reconfigura la aplicación para usar una nueva URI"""
    global current_active_uri
    
    if uri == current_active_uri:
        return
    
    print(f"🔄 Reconfigurando base de datos a: {uri}")
    app.config['SQLALCHEMY_DATABASE_URI'] = uri
    
    # Reiniciar conexiones
    db.session.remove()
    db.engine.dispose()
    
    # Actualizar estado
    current_active_uri = uri
    last_failover_time = time.time()

# Inicialización
with app.app_context():
    # Seleccionar base de datos inicial
    current_active_uri = pick_live_database()
    app.config['SQLALCHEMY_DATABASE_URI'] = current_active_uri
    db.init_app(app)

    # Crear tablas si no existen
    try:
        db.create_all()
        print("✅ Tablas creadas/verificadas correctamente")
    except Exception as e:
        print(f"⚠️ Error al crear tablas: {e}")

# Función para manejar operaciones con failover
def execute_with_failover(operation, operation_name):
    """Ejecuta una operación con manejo de failover"""
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            return operation()
        except (OperationalError, DisconnectionError) as e:
            if attempt < max_retries:
                print(f"⚠️ Error en {operation_name} (intento {attempt+1}/{max_retries}): {e}")
                
                # Seleccionar nueva base de datos
                new_uri = pick_live_database()
                if new_uri:
                    reconfigure_database(new_uri)
                else:
                    print("❌ No se encontró base de datos alternativa")
                
                # Esperar antes de reintentar
                time.sleep(1)
            else:
                print(f"❌ Fallo definitivo en {operation_name}: {e}")
                raise
        except SQLAlchemyError as e:
            print(f"❌ Error de base de datos en {operation_name}: {e}")
            raise

# Funcion para la simulacion del paralelismo
def run_parallel_test(num_requests):
    """Ejecuta pruebas de paralelismo en bases de datos usando SQLAlchemy"""
    print(f"\n🚀 Iniciando prueba de paralelismo con {num_requests} peticiones")
    
    # Crear motores para cada base de datos
    engines = {
        'db1': create_engine(app.config['DATABASE_URIS'][0]),
        'db2': create_engine(app.config['SQLALCHEMY_BINDS']['db2']),
        'db3': create_engine(app.config['SQLALCHEMY_BINDS']['db3'])
    }

    # Función que ejecutará cada consulta
    def run_query(engine, db_name):
        name = f"{engine.url.host}:{engine.url.port}"
        print(f"[{name}] Iniciando consulta...")
        
        try:
            start_time = time.time()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM sedes")).scalar()
                duration = time.time() - start_time
                print(f"[{name}] Consulta completada en {duration:.4f}s: {result} registros")
                return (name, result, duration)
        except Exception as e:
            print(f"[{name}] Error: {str(e)}")
            return (name, None, 0)

    # Ejecutar pruebas
    all_results = []
    for round_num in range(1, num_requests + 1):
        print(f"\n🌀 Ronda de prueba #{round_num}/{num_requests}")
        round_results = []
        total_duration = 0
        
        # Usar ThreadPoolExecutor para ejecución paralela
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(run_query, engine, db_name): db_name
                for db_name, engine in engines.items()
            }
            
            for future in concurrent.futures.as_completed(futures):
                db_name = futures[future]
                try:
                    name, count, duration = future.result()
                    result_str = f"{name}: {count} registros" if count is not None else f"{name}: Error"
                    round_results.append(result_str)
                    total_duration += duration
                except Exception as e:
                    print(f"Error procesando resultado: {str(e)}")
        
        avg_duration = total_duration / len(engines) if len(engines) > 0 else 0
        print(f"✅ Ronda #{round_num} completada en {avg_duration:.4f}s promedio")
        
        all_results.append({
            "round": round_num,
            "results": round_results,
            "avg_duration": avg_duration
        })
    
    print("\n✅ Todas las pruebas de paralelismo completadas")
    return all_results

#Funcion de fragmentacion Horizontal por region
# Configuración de fragmentos
FRAGMENT_CONFIG = {
    'region': {
        'db1': {'sede_ids': [1, 2, 3], 'name': 'Norte'},
        'db2': {'sede_ids': [4, 5, 6], 'name': 'Centro'}, 
        'db3': {'sede_ids': [7, 8, 9], 'name': 'Sur'}
    }
}

def execute_fragment_query(engine, sede_ids, table='conciertos'):
    """Ejecuta fragmentación en una BD específica"""
    with engine.connect() as conn:
        ids_str = ','.join(map(str, sede_ids))
        
        # Determinar columna correcta según la tabla
        if table == 'conciertos':
            column = 'id_sede'
        elif table == 'tickets':
            # Para tickets fragmentamos por conciertos relacionados
            query = f"""
                CREATE TABLE IF NOT EXISTS {table}_fragmento AS 
                SELECT t.* FROM {table} t 
                JOIN conciertos c ON t.id_concierto = c.id_concierto 
                WHERE c.id_sede IN ({ids_str})
            """
        else:
            column = 'id_sede'
        
        # Crear tabla fragmentada
        if table == 'tickets':
            conn.execute(text(query))
        else:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table}_fragmento AS 
                SELECT * FROM {table} WHERE {column} IN ({ids_str})
            """))
        
        # Contar registros
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}_fragmento")).scalar()
        return count

def fragment_by_region(table='conciertos'):
    """Fragmentación horizontal optimizada"""
    try:
        # Usar la nueva sintaxis de Flask-SQLAlchemy
        engines = {
            'db1': db.engine,
            'db2': db.engines.get('db2', db.engine),
            'db3': db.engines.get('db3', db.engine)
        }
        
        results = {}
        config = FRAGMENT_CONFIG['region']
        
        # Ejecutar en paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(execute_fragment_query, engines[db], config[db]['sede_ids'], table): db
                for db in config.keys()
            }
            
            for future in concurrent.futures.as_completed(futures):
                db_name = futures[future]
                try:
                    count = future.result()
                    results[db_name] = {
                        'region': config[db_name]['name'],
                        'count': count,
                        'status': 'success'
                    }
                except Exception as e:
                    results[db_name] = {'status': 'error', 'error': str(e)}
        
        return results
    except Exception as e:
        raise Exception(f"Error en fragmentación: {str(e)}")

# Endpoint para fragmentación
@app.route('/fragment', methods=['POST'])
def fragment_data():
    """Endpoint optimizado para fragmentación desde el frontend"""
    try:
        data = request.get_json() or {}
        table = data.get('table', 'conciertos')
        fragment_type = data.get('type', 'region')
        
        if fragment_type != 'region':
            return jsonify({'error': 'Solo soporta fragmentación por región'}), 400
        
        # Validar tabla según el esquema real
        valid_tables = ['conciertos', 'tickets', 'compradores', 'sedes']
        if table not in valid_tables:
            return jsonify({'error': f'Tabla debe ser: {valid_tables}'}), 400
        
        # Limpiar tablas fragmentadas existentes
        try:
            engines = {
                'db1': db.engine,
                'db2': db.engines.get('db2', db.engine),
                'db3': db.engines.get('db3', db.engine)
            }
            
            for engine in engines.values():
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table}_fragmento"))
                    conn.commit()
        except:
            pass  # Ignorar si no existe
        
        # Ejecutar fragmentación
        results = fragment_by_region(table)
        
        # Calcular estadísticas
        total_fragments = sum(r.get('count', 0) for r in results.values() if r.get('status') == 'success')
        successful_dbs = sum(1 for r in results.values() if r.get('status') == 'success')
        
        return jsonify({
            'success': True,
            'table': table,
            'fragments': results,
            'summary': {
                'total_records': total_fragments,
                'successful_dbs': successful_dbs,
                'total_dbs': len(results)
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Endpoint para verificar estado de fragmentación
@app.route('/fragment/status', methods=['GET'])
def fragment_status():
    """Verifica el estado actual de la fragmentación"""
    try:
        table = request.args.get('table', 'conciertos')
        engines = {
            'db1': db.engine,
            'db2': db.engines.get('db2', db.engine),
            'db3': db.engines.get('db3', db.engine)
        }
        
        status = {}
        for db_name, engine in engines.items():
            try:
                with engine.connect() as conn:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}_fragmento")).scalar()
                    status[db_name] = {'exists': True, 'count': count}
            except:
                status[db_name] = {'exists': False, 'count': 0}
        
        return jsonify({'status': status, 'table': table})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
# Endpoint para las pruebas del paralelismo
@app.route('/parallel/<int:num_requests>', methods=['GET'])
def test_parallel(num_requests):
    """Endpoint para ejecutar pruebas de paralelismo en bases de datos"""
    try:
        if num_requests < 1 or num_requests > 50:
            return jsonify({
                'status': 'error',
                'message': 'El número de peticiones debe estar entre 1 y 20'
            }), 400
        
        results = run_parallel_test(num_requests)
        
        return jsonify({
            'status': 'success',
            'num_requests': num_requests,
            'results': results
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error en prueba de paralelismo: {str(e)}'
        }), 500


# Health Check mejorado
@app.route('/health', methods=['GET'])
def health_check():
    """Verifica el estado de todas las bases de datos"""
    status = {}
    for uri in app.config['DATABASE_URIS']:
        parsed = make_url(uri)
        host = parsed.host
        port = parsed.port or 3306
        status[f"{host}:{port}"] = probe_database(uri)
    
    return jsonify({
        'status': 'healthy' if any(status.values()) else 'down',
        'databases': status,
        'active_database': current_active_uri
    }), 200 if any(status.values()) else 503

# Endpoint para obtener la base de datos actual
@app.route('/current-db', methods=['GET'])
def current_db():
    """Devuelve la base de datos actualmente conectada"""
    return jsonify({
        'status': 'success',
        'uri': current_active_uri
    })

# Todos los endpoints GET usan execute_with_failover
@app.route('/conciertos', methods=['GET'])
def get_conciertos():
    def query():
        return Concierto.query.all()
    
    conciertos = execute_with_failover(query, "get_conciertos")
    return jsonify([{
        'id_concierto': c.id_concierto,
        'id_sede': c.id_sede,
        'artista': c.artista,
        'fecha': c.fecha.isoformat() if c.fecha else None
    } for c in conciertos])

# ... (Similar para otros endpoints GET: sedes, compradores, tickets, stats)

# Endpoints de escritura con manejo de transacciones
@app.route('/tickets', methods=['POST'])
def create_ticket():
    data = request.get_json()
    
    # Validación
    required_fields = ['id_concierto', 'id_comprador', 'asiento', 'precio']
    if not all(field in data for field in required_fields):
        return jsonify({
            'status': 'error',
            'message': 'Faltan campos obligatorios'
        }), 400
    
    # Crear ticket
    new_ticket = Ticket(
        id_concierto=data['id_concierto'],
        id_comprador=data['id_comprador'],
        asiento=data['asiento'],
        precio=data['precio'],
        fecha_compra=datetime.now()
    )
    
    def write_operation():
        db.session.add(new_ticket)
        db.session.commit()
        return new_ticket
    
    try:
        ticket = execute_with_failover(write_operation, "create_ticket")
        return jsonify({
            'id_ticket': ticket.id_ticket,
            'status': 'success'
        }), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'status': 'error',
            'message': f'Error al crear ticket: {str(e)}'
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


@app.route('/tickets/<int:id_ticket>', methods=['DELETE'])
def delete_ticket(id_ticket):
    def delete_operation():
        ticket = Ticket.query.get(id_ticket)
        if not ticket:
            return None
        
        db.session.delete(ticket)
        db.session.commit()
        return ticket
    
    try:
        ticket = execute_with_failover(delete_operation, "delete_ticket")
        if ticket:
            return jsonify({
                'status': 'success',
                'message': f'Ticket {id_ticket} eliminado'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Ticket no encontrado'
            }), 404
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'status': 'error',
            'message': f'Error al eliminar ticket: {str(e)}'
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
    
if __name__ == '__main__':
    print("🚀 Iniciando aplicación...")
    print(f"🌐 URI inicial: {current_active_uri}")
    app.run(debug=True, host='127.0.0.1', port=5000)