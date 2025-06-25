import mariadb
from multiprocessing import Process, Queue, Manager
import time
from typing import List, Dict, Any, Optional
import json

# Configuración de las 3 bases de datos con fragmentación por rangos de ID
DATABASES = [
    {
        "host": "25.40.94.84", 
        "port": 3307, 
        "user": "root", 
        "password": "root", 
        "database": "venta_tickets",
        "fragment_name": "DB1_Fragmento_1-100",
        "id_range": {"min": 1, "max": 100}
    },
    {
        "host": "25.40.94.84", 
        "port": 3308, 
        "user": "root", 
        "password": "root", 
        "database": "venta_tickets",
        "fragment_name": "DB2_Fragmento_101-200", 
        "id_range": {"min": 101, "max": 200}
    },
    {
        "host": "25.40.94.84", 
        "port": 3310, 
        "user": "root", 
        "password": "root", 
        "database": "venta_tickets",
        "fragment_name": "DB3_Fragmento_201-300",
        "id_range": {"min": 201, "max": 300}
    }
]

class DistributedDatabaseManager:
    """Gestor de base de datos distribuida con fragmentación horizontal"""
    
    def __init__(self, db_configs):
        self.db_configs = db_configs
        self.fragment_map = self._create_fragment_map()
    
    def _create_fragment_map(self):
        """Crea un mapa de fragmentos por rango de ID"""
        fragment_map = {}
        for config in self.db_configs:
            for id_val in range(config["id_range"]["min"], config["id_range"]["max"] + 1):
                fragment_map[id_val] = config
        return fragment_map
    
    def get_fragment_for_id(self, sede_id: int) -> Optional[Dict]:
        """Determina en qué fragmento está un ID específico"""
        return self.fragment_map.get(sede_id)
    
    def get_fragments_for_range(self, min_id: int, max_id: int) -> List[Dict]:
        """Determina qué fragmentos contienen un rango de IDs"""
        fragments = set()
        for id_val in range(min_id, max_id + 1):
            fragment = self.get_fragment_for_id(id_val)
            if fragment:
                fragments.add(json.dumps(fragment, sort_keys=True))
        return [json.loads(f) for f in fragments]

def execute_fragmented_query(db_config, query_info, result_queue):
    """Ejecuta consulta en un fragmento específico"""
    name = f"{db_config['fragment_name']} ({db_config['port']})"
    print(f"[{name}] Iniciando consulta en fragmento...")
    
    try:
        # Conectar a la base de datos
        conn = mariadb.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )
        cursor = conn.cursor()
        
        print(f"[{name}] Conectado. Ejecutando: {query_info['query']}")
        
        # Ejecutar la consulta
        if query_info.get('params'):
            cursor.execute(query_info['query'], query_info['params'])
        else:
            cursor.execute(query_info['query'])
        
        # Obtener resultados
        if query_info['query_type'] == 'SELECT':
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            result_data = {
                'fragment': db_config['fragment_name'],
                'data': results,
                'columns': columns,
                'count': len(results)
            }
        elif query_info['query_type'] == 'INSERT':
            conn.commit()
            result_data = {
                'fragment': db_config['fragment_name'],
                'affected_rows': cursor.rowcount,
                'last_insert_id': cursor.lastrowid
            }
        else:
            conn.commit()
            result_data = {
                'fragment': db_config['fragment_name'],
                'affected_rows': cursor.rowcount
            }
        
        result_queue.put((name, 'SUCCESS', result_data))
        print(f"[{name}] ✓ Consulta completada exitosamente")
        
    except mariadb.Error as e:
        print(f"[{name}] ✗ Error en base de datos: {e}")
        result_queue.put((name, 'ERROR', str(e)))
    except Exception as e:
        print(f"[{name}] ✗ Error general: {e}")
        result_queue.put((name, 'ERROR', str(e)))
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print(f"[{name}] Conexión cerrada")

class DistributedQueryExecutor:
    """Ejecutor de consultas distribuidas"""
    
    def __init__(self, db_manager: DistributedDatabaseManager):
        self.db_manager = db_manager
    
    def execute_global_select(self, base_query: str, filters: Dict = None) -> Dict:
        """Ejecuta SELECT en todos los fragmentos y consolida resultados"""
        print("=" * 60)
        print("🌐 EJECUTANDO CONSULTA GLOBAL DISTRIBUIDA")
        print("=" * 60)
        
        # Preparar consulta para cada fragmento
        query_info = {
            'query': base_query,
            'query_type': 'SELECT',
            'params': None
        }
        
        # Si hay filtros, construir WHERE clause
        if filters:
            if 'id_range' in filters:
                min_id, max_id = filters['id_range']
                query_info['query'] += f" WHERE id_sede BETWEEN {min_id} AND {max_id}"
                # Solo consultar fragmentos relevantes
                relevant_fragments = self.db_manager.get_fragments_for_range(min_id, max_id)
                print(f"📊 Consultando {len(relevant_fragments)} fragmentos para rango ID {min_id}-{max_id}")
            else:
                relevant_fragments = self.db_manager.db_configs
        else:
            relevant_fragments = self.db_manager.db_configs
        
        return self._execute_parallel_query(relevant_fragments, query_info)
    
    def execute_targeted_insert(self, sede_data: Dict) -> Dict:
        """Ejecuta INSERT en el fragmento apropiado según el ID"""
        print("=" * 60)
        print("📝 EJECUTANDO INSERCIÓN DIRIGIDA")
        print("=" * 60)
        
        sede_id = sede_data.get('id')
        if not sede_id:
            return {'status': 'ERROR', 'message': 'ID de sede requerido'}
        
        # Determinar fragmento apropiado
        target_fragment = self.db_manager.get_fragment_for_id(sede_id)
        if not target_fragment:
            return {'status': 'ERROR', 'message': f'No hay fragmento para ID {sede_id}'}
        
        print(f"🎯 Dirigiendo inserción de ID {sede_id} al fragmento: {target_fragment['fragment_name']}")
        
        # Construir query de inserción
        columns = ', '.join(sede_data.keys())
        placeholders = ', '.join(['?' for _ in sede_data.values()])
        query = f"INSERT INTO sedes ({columns}) VALUES ({placeholders})"
        
        query_info = {
            'query': query,
            'query_type': 'INSERT',
            'params': list(sede_data.values())
        }
        
        return self._execute_parallel_query([target_fragment], query_info)
    
    def _execute_parallel_query(self, fragments: List[Dict], query_info: Dict) -> Dict:
        """Ejecuta consulta en paralelo en los fragmentos especificados"""
        result_queue = Queue()
        processes = []
        
        # Lanzar procesos para cada fragmento
        start_time = time.time()
        for fragment in fragments:
            p = Process(target=execute_fragmented_query, 
                       args=(fragment, query_info, result_queue))
            p.start()
            processes.append(p)
        
        # Esperar resultados
        for p in processes:
            p.join()
        
        execution_time = time.time() - start_time
        
        # Consolidar resultados
        return self._consolidate_results(result_queue, execution_time, query_info['query_type'])
    
    def _consolidate_results(self, result_queue: Queue, execution_time: float, query_type: str) -> Dict:
        """Consolida resultados de múltiples fragmentos"""
        print("\n" + "🔄 CONSOLIDANDO RESULTADOS..." + "\n")
        
        consolidated = {
            'status': 'SUCCESS',
            'query_type': query_type,
            'execution_time': round(execution_time, 3),
            'fragments_processed': 0,
            'total_records': 0,
            'fragments_results': [],
            'consolidated_data': [],
            'errors': []
        }
        
        # Procesar resultados de cada fragmento
        while not result_queue.empty():
            fragment_name, status, result_data = result_queue.get()
            consolidated['fragments_processed'] += 1
            
            if status == 'SUCCESS':
                consolidated['fragments_results'].append({
                    'fragment': fragment_name,
                    'status': 'SUCCESS',
                    'data': result_data
                })
                
                if query_type == 'SELECT' and 'data' in result_data:
                    consolidated['total_records'] += result_data['count']
                    consolidated['consolidated_data'].extend(result_data['data'])
                elif query_type in ['INSERT', 'UPDATE', 'DELETE']:
                    consolidated['total_records'] += result_data.get('affected_rows', 0)
            else:
                consolidated['errors'].append({
                    'fragment': fragment_name,
                    'error': result_data
                })
        
        # Determinar status general
        if consolidated['errors']:
            consolidated['status'] = 'PARTIAL_SUCCESS' if consolidated['fragments_results'] else 'ERROR'
        
        return consolidated

def print_consolidated_results(results: Dict):
    """Imprime resultados consolidados de forma legible"""
    print("=" * 60)
    print("📊 RESULTADOS CONSOLIDADOS")
    print("=" * 60)
    
    print(f"Status: {results['status']}")
    print(f"Tipo de consulta: {results['query_type']}")
    print(f"Tiempo de ejecución: {results['execution_time']}s")
    print(f"Fragmentos procesados: {results['fragments_processed']}")
    print(f"Total de registros: {results['total_records']}")
    
    if results['query_type'] == 'SELECT' and results['consolidated_data']:
        print(f"\n📋 DATOS CONSOLIDADOS:")
        print("-" * 40)
        for i, row in enumerate(results['consolidated_data'][:10], 1):  # Mostrar primeros 10
            print(f"{i:2d}. {row}")
        
        if len(results['consolidated_data']) > 10:
            print(f"... y {len(results['consolidated_data']) - 10} registros más")
    
    if results['fragments_results']:
        print(f"\n🗂️  RESULTADOS POR FRAGMENTO:")
        print("-" * 40)
        for fragment_result in results['fragments_results']:
            fragment_name = fragment_result['fragment']
            data = fragment_result['data']
            if results['query_type'] == 'SELECT':
                print(f"✓ {fragment_name}: {data['count']} registros")
            else:
                print(f"✓ {fragment_name}: {data.get('affected_rows', 0)} filas afectadas")
    
    if results['errors']:
        print(f"\n❌ ERRORES:")
        print("-" * 40)
        for error in results['errors']:
            print(f"✗ {error['fragment']}: {error['error']}")

def demonstrate_fragmentation():
    """Demuestra el funcionamiento de la fragmentación"""
    print("🚀 INICIANDO DEMOSTRACIÓN DE BASE DE DATOS DISTRIBUIDA")
    print("=" * 60)
    
    # Inicializar gestor de BD distribuida
    db_manager = DistributedDatabaseManager(DATABASES)
    executor = DistributedQueryExecutor(db_manager)
    
    # Mostrar configuración de fragmentos
    print("📋 CONFIGURACIÓN DE FRAGMENTOS:")
    for config in DATABASES:
        print(f"  • {config['fragment_name']}: IDs {config['id_range']['min']}-{config['id_range']['max']}")
    
    # Caso 1: Consulta global (todos los fragmentos)
    print("\n🔍 CASO 1: CONSULTA GLOBAL - Todas las sedes")
    results1 = executor.execute_global_select("SELECT * FROM sedes")
    print_consolidated_results(results1)
    
    time.sleep(2)
    
    # Caso 2: Consulta con filtro de rango
    print("\n🔍 CASO 2: CONSULTA CON FILTRO - Sedes ID 50-150")
    results2 = executor.execute_global_select(
        "SELECT * FROM sedes", 
        filters={'id_range': (50, 150)}
    )
    print_consolidated_results(results2)
    
    time.sleep(2)
    
    # Caso 3: Inserción dirigida
    print("\n📝 CASO 3: INSERCIÓN DIRIGIDA - Nueva sede ID 75")
    new_sede = {
        'id': 75,
        'nombre': 'Sede Lima Centro',
        'direccion': 'Av. Javier Prado 123',
        'telefono': '01-1234567'
    }
    
    results3 = executor.execute_targeted_insert(new_sede)
    print_consolidated_results(results3)

if __name__ == '__main__':
    try:
        demonstrate_fragmentation()
    except KeyboardInterrupt:
        print("\n\n⚠️  Ejecución interrumpida por el usuario")
    except Exception as e:
        print(f"\n\n❌ Error en la demostración: {e}")
    finally:
        print("\n" + "="*60)
        print("🏁 DEMOSTRACIÓN FINALIZADA")
        print("="*60)