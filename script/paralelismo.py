import mariadb
from multiprocessing import Process, Queue
import time

# Configuración de las 3 bases de datos
DATABASES = [
    {"host": "localhost", "port": 3307, "user": "root", "password": "root", "database": "venta_tickets"},
    {"host": "localhost", "port": 3308, "user": "root", "password": "root", "database": "venta_tickets"},
    {"host": "localhost", "port": 3310, "user": "root", "password": "root", "database": "venta_tickets"}
]

# Función que ejecutará cada proceso
def run_query(db_config, result_queue):
    name = f"{db_config['host']}:{db_config['port']}"
    print(f"[{name}] (Proceso) Conectando...")
    
    try:
        conn = mariadb.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )
        cursor = conn.cursor()
        
        print(f"[{name}] (Proceso) ¡Conectado! Realizando consulta...")
        
        # Ejecutar consulta
        cursor.execute("SELECT * FROM sedes")
        result = cursor.fetchone()
        
        # Guardar resultado en la cola
        result_queue.put((name, result[0]))
        
        print(f"[{name}] (Proceso) Resultado obtenido")
        
    except mariadb.Error as e:
        print(f"[{name}] (Proceso) Error: {e}")
        result_queue.put((name, None))
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            print(f"[{name}] (Proceso) Conexión cerrada")
        print(f"[{name}] (Proceso) Finalizado\n")

if __name__ == '__main__':
    # Cola para compartir resultados entre procesos
    result_queue = Queue()
    
    # Crear y ejecutar procesos
    processes = []
    for db_config in DATABASES:
        p = Process(target=run_query, args=(db_config, result_queue))
        p.start()
        processes.append(p)
        print(f"Lanzado proceso para puerto {db_config['port']}")

    # Esperar a que todos los procesos terminen
    for p in processes:
        p.join()
    
    print("\nTodos los procesos han finalizado. Resultados:")
    
    # Recoger resultados de la cola
    while not result_queue.empty():
        name, count = result_queue.get()
        if count is not None:
            print(f"{name}: {count} registros")
        else:
            print(f"{name}: Error en la consulta")