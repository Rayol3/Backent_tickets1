import mariadb
import threading
import time

# Configuración de las 3 bases de datos
DATABASES = [
    {"host": "25.40.94.84", "port": 3307, "user": "root", "password": "root", "database": "venta_tickets"},
    {"host": "25.40.94.84", "port": 3308, "user": "root", "password": "root", "database": "venta_tickets"},
    {"host": "25.40.94.84", "port": 3310, "user": "root", "password": "root", "database": "venta_tickets"}
]

# Función para ejecutar consultas en una base de datos
def run_query(db_config):
    name = f"{db_config['host']}:{db_config['port']}"
    print(f"[{name}] Conectando...")
    
    try:
        # Establecer conexión
        conn = mariadb.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )
        cursor = conn.cursor()
        
        print(f"[{name}] ¡Conectado! Realizando consulta...")
        
        # Ejecutar consulta
        cursor.execute("SELECT * FROM compradores")
        result = cursor.fetchone()
        
        print(f"[{name}] Resultado: ")
        for row in result:
            print(row)
        
    except mariadb.Error as e:
        print(f"[{name}] Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            print(f"[{name}] Conexión cerrada")
        print(f"[{name}] Proceso completado\n")

# Crear y ejecutar hilos
threads = []
for i, db_config in enumerate(DATABASES):
    thread = threading.Thread(
        target=run_query,
        args=(db_config,),
        name=f"DB-{db_config['port']}"  # Nombre del hilo
    )
    threads.append(thread)
    thread.start()
    print(f"Iniciado hilo para puerto {db_config['port']}")

# Esperar a que todos los hilos terminen
for thread in threads:
    thread.join()

print("Todas las consultas han finalizado")