import mariadb

try:
    # 1. Establecer conexión con manejo de errores
    conn = mariadb.connect(
        host="25.40.94.84",
        port=3307,
        user="root",
        password="root",
        database="venta_tickets"
    )
    
    cursor = conn.cursor()
    
    # 2. Ejecutar consulta
    cursor.execute("SELECT * FROM conciertos")
    
    # 3. Obtener UNA fila
    results = cursor.fetchall()
    # 4. Verificar si hay resultados
    if results:
        # Mostrar todas las columnas de la fila
        print(f"Registros encontrados: {len(results)}")
        for row in results:
            print(" | ".join(map(str, row)))  #
    else:
        print("No se encontraron registros")
        
except mariadb.Error as e:
    print(f"Error de base de datos: {e}")
    
except Exception as e:
    print(f"Error inesperado: {e}")
    
finally:
    # 5. Cerrar recursos siempre
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()