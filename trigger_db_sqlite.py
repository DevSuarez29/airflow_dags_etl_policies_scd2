import sqlite3

# Nombre del archivo de la base de datos
db_name = "prueba_claro_database.db"

# Nombre del archivo SQL que contiene las instrucciones
sql_file = "prueba_claro_tecnica.sql"

# Crear la conexión y ejecutar las instrucciones del archivo SQL
try:
    # Conectarse (creará la base si no existe)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Leer el archivo SQL
    with open(sql_file, 'r') as file:
        sql_script = file.read()
    
    # Ejecutar las instrucciones del archivo SQL
    cursor.executescript(sql_script)
    
    print(f"Base de datos '{db_name}' creada y script ejecutado con éxito.")
except Exception as e:
    print(f"Error: {e}")
finally:
    # Cerrar la conexión
    conn.close()
