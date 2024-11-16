import sqlite3
import pandas as pd
from datetime import datetime

# Extract data from CSV
def extract_data_by_csv():
    # Conexión y consulta a la base de datos fuente
    path = 'insurance_data.csv'
    df = pd.read_csv(path)
    # Modificación de los nombres de las columnas
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    return df

# Transformación para Agentes (SCD Tipo 2)
def transform_data_agents(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='extract_data_by_csv')

    # Transformación SCD Tipo 2 en dimensión 'Agents'
    df_agents = df[['holder_id', 'holder_name', 'holder_age', 'holder_phone_number', 
                    'holder_incomes', 'holder_address']].copy()
    
    # Crear las columnas necesarias para SCD Tipo 2 en agentes
    df_agents['created_at'] = datetime.now()
    df_agents['updated_at'] = datetime.now()
    df_agents['valid_from'] = datetime.now()
    df_agents['valid_to'] = None
    df_agents['is_current'] = True

    # Convertir las columnas datetime a formato string para SQL
    df_agents['created_at'] = df_agents['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_agents['updated_at'] = df_agents['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_agents['valid_from'] = df_agents['valid_from'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df_agents

# Carga de datos para Agentes (SCD Tipo 2)
def load_data_agents(**kwargs):
    df_agents = kwargs['ti'].xcom_pull(task_ids='transform_data_agents')

    # Conexión a la base de datos de destino SQLite
    conn = sqlite3.connect('prueba_claro_database.db')
    cursor = conn.cursor()

    for _, row in df_agents.iterrows():
        cursor.execute("SELECT * FROM agents WHERE holder_id = ? AND is_current = 1", (row['holder_id'],))
        existing_agent = cursor.fetchone()

        if existing_agent:
            cursor.execute("""
            UPDATE agents
            SET is_current = 0, valid_to = ?
            WHERE holder_id = ? AND is_current = 1
            """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row['holder_id']))
            created_at = existing_agent[11]  # Tomamos el 'created_at' más antiguo
        else:
            created_at = row['created_at']

        cursor.execute("""
        INSERT INTO agents (holder_id, holder_name, holder_age, holder_phone_number,
                            holder_incomes, holder_address, valid_from, valid_to, is_current, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (row['holder_id'], row['holder_name'], row['holder_age'], row['holder_phone_number'],
              row['holder_incomes'], row['holder_address'], row['valid_from'], row['valid_to'], row['is_current'], row['updated_at'], created_at))

    conn.commit()  # Asegura que los cambios se guarden en la base de datos
    conn.close()   # Cierra la conexión

# Transformación para Pólizas (SCD Tipo 2)
def transform_data_policies(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='extract_data_by_csv')

    # Transformación SCD Tipo 2 en dimensión 'Policies'
    df_policies = df[['policy_id', 'start_date', 'end_date', 'carrier_name']].copy()
    
    # Crear las columnas necesarias para SCD Tipo 2 en pólizas
    df_policies['created_at'] = datetime.now()
    df_policies['updated_at'] = datetime.now()
    df_policies['valid_from'] = datetime.now()
    df_policies['valid_to'] = None
    df_policies['is_current'] = True

    # Convertir las columnas datetime a formato string para SQL
    df_policies['created_at'] = df_policies['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_policies['updated_at'] = df_policies['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_policies['valid_from'] = df_policies['valid_from'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df_policies

# Carga de datos para Pólizas (SCD Tipo 2)
def load_data_policies(**kwargs):
    df_policies = kwargs['ti'].xcom_pull(task_ids='transform_data_policies')

    # Conexión a la base de datos de destino SQLite
    conn = sqlite3.connect('prueba_claro_database.db')
    cursor = conn.cursor()

    for _, row in df_policies.iterrows():
        cursor.execute("SELECT * FROM policies WHERE policy_id = ? AND is_current = 1", (row['policy_id'],))
        existing_policy = cursor.fetchone()

        if existing_policy:
            cursor.execute("""
            UPDATE policies
            SET is_current = 0, valid_to = ?
            WHERE policy_id = ? AND is_current = 1
            """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row['policy_id']))
            created_at = existing_policy[9]  # Tomamos el 'created_at' más antiguo
        else:
            created_at = row['created_at']

        cursor.execute("""
        INSERT INTO policies (policy_id, start_date, end_date, carrier_name, valid_from, valid_to, is_current, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (row['policy_id'], row['start_date'], row['end_date'], row['carrier_name'], row['valid_from'], row['valid_to'], row['is_current'], row['updated_at'], created_at))

    conn.commit()  # Asegura que los cambios se guarden en la base de datos
    conn.close()   # Cierra la conexión

def transform_data_bob(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='extract_data_by_csv')

    # Seleccionamos las columnas necesarias de la tabla original
    df_bob = df[['holder_id', 'policy_id', 'days_of_payment_delay', 'commission_amount', 'members']]

    # Convertimos las fechas a formato string para su uso posterior en la base de datos
    df_bob['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_bob['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return df_bob

# Carga de datos para Bob (SCD Tipo 2)
def load_data_bob(**kwargs):
    df_bob = kwargs['ti'].xcom_pull(task_ids='transform_data_bob')

    # Conexión a la base de datos de destino SQLite
    conn = sqlite3.connect('prueba_claro_database.db')
    cursor = conn.cursor()

    for _, row in df_bob.iterrows():
        cursor.execute("""
            SELECT * FROM bob WHERE holder_id = ? AND policy_id = ?
        """, (row['holder_id'], row['policy_id']))
        existing_bob = cursor.fetchone()

        if existing_bob:
            # Si el registro existe, actualizamos la fila actual
            cursor.execute("""
                UPDATE bob
                SET 
                    days_of_payment_delay = ?,
                    commission_amount = ?,
                    members = ?,
                    updated_at = ?, 
                WHERE holder_id = ? AND policy_id = ?
            """, (row['days_of_payment_delay'], row['commission_amount'], row['members'],
                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'), # valid_to es la fecha de la actualización
                  row['holder_id'], row['policy_id']))
        else:
            # Si el registro no existe, insertamos uno nuevo
            cursor.execute("""
                INSERT INTO bob (holder_id, policy_id, days_of_payment_delay, commission_amount, 
                                 members, updated_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (row['holder_id'], row['policy_id'], row['days_of_payment_delay'], row['commission_amount'],
                  row['members'], row['updated_at'], row['created_at']
                  ))

    conn.commit()  # Asegura que los cambios se guarden en la base de datos
    conn.close()   # Cierra la conexión

# Simulación de la clase TaskInstance para pruebas locales
class FakeTaskInstance:
    def __init__(self, data):
        self.data = data

    def xcom_pull(self, task_ids):
        # Simula la acción de xcom_pull, devolviendo el DataFrame
        return self.data

# Simula la ejecución de las funciones
# Paso 1: Extraer datos
data = extract_data_by_csv()

# Simula un objeto TaskInstance que contiene los datos extraídos
fake_ti = FakeTaskInstance(data)

# Paso 2: Transformar datos para Agentes
transformed_agents_data = transform_data_agents(ti=fake_ti)

# Simula un objeto TaskInstance que contiene los datos transformados para Agentes
fake_ti = FakeTaskInstance(transformed_agents_data)

# Paso 3: Cargar datos para Agentes
load_data_agents(ti=fake_ti)

print("Extracted Data (Agents):")

fake_ti = FakeTaskInstance(data)

# Paso 2: Transformar datos para Pólizas
transformed_policies_data = transform_data_policies(ti=fake_ti)

# Simula un objeto TaskInstance que contiene los datos transformados para Pólizas
fake_ti = FakeTaskInstance(transformed_policies_data)

# Paso 3: Cargar datos para Pólizas
load_data_policies(ti=fake_ti)

print("Extracted Data (Policies):")
fake_ti = FakeTaskInstance(data)

# Paso 4: Transformar datos para Pólizas
transformed_data_bob = transform_data_bob(ti=fake_ti)

# Simula un objeto TaskInstance que contiene los datos transformados para Pólizas
fake_ti = FakeTaskInstance(transformed_data_bob)

# Paso 5: Transformar datos para Pólizas
load_data_bob(ti=fake_ti)

print("Extracted Data (bob):")

