from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
import sqlite3

# Context manager para conexión a la base de datos
class DatabaseConnection:
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.connection = None

    def __enter__(self):
        conn_config = BaseHook.get_connection(self.conn_id)
        self.connection = sqlite3.connect(conn_config.host)  # Usa el host como nombre de archivo DB
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.commit()
            self.connection.close()

# Función para extraer datos desde CSV
def extract_data_by_csv(**kwargs):
    path = 'insurance_data.csv'
    df = pd.read_csv(path)
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    kwargs['ti'].xcom_push(key='dataframe', value=df.to_dict(orient='list'))

# Función genérica para actualizar tablas (SCD Tipo 2)
def upsert_scd2_table(df, table_name, unique_keys, conn):
    cursor = conn.cursor()

    for _, row in df.iterrows():
        conditions = " AND ".join([f"{key} = ?" for key in unique_keys])
        query_select = f"SELECT * FROM {table_name} WHERE {conditions} AND is_current = 1"
        cursor.execute(query_select, tuple(row[key] for key in unique_keys))
        existing_row = cursor.fetchone()

        if existing_row:
            cursor.execute(f"""
                UPDATE {table_name}
                SET is_current = 0, valid_to = ?
                WHERE {conditions} AND is_current = 1
            """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), *[row[key] for key in unique_keys]))

        row['created_at'] = row.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        row['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row['valid_from'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row['valid_to'] = None
        row['is_current'] = True

        cursor.execute(f"""
            INSERT INTO {table_name} ({", ".join(row.keys())})
            VALUES ({", ".join(["?" for _ in row])})
        """, tuple(row.tolist()))

# Funciones específicas para transformación
def transform_data_agents(**kwargs):
    df = pd.DataFrame(kwargs['ti'].xcom_pull(key='dataframe', task_ids='extract_data_by_csv'))
    df_agents = df[['holder_id', 'holder_name', 'holder_age', 'holder_phone_number', 
                    'holder_incomes', 'holder_address']].copy()
    kwargs['ti'].xcom_push(key='df_agents', value=df_agents.to_dict(orient='list'))

def load_data_agents(**kwargs):
    df_agents = pd.DataFrame(kwargs['ti'].xcom_pull(key='df_agents', task_ids='transform_data_agents'))
    with DatabaseConnection('sqlite_prueba') as conn:
        upsert_scd2_table(df_agents, 'agents', ['holder_id'], conn)

# Similar para pólizas y Bob
def transform_data_policies(**kwargs):
    df = pd.DataFrame(kwargs['ti'].xcom_pull(key='dataframe', task_ids='extract_data_by_csv'))
    df_policies = df[['policy_id', 'start_date', 'end_date', 'carrier_name']].copy()
    kwargs['ti'].xcom_push(key='df_policies', value=df_policies.to_dict(orient='list'))

def load_data_policies(**kwargs):
    df_policies = pd.DataFrame(kwargs['ti'].xcom_pull(key='df_policies', task_ids='transform_data_policies'))
    with DatabaseConnection('sqlite_prueba') as conn:
        upsert_scd2_table(df_policies, 'policies', ['policy_id'], conn)

def transform_data_bob(**kwargs):
    df = pd.DataFrame(kwargs['ti'].xcom_pull(key='dataframe', task_ids='extract_data_by_csv'))
    df_bob = df[['holder_id', 'policy_id', 'days_of_payment_delay', 'commission_amount', 'members']].copy()
    kwargs['ti'].xcom_push(key='df_bob', value=df_bob.to_dict(orient='list'))

def load_data_bob(**kwargs):
    df_bob = pd.DataFrame(kwargs['ti'].xcom_pull(key='df_bob', task_ids='transform_data_bob'))
    with DatabaseConnection('sqlite_prueba') as conn:
        upsert_scd2_table(df_bob, 'bob', ['holder_id', 'policy_id'], conn)

# DAG de Airflow
with DAG(
    'insurance_data_etl',
    description='ETL para datos de seguros',
    schedule_interval=None,
    start_date=datetime(2024, 11, 16),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data_by_csv',
        python_callable=extract_data_by_csv
    )

    transform_agents_task = PythonOperator(
        task_id='transform_data_agents',
        python_callable=transform_data_agents
    )

    load_agents_task = PythonOperator(
        task_id='load_data_agents',
        python_callable=load_data_agents
    )

    transform_policies_task = PythonOperator(
        task_id='transform_data_policies',
        python_callable=transform_data_policies
    )

    load_policies_task = PythonOperator(
        task_id='load_data_policies',
        python_callable=load_data_policies
    )

    transform_bob_task = PythonOperator(
        task_id='transform_data_bob',
        python_callable=transform_data_bob
    )

    load_bob_task = PythonOperator(
        task_id='load_data_bob',
        python_callable=load_data_bob
    )

    # Define el flujo de tareas
    extract_task >> [transform_agents_task, transform_policies_task, transform_bob_task]
    transform_agents_task >> load_agents_task
    transform_policies_task >> load_policies_task
    transform_bob_task >> load_bob_task
