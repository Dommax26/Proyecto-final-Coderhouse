from datetime import timedelta, datetime
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import smtplib
from email.mime.text import MIMEText

# Configuración de la conexión a Redshift usando variables de entorno
redshift_conn = {
    'host': os.getenv('REDSHIFT_HOST'),
    'username': os.getenv('REDSHIFT_USER'),
    'database': os.getenv('REDSHIFT_DB'),
    'port': '5439',
    'pwd': os.getenv('REDSHIFT_PASSWORD')
}

# Configuración de la conexión a PostgreSQL usando variables de entorno
postgres_conn = {
    'host': os.getenv('POSTGRES_HOST'),
    'username': os.getenv('POSTGRES_USER'),
    'database': os.getenv('POSTGRES_DB'),
    'port': '5432',
    'pwd': os.getenv('POSTGRES_PASSWORD')
}

# Definición del DAG
default_args = {
    'owner': 'Dommax',
    'start_date': datetime(2023, 5, 30),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='ETL_monedas',
    default_args=default_args,
    description='ETL para tasas de cambio de divisas',
    schedule_interval="@daily",
    catchup=False
)

# Función para crear la tabla en Redshift
def create_table_in_redshift():
    conn = None
    try:
        conn = psycopg2.connect(
            host=redshift_conn['host'],
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439'
        )
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tipos_cambio (
            fecha DATE,
            moneda_base VARCHAR(3),
            moneda1 DECIMAL(10, 4),
            moneda2 DECIMAL(10, 4),
            columna_extra1 VARCHAR(255),
            columna_extra2 DECIMAL(10, 4)
        );
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        
    except Exception as e:
        print(f"Error al crear la tabla en Redshift: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

# Función para adquirir datos de tasas de cambio desde una API
def fetch_exchange_rates(exec_date):
    try:
        print(f"Adquiriendo data para la fecha: {exec_date}")
        base_url = "https://api.exchangerate-api.com/v4/latest/"
        base_currency = "USD"
        url = base_url + base_currency
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            raise Exception(f"Error al obtener datos de la API: {response.status_code}")
    except Exception as e:
        print(f"Error en la extracción de datos: {e}")
        raise e

# Función para adquirir datos desde PostgreSQL
def fetch_postgres_data(exec_date):
    try:
        conn = psycopg2.connect(
            host=postgres_conn['host'],
            dbname=postgres_conn["database"],
            user=postgres_conn["username"],
            password=postgres_conn["pwd"],
            port='5432'
        )
        query = "SELECT * FROM tabla_relevante;"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error en la extracción de datos desde PostgreSQL: {e}")
        raise e

# Función para procesar los datos de las tasas de cambio y convertirlos en un DataFrame
def process_data(exec_date, api_data, db_data):
    base_currency = "USD"
    target_currencies = list(api_data["rates"].keys())
    base_rate = api_data["rates"].get(base_currency, 1)
    converted_rates = {currency: api_data["rates"][currency] / base_rate for currency in target_currencies}

    today = datetime.today().date()
    df_api = pd.DataFrame({
        "fecha": [today],
        "moneda_base": [base_currency],
        **converted_rates
    })

    # Combinar datos de API y base de datos
    df_combined = pd.merge(df_api, db_data, how='inner', on='alguna_columna_comun')
    return df_combined

# Función para almacenar los datos en una base de datos de Redshift
def store_data_in_db(df, exec_date):
    conn = None
    try:
        conn = psycopg2.connect(
            host=redshift_conn['host'],
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439'
        )
        cursor = conn.cursor()

        # Crear tabla si no existe
        columns = ", ".join([f"{currency} DECIMAL(10,4)" for currency in df.columns if currency not in ["fecha", "moneda_base"]])
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS tipos_cambio (
            fecha DATE,
            moneda_base VARCHAR(3),
            {columns}
        )
        """)

        # Insertar datos en la tabla
        insert_query = f"""
        INSERT INTO tipos_cambio (fecha, moneda_base, {', '.join(df.columns[2:])})
        VALUES %s
        """
        execute_values(cursor, insert_query, df.values)

        conn.commit()
    except Exception as e:
        print(f"Error al conectar o insertar en la base de datos: {e}")
        raise e
    finally:
        if conn:
            cursor.close()
            conn.close()

# Función para enviar alertas por email
def send_alert(exec_date, df):
    alert_value = 1.5  # Ejemplo de valor límite
    for index, row in df.iterrows():
        if row['algun_valor'] > alert_value:
            msg = MIMEText(f"Alerta: El valor de {row['moneda']} ha sobrepasado el límite: {row['algun_valor']}")
            msg['Subject'] = "Alerta de Valor de Moneda"
            msg['From'] = "tu_email@dominio.com"
            msg['To'] = "destinatario@dominio.com"

            with smtplib.SMTP('smtp.dominio.com') as server:
                server.login("tu_email@dominio.com", "tu_contraseña")
                server.sendmail(msg['From'], [msg['To']], msg.as_string())

# Función de extracción de datos para Airflow
def extract_task(exec_date):
    api_data = fetch_exchange_rates(exec_date)
    db_data = fetch_postgres_data(exec_date)
    df = process_data(exec_date, api_data, db_data)
    df.to_csv('/opt/airflow/processed_data/' + f"data_{datetime.today().date()}.csv", index=False)
    return df

# Función de carga de datos para Airflow
def load_task(exec_date):
    df = pd.read_csv('/opt/airflow/processed_data/' + f"data_{datetime.today().date()}.csv")
    store_data_in_db(df, exec_date)
    send_alert(exec_date, df)

# Definición de tareas en el DAG
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table_in_redshift,
    dag=BC_dag,
)

task_1 = PythonOperator(
    task_id='fetch_data',
    python_callable=extract_task,
    op_args=["{{ ds }}"],
    dag=BC_dag,
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    op_args=["{{ ds }}"],
    dag=BC_dag,
)

# Definición del orden de las tareas
create_table_task >> task_1 >> task_2
