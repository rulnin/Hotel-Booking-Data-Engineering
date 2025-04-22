from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


from datetime import datetime
import pandas as pd

# === Config ===
CSV_PATH = '/opt/airflow/dags/data/hotel_bookings.csv'

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='hotel_etl_dag',
    default_args=default_args,
    schedule_interval=None,
    description='ETL hotel bookings into Postgres',
    tags=['hotel', 'etl'],
) as dag:

    # === TASK 1: CREATE TABLE ===
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS hotel_bookings (
        hotel TEXT,
        is_canceled INTEGER,
        lead_time INTEGER,
        arrival_date_year INTEGER,
        arrival_date_month TEXT,
        arrival_date_week_number INTEGER,
        arrival_date_day_of_month INTEGER,
        stays_in_weekend_nights INTEGER,
        stays_in_week_nights INTEGER,
        adults INTEGER,
        children REAL,
        babies INTEGER,
        meal TEXT,
        country TEXT,
        market_segment TEXT,
        distribution_channel TEXT,
        is_repeated_guest INTEGER,
        previous_cancellations INTEGER,
        previous_bookings_not_canceled INTEGER,
        reserved_room_type TEXT,
        assigned_room_type TEXT,
        booking_changes INTEGER,
        deposit_type TEXT,
        days_in_waiting_list INTEGER,
        customer_type TEXT,
        adr REAL,
        required_car_parking_spaces INTEGER,
        total_of_special_requests INTEGER,
        reservation_status TEXT,
        reservation_status_date DATE
        );
        """
    )

    # === TASK 2: FETCH DATA (Read CSV) ===
    def fetch_data(**kwargs):
        df = pd.read_csv(CSV_PATH)
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data
    )

    # === TASK 3: TRANSFORM DATA ===
    def transform_data(**kwargs):
        raw_json = kwargs['ti'].xcom_pull(task_ids='fetch_data', key='raw_data')
        df = pd.read_json(raw_json)

        # Remove duplicates
        df.drop_duplicates(inplace=True)

        # Drop unneeded columns
        df.drop(['company','agent'], axis =1, inplace = True)

        # Handle missing values
        df.dropna(inplace =True)

        # Return processed dataframe
        kwargs['ti'].xcom_push(key='transformed_data', value=df.to_json(orient='records'))

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    # === TASK 4: INSERT DATA INTO POSTGRES ===
    def insert_data(**kwargs):
        json_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')
        df = pd.read_json(json_data)

        pg_hook = PostgresHook(postgres_conn_id='postgres')
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql('hotel_bookings', con=engine, if_exists='append', index=False)

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data
    )

    # === DAG TASK ORDER ===
    create_table >> fetch_data >> transform_data >> insert_data