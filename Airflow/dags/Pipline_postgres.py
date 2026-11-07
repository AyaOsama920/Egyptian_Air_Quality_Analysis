from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# ---------------------------
# Default Arguments
# ---------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['alaaosamamohammed15@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ---------------------------
# DAG Definition
# ---------------------------
dag = DAG(
    'etl_csv_to_postgres',
    default_args=default_args,
    description='ETL pipeline: CSV → Clean → PostgreSQL → Email Notification',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'postgres', 'cleaning'],
)

# ---------------------------
# Task 1: Extract CSV
# ---------------------------
def extract_data(**kwargs):
    """Read data from CSV file"""
    csv_path = '/opt/airflow/data/shopping_trends_updated.csv'
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    df = pd.read_csv(csv_path)
    print(f"Extracted {len(df)} rows from CSV.")
    print(f"Columns: {list(df.columns)}")
    print(f" Shape: {df.shape}")
    
    # Save to temp file
    df.to_csv('/tmp/extracted_data.csv', index=False)
    return len(df)

# ---------------------------
# Task 2: Transform (Clean)
# ---------------------------
def transform_data(**kwargs):
    """Clean data: remove nulls and duplicates"""
    df = pd.read_csv('/tmp/extracted_data.csv')
    original_count = len(df)
    
    print(f" Original data shape: {df.shape}")
    print(f" Null values per column:")
    print(df.isnull().sum())
    print(f"Duplicates found: {df.duplicated().sum()}")
    
    # Remove null values
    df_cleaned = df.dropna()
    after_null_removal = len(df_cleaned)
    
    # Remove duplicate rows
    df_cleaned = df_cleaned.drop_duplicates()
    final_count = len(df_cleaned)
    
    print(f"\nCleaned data shape: {df_cleaned.shape}")
    print(f" Removed due to nulls: {original_count - after_null_removal}")
    print(f" Removed due to duplicates: {after_null_removal - final_count}")
    print(f" Total removed rows: {original_count - final_count}")
    
    # Save cleaned data
    df_cleaned.to_csv('/tmp/cleaned_data.csv', index=False)
    
    # Push statistics to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='original_rows', value=original_count)
    ti.xcom_push(key='cleaned_rows', value=final_count)
    ti.xcom_push(key='removed_rows', value=original_count - final_count)
    ti.xcom_push(key='null_removed', value=original_count - after_null_removal)
    ti.xcom_push(key='duplicates_removed', value=after_null_removal - final_count)
    
    return final_count

# ---------------------------
# Task 3: Load to PostgreSQL
# ---------------------------
def load_data(**kwargs):
    """Load cleaned data into PostgreSQL"""
    df = pd.read_csv('/tmp/cleaned_data.csv')
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Load data to PostgreSQL
    table_name = 'cleaned_shopping_data'
    df.to_sql(
        table_name,
        engine,
        if_exists='replace',
        index=False
    )
    
    print(f"Successfully loaded {len(df)} rows into PostgreSQL table: {table_name}")
    
    # Push table name to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='table_name', value=table_name)
    
    return len(df)

# ---------------------------
# Task 4: Prepare Email Body
# ---------------------------
def prepare_email(**kwargs):
    """Prepare HTML content for email notification"""
    ti = kwargs['ti']
    
    # Pull statistics from XCom
    original = ti.xcom_pull(key='original_rows', task_ids='transform_task')
    cleaned = ti.xcom_pull(key='cleaned_rows', task_ids='transform_task')
    removed = ti.xcom_pull(key='removed_rows', task_ids='transform_task')
    null_removed = ti.xcom_pull(key='null_removed', task_ids='transform_task')
    dup_removed = ti.xcom_pull(key='duplicates_removed', task_ids='transform_task')
    table_name = ti.xcom_pull(key='table_name', task_ids='load_task')
    
    # Calculate percentage
    clean_percentage = (cleaned / original * 100) if original > 0 else 0
    
    # Create HTML email content
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            h2 {{ color: #2E7D32; }}
            h3 {{ color: #1976D2; }}
            .success {{ color: #2E7D32; font-weight: bold; }}
            .stats {{ background-color: #F5F5F5; padding: 15px; border-radius: 5px; }}
            ul {{ line-height: 1.8; }}
            .footer {{ margin-top: 20px; color: #666; font-size: 12px; }}
        </style>
    </head>
    <body>
        <h2> ETL Pipeline Completed Successfully!</h2>
        <p><strong>Execution Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>DAG:</strong> etl_csv_to_postgres</p>

        <div class="stats">
            <h3> ETL Statistics</h3>
            <ul>
                <li><strong>Original Rows:</strong> {original:,}</li>
                <li><strong>Cleaned Rows:</strong> {cleaned:,}</li>
                <li><strong>Removed Rows:</strong> {removed:,}</li>
                <li style="margin-left: 20px;">↳ Due to Nulls: {null_removed:,}</li>
                <li style="margin-left: 20px;">↳ Due to Duplicates: {dup_removed:,}</li>
                <li><strong>Data Quality:</strong> <span class="success">{clean_percentage:.2f}%</span></li>
            </ul>
        </div>

        <h3> Database Information</h3>
        <ul>
            <li><strong>Table Name:</strong> {table_name}</li>
            <li><strong>Database:</strong> PostgreSQL (airflow)</li>
            <li><strong>Port:</strong> 5440</li>
        </ul>

        <p class="success"> The cleaned dataset was successfully loaded into PostgreSQL!</p>
        
        <div class="footer">
            <p>This is an automated message from Apache Airflow.</p>
            <p>Pipeline: ETL CSV to PostgreSQL</p>
        </div>
    </body>
    </html>
    """

    return html_content

# ---------------------------
# Define Tasks
# ---------------------------
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    dag=dag,
)

prepare_email_task = PythonOperator(
    task_id='prepare_email_task',
    python_callable=prepare_email,
    dag=dag,
)

email_task = EmailOperator(
    task_id='send_email',
    to='alaaosamamohammed15@gmail.com',
    subject='Airflow ETL Pipeline - Success Report',
    html_content="{{ task_instance.xcom_pull(task_ids='prepare_email_task') }}",
    dag=dag,
)

# ---------------------------
# Task Dependencies
# ---------------------------
extract_task >> transform_task >> load_task >> prepare_email_task >> email_task