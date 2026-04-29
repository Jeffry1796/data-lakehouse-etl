"""
Airflow DAG for NYC Taxi Dataset Ingestion
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from ingestion.ingest_dataset import NYCTaxiIngestion

# ============ Default Arguments ============
default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

# ============ DAG Definition ============
dag = DAG(
    'nyc_taxi_etl',
    default_args=default_args,
    schedule_interval='0 2 1 * *',
    catchup=False,
    tags=['data-ingestion', 'gcs', 'taxi-data'],
)


# ============ Task Functions ============
def ingest_taxi_data(**context):
    """
    NYC Taxi Dataset Ingestion Task
    Reads configuration from Airflow Variables and runs the ingestion process
    """

    project_id = Variable.get('GCP_PROJECT_ID', default_var='your-gcp-project-id')
    bucket_name = Variable.get('GCS_BUCKET_NAME', default_var='data-project-bronze')
    credentials_path = Variable.get('GCP_CREDENTIALS_PATH', default_var=None)

    taxi_types = Variable.get('TAXI_TYPES', default_var='yellow,green').split(',')
    taxi_types = [t.strip().lower() for t in taxi_types]

    start_year = Variable.get('INGESTION_START_YEAR', default_var=2024)
    start_month = Variable.get('INGESTION_START_MONTH', default_var=1)
    end_year = Variable.get('INGESTION_END_YEAR', default_var=2024)
    end_month = Variable.get('INGESTION_END_MONTH', default_var=2)
    include_zones = Variable.get('INCLUDE_TAXI_ZONES', default_var=True)

    start_year = int(start_year) if isinstance(start_year, str) else start_year
    start_month = int(start_month) if isinstance(start_month, str) else start_month
    end_year = int(end_year) if isinstance(end_year, str) else end_year
    end_month = int(end_month) if isinstance(end_month, str) else end_month
    include_zones = bool(include_zones) if isinstance(include_zones, str) else include_zones

    if start_year > end_year:
        raise ValueError("start-year must be <= end-year")
    if start_year == end_year and start_month > end_month:
        raise ValueError("start-month must be <= end-month when start-year == end-year")

    print(f"=== NYC Taxi Ingestion Configuration ===")
    print(f"Project ID: {project_id}")
    print(f"Bucket Name: {bucket_name}")
    print(f"Taxi Types: {taxi_types}")
    print(f"Date Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
    print(f"Include Zones: {include_zones}")
    print("=" * 40)

    # Initialize ingestion
    ingestion = NYCTaxiIngestion(
        project_id=project_id,
        bucket_name=bucket_name,
        gcs_credentials_path=credentials_path
    )

    # Run ingestion
    uploaded_files = ingestion.ingest_all(
        taxi_types=taxi_types,
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
        include_zones=include_zones
    )

    return {
        'status': 'success',
        'files_count': len(uploaded_files),
        'uploaded_files': uploaded_files
    }

# ============ Task Definitions ============
with dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt deps --profiles-dir . --project-dir ."
        ),
    )

    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_taxi_data,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    dbt_test_source = BashOperator(
        task_id="dbt_test_source",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --select source:* --target dev"
        ),
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select path:models/silver --target dev"
        ),
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --select path:models/silver --target dev"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select path:models/gold --target dev"
        ),
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --select path:models/gold --target dev"
        ),
    )

    # ============ Task Order ============
    dbt_deps >> ingest_task >> dbt_test_source >> dbt_run_staging >> dbt_test_staging >> dbt_run_marts >> dbt_test_marts
