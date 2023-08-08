from datetime import datetime, timedelta  
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'adedayo.okubanjo',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    dag_id='install_dbt_snowflake',
    default_args=default_args,
    description='This dag installs dbt-snowflake on the server.',
    start_date=datetime(2022, 12, 11),
    catchup=False,
    schedule_interval='@daily'
) as dag:

    install_dbt_snowflake = BashOperator(
        task_id='install_dbt_snowflake',
        bash_command="pip install dbt-snowflake"
    )

    install_dbt_snowflake
