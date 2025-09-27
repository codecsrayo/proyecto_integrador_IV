from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Constantes
OWNER = "main_demo"
DAG_NAME = "main_dag"
GROUP = ["main_tasks"]

# Default args
default_args = {
    "owner": OWNER,
    "start_date": datetime(2025, 1, 1),
}

# DAG
dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=GROUP,
)

# Tareas
start_task = EmptyOperator(
    task_id="start",
    dag=dag,
)

main_execution_task = BashOperator(
    task_id="main_execution",
    bash_command="cd /opt/airflow/dags/ && uv run python main.py",
    dag=dag,
)

end_task = EmptyOperator(
    task_id="end",
    dag=dag,
)

# Dependencias
start_task >> main_execution_task >> end_task

# Exportar DAG
init_dag = dag
