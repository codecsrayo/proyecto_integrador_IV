from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
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


# Función principal
def execute_main(**context):
    try:
        from main import main

        logger.info("Starting main function execution")
        result = main()
        logger.info(f"Main function completed successfully: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise


# Tareas
start_task = EmptyOperator(
    task_id="start",
    dag=dag,
)

main_execution_task = PythonOperator(
    task_id="main_execution",
    python_callable=execute_main,
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
