from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Constants
OWNER = 'main_demo'
DAG_NAME = 'main_dag'
GROUP = ['main_tasks']

# Default arguments
default_args = {
    "owner": OWNER,
    "start_date": datetime(2025, 1, 1),
}

# DAG definition
dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval="@daily",
    # schedule_interval=None,  # Use this for manual triggers
    catchup=False,
    tags=GROUP,
)

# Task functions
def execute_main(**kwargs):
    """Execute the main function with proper error handling"""
    try:
        from main import main
        logger.info("Starting main function execution")
        result = main()
        logger.info(f"Main function completed successfully: {result}")
        return result
    except ImportError as e:
        logger.error(f"Failed to import main function: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error executing main function: {str(e)}")
        raise

def pre_execution_check(**kwargs):
    """Pre-execution validation"""
    try:
        logger.info("Performing pre-execution checks")
        # Add any validation logic here
        logger.info("Pre-execution checks completed successfully")
        return True
    except Exception as e:
        logger.error(f"Pre-execution check failed: {str(e)}")
        raise

def post_execution_cleanup(**kwargs):
    """Post-execution cleanup tasks"""
    try:
        logger.info("Performing post-execution cleanup")
        # Add any cleanup logic here
        logger.info("Post-execution cleanup completed successfully")
        return True
    except Exception as e:
        logger.error(f"Post-execution cleanup failed: {str(e)}")
        raise

# Task definitions
start_task = DummyOperator(
    task_id="start_task",
    dag=dag,
)

pre_check_task = PythonOperator(
    task_id="pre_execution_check",
    python_callable=pre_execution_check,
    provide_context=True,
    dag=dag,
)

main_execution_task = PythonOperator(
    task_id="main_execution",
    python_callable=execute_main,
    provide_context=True,
    dag=dag,
)

post_cleanup_task = PythonOperator(
    task_id="post_execution_cleanup",
    python_callable=post_execution_cleanup,
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(
    task_id="end_task",
    dag=dag,
)

# Task dependencies
start_task >> pre_check_task >> main_execution_task >> post_cleanup_task >> end_task

# Export DAG for Airflow discovery
init_dag = dag