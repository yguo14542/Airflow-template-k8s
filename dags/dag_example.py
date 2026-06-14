from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
from common.operators import EmailNotificationOperator
from common.dag_template import build_dag
from common.utils import get_logger
from common.tasks import *
from common.params import *

logger = get_logger()


# -------------------------------
# Global Variables
# -------------------------------
DATA_DIR = "data"

# -------------------------------
# DAG default params
# -------------------------------
default_args = {
    "owner": "airflow",                     
    "depends_on_past": False,               
    "email_on_failure": False,       
    "email_on_retry": False,     
    "start_date": datetime(2026, 1, 1),   
    "catchup": False,                   
}

dag_extra_params = {
    "do_augmentation": True,
}

user_params = {
    **GIT,           # common.params.GIT default params
    **MODEL,         # common.params.MODEL default params
    **dag_extra_params,  # Custom additional parameters
    **EMAIL          # common.params.EMAIL default params
}

# -------------------------------
# DAG-specific Python callables
# -------------------------------
def mock_augmentation(**context):
    logger.info("Execution data generation...")
    import time; time.sleep(10) 
    logger.info("Data generation complete!")

# -------------------------------
# Branching Functions
# -------------------------------
def branch_data_augmentation(**context):
    if context["params"].get("do_augmentation", True):
        return "mock_augmentation_task"
    else: 
        raise AirflowFailException(f"Test Failed")

# -------------------------------
# Define stages
# -------------------------------
stages = [
    {
        "operator": prepare_user_workspace,
        "operator_kwargs": {
            "op_kwargs": {
                "workspace_dirs": [DATA_DIR], 
                "root_dir": "/workspace",               
            },
        },
    },
    {
        "task_id": "choose_augmentation",
        "operator": BranchPythonOperator,
        "operator_kwargs": {
            "python_callable": branch_data_augmentation,
        },
        "dependencies": ["prepare_workspace_task"],
    },
    {
        "task_id": "mock_augmentation_task",
        "operator": PythonOperator,
        "operator_kwargs": {
            "python_callable": mock_augmentation,
        },
        "dependencies": ["choose_augmentation"],
    },
    {
        "task_id": "skip_augmentation",
        "operator": EmptyOperator,
        "dependencies": ["choose_augmentation"],
    },
    {
        "task_id": "merge_augmentation",
        "operator": EmptyOperator,
        "operator_kwargs": {
            "trigger_rule": "none_failed_min_one_success",
        },
        "dependencies": ["mock_augmentation_task", "skip_augmentation"],
    },
]
# -------------------------------
# Notification & Cleanup Tasks
# -------------------------------
notify_tasks = {
    "success": EmailNotificationOperator(
        task_id="notify_success_task",
        success=True,
        task="TEST-DAG",
        trigger_rule="all_success",
    ),
    "failure": EmailNotificationOperator(
        task_id="notify_failure_task",
        success=False,
        task="TEST-DAG",
        trigger_rule="one_failed",
    ),
    "cleanup": clear_user_workspace(
        preserve_on_failure=False,
    ),
    "check_failure": mark_dag_failed()   # If any previous stage fails, the entire DAG is marked as failed.
}

# -------------------------------
# Build DAG
# -------------------------------
example_dag = build_dag(
    dag_id="dag_example",
    default_args=default_args,
    schedule=None,
    params=user_params,
    stages=stages,
    notify_tasks=notify_tasks,
)