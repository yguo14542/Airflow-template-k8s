from __future__ import annotations

from airflow.sdk import dag
from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from common.utils import get_logger, airflow_callback_to_portal

logger = get_logger()

def build_dag(
    dag_id: str,
    default_args: dict,
    schedule: str = None,
    params: dict = None,
    stages: list = None,  
    notify_tasks: dict = None,  
    render_template_as_native_obj: bool = False,
) -> DAG:
    """
    This is a shared DAG template function. It returns a standardized Airflow DAG instance.
    
    Args:
        dag_id: The unique ID of a DAG.
        default_args: A dictionary containing the core parameters of the DAG.
        schedule: Scheduling of DAG.
        params: DAG user parameter dictionary.
        stages: The list of stages in the DAG includes operator, operator_kwargs, task_id, and dependencies.
        notify_tasks: A dictionary containing terms such as success, failure, cleanup, and check_failure indicating the end of the task/please clean up.
    """
    
    @dag(
        dag_id=dag_id,
        default_args=default_args,
        schedule=schedule,
        start_date=datetime(2025, 1, 1),
        params=params,
        catchup=False,
        render_template_as_native_obj=render_template_as_native_obj,
        on_success_callback=airflow_callback_to_portal,
        on_failure_callback=airflow_callback_to_portal,
    )
    def BasePipeline():
        task_dict = {}

        # Use EmptyOperator as a node in TaskFlow.
        start_node = EmptyOperator(task_id="start")
        end_node = EmptyOperator(task_id="end")

        prev_task = start_node

        for stage in stages:
            operator_cls = stage.get("operator")
            operator_kwargs = stage.get("operator_kwargs", {})
            stage_id = stage.get("task_id")

            # Process decorated operator (TaskFlow)
            if callable(operator_cls) and hasattr(operator_cls, "__wrapped__"):
                op_kwargs = operator_kwargs.get("op_kwargs", {})
                task = operator_cls(**op_kwargs)
                # get operator-level kwargs
                operator_level_args = {
                    k: v for k, v in operator_kwargs.items() if k != "op_kwargs"
                }
                # Apply to operator objects
                for k, v in operator_level_args.items():
                    setattr(task.operator, k, v)
                resolved_task_id = task.operator.task_id
                stage["task_id"] = resolved_task_id
            # general operator case
            else:
                task = operator_cls(
                    task_id=stage_id,
                    **operator_kwargs
                )
                resolved_task_id = stage_id

            task_dict[resolved_task_id] = task

            # Set dependencies
            deps = stage.get("dependencies")
            if deps:
                for dep in deps:
                    task_dict[dep] >> task
            else:
                prev_task >> task

            prev_task = task

        # Success/Failure Notification & cleanup
        if notify_tasks:
            success_task = notify_tasks.get("success")
            failure_task = notify_tasks.get("failure")
            cleanup_task = notify_tasks.get("cleanup")
            check_failure_task = notify_tasks.get("check_failure")

            if check_failure_task:
                prev_task >> check_failure_task
            if success_task:
                prev_task >> success_task
            if failure_task:
                prev_task >> failure_task
            if cleanup_task:
                if success_task: success_task >> cleanup_task
                if failure_task: failure_task >> cleanup_task

        if notify_tasks and cleanup_task:
            cleanup_task >> end_node
        else:
            prev_task >> end_node
    
    return BasePipeline()