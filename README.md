# Airflow Template with K8s

[[_TOC_]]


## What is this Repo?
This Airflow Template provides a universal DAG development framework. It helps developers quickly build DAGs equipped with standard features such as `KubernetesPodOperator`, `BranchPythonOperator`, notifications, and workspace cleanup.

## Directory Structure
- The repository is split into two main folders:
  - `dags`: Houses all Airflow DAGs and shared components.
  - `k8s-config`: Contains templates for Kubernetes configuration files.
```sh
Airflow-template-k8s/
├── dags/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── dag_template.py  # Universal build_dag() function to auto-generate DAGs from stages
│   │   ├── operators.py     # Custom core operators (e.g., EmailNotificationOperator)
│   │   ├── params.py        # Shared DAG parameter definitions and default values
│   │   ├── tasks.py         # Shared @task decorator Python functions (TaskFlow API)
│   │   └── utils.py         # Helper utilities (e.g., get_logger(), send_mail()...)
│   ├── dag_example.py       # A blueprint showcase using dag_template and common components
│   └── scripts/             # Task-specific Shell or Python scripts
└── k8s-config/              # Sample Kubernetes deployment configurations
    ├── airflow-data-pvc.yaml
    ├── daemonset-prepull-llamafactory.yaml
    ├── git-credentials.yaml
    ├── namespace.yaml
    ├── token.yaml
    └── values-1.20.0_airflow.yaml # Helm chart v1.20.0 config
```
### The `dags` Folder Components
1. `common/dag_template.py`

   Provides a universal initialization function `build_dag()`.
    
   - **Features**: Automatically structures tasks based on a `stages` list, natively supports TaskFlow API, triggers success/failure notifications, and appends final cleanup tasks.

      <details><summary>Example Usage</summary>
          
        ```py
        from dags.common.dag_template import build_dag
        from dags.common.operators import EmailNotificationOperator
        from dags.common.tasks import prepare_user_workspace, clear_user_workspace, mark_dag_failed
        
        stages = [
            {
                "operator": prepare_user_workspace,
                "operator_kwargs": {
                    "op_kwargs": {
                      "workspace_dirs": "data", 
                      "root_dir": "/workspace",               
                    },
                },
            },
            {
                "task_id": "train_model",
                "operator": KubernetesPodOperator,
                "operator_kwargs": {
                    "name": "train-model",
                    "namespace": "default",
                    "image": "my-docker-image",
                    "cmds": ["bash", "-c"],
                    "arguments": ["echo 'training...'"],
                },
                "dependencies": ["prepare_workspace"]
            }
        ]
        
        notify_tasks = {
            "success": EmailNotificationOperator(
                task_id="notify_success",
                success=True,
                trigger_rule="all_success",
            ),
            "failure": EmailNotificationOperator(
                task_id="notify_failure",
                success=False,
                trigger_rule="one_failed",
            ),
            "cleanup": clear_user_workspace(
                preserve_on_failure=False,
            ),
            "check_failure": mark_dag_failed()
        }
        
        dag = build_dag(
            dag_id="my_example_dag",
            default_args={"owner": "airflow"},
            stages=stages,
            notify_tasks=notify_tasks
        )
        ```
      
      </details>
---

2.  `common/operators.py`
      
    Houses reusable custom operators like `EmailNotificationOperator` which flags execution statuses automatically via email. Settings can be overridden using runtime configuration parameters.

---

3.  `common/params.py`
      
    Maintains dictionary schemas for parameters (`GIT`, `WANDB`, `MODEL`, `GPU`, `TRAIN`, `EMAIL`) to enforce unified naming conventions across DAGs.
      
    <details><summary>Example Usage</summary>
          
    ```python
    from common.params import MODEL, GIT, EMAIL

    dag_params = {
        **GIT,               # Default git settings
        **MODEL,             # Default model parameters
        **dag_extra_params,  # Custom specific arguments
        **EMAIL              # Notification rules
    }
    ```
          
    </details>

---

4.  `common/tasks.py`
      
    Defines pre-built Python functions wrapped in `@task` decorators ready for `PythonOperator` or `KubernetesPodOperator` architectures (e.g., `prepare_user_workspace()`, `validate_user_params()`, `fetch_data_from_git()`).

---

5.  `common/utils.py`
      
    Utility toolkit offering logging tools (`get_logger()` with console & file mirroring), backend dispatching mechanisms (`send_mail()`), and directory cleaner tasks.

---

6. `dag_example.py`
      
    An out-of-the-box demo DAG highlighting the assembly of workflow sequences.
      
    <details><summary>Example Workflow</summary>


    ```mermaid
    flowchart TD
        A[start] --> B["prepare_workspace_task (TaskFlow decorator)"]
        B --> C["choose_augmentation (BranchPythonOperator)"]
        C --> D["mock_augmentation_task (PythonOperator)"]
        C --> E["skip_augmentation (EmptyOperator)"]
        D & E --> F["merge_augmentation (EmptyOperator)"]
        F --> G["notify_success (EmailNotificationOperator)"]
        F --> H["notify_failure (EmailNotificationOperator)"]
        F --> I["mark_failed_task (TaskFlow decorator)"]
        G & H --> J["clear_workspace_task (TaskFlow decorator)"]
        J --> K[end]
    ```
        
    </details>


## Recommended DAG Development Workflow
### Prerequisite
1. Place your new DAG script into the `dags/` folder following the format: `dag_{dag_id}.py`.
2. Save any task-specific execution logic inside the `scripts/` subdirectory.

### Step-by-Step Implementation
 1. **Setup Arguments & Param Schemas**
 
    Define runtime parameters using the snake_case naming convention following the [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/3.1.8/core-concepts/params.html#use-params-to-provide-a-trigger-ui-form) 

    ```python
    from common.params import *
    from airflow.models.param import Param

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "start_date": datetime(2026, 1, 1),
        "catchup": False,
    }

    dag_extra_params = {
        "do_augmentation": Param(True, type="boolean", description="Whether to execute data generation"),
    }

    user_params = {
        **GIT, 
        **MODEL, 
        **dag_extra_params, 
        **EMAIL
    }
    ```
     
 2. **Global Level Configurations**
    
    Initialize global variables using UPPER_CASE notation.

    ```python
    NAMESPACE = conf.get("kubernetes", "namespace", fallback="default")
    IMAGE_PULL_SECRET = "regcred"
    ```
     
 3. **Task Logic Initialization**

    Import standard workflows or define internal custom hooks. Utilize the custom logger instance:

    ```python
    from common.utils import get_logger
    logger = get_logger()

    def write_dataset_info(dest_subdir: str = DATA_DIR, dataset_info_file: str = "dataset_info.json", **context):
        # Execution details...
        logger.info(f"{dataset_info_file} write operation successful")
    ```
      
 4. **Declare the Execution Stages**

    Construct your processing workflow through standard `dictionary` specifications within a stages block array.

    - **Key Properties Required Per Stage**:
        - `task_id`: Unique identifier string. (Omit if initialized internally inside `@task(task_id=...)` specs).
        - `operator`: Target class module identifier (e.g., `PythonOperator`, `KubernetesPodOperator`).
        - `operator_kwargs`: Inner specific settings block (e.g., `python_callable`, `op_kwargs`).
        - `dependencies`: List arrays of dependent `task_id` blocks defining execution priority.

    - Using general python function
        ```python
        from airflow.operators.python import PythonOperator
        from scripts.funcs import validate_user_params
        {
            "task_id": "validate_params_task",
            "operator": PythonOperator,
            "operator_kwargs": {
                "python_callable": validate_user_params,
                "op_kwargs": {
                    "validation_map": {
                        ...
                    }
                },
            },
            "dependencies": ["prepare_workspace_task"]
        },
        ```
    - Using @task Decorator python function
        ```python
        from common.tasks import validate_user_params
        {
            "operator": validate_user_params,
            "operator_kwargs": {
                "op_kwargs": {
                    "validation_map": {
                        ...
                    }
                },
            },
            "dependencies": ["prepare_workspace_task"]
        },
        ```
         
    <details><summary>Example: Stages executed sequentially</summary>

    - `prepare_workspace_task >> validate_params_task >> fetch_data_task`
    - For the second stage onwards, enter the task_id from the previous stage in the dependencies field.
        
        ```python
        {
            "operator": prepare_user_workspace,
            "operator_kwargs": {
                "op_kwargs": {
                    "workspace_dirs": "data", 
                    "root_dir": "/workspace",               
                },
            },
        },
        {
            "operator": validate_user_params,
            "operator_kwargs": {
                "op_kwargs": {
                    "validation_map": {
                        "wandb": {
                            "report_to": "report_to_wandb", 
                            "wandb_key": "wandb_api_key"
                        },
                        "mlflow": {
                            "report_to": "report_to_mlflow", 
                            "uri": "mlflow_tracking_uri", 
                            "username": "mlflow_tracking_username", 
                            "password": "mlflow_tracking_password", 
                            "experiment_name": "mlflow_experiment_name"
                        },
                        "huggingface": {
                            "model": "hf_model_path",  
                            "token": "hf_token",  
                        },
                        "git": {
                            "repo": "git_repo_url",   
                            "token": "git_token",  
                        },
                    },
                    "mlflow_default_experiment_name": "llamafactory-ft",
                },
            },
            "dependencies": ["prepare_workspace_task"]
        },
        {
            "operator": fetch_data_from_git,
            "operator_kwargs": {
                "execution_timeout": timedelta(minutes=20),
                "op_kwargs": {
                    "repo_url": "{{ params.git_repo_url }}",
                    "git_token": "{{ params.git_token }}",
                    "data_paths": "{{ params.data_path }}",
                    "dest_subdir": DATA_DIR
                },
            },
            "dependencies": ["validate_params_task"],
        },
        ```
         
    </details>

    <details><summary>Example: Stages executed synchronously</summary>

    - `fetch_data_task >> [prepare_ds_task, write_datainfo_task] >> end`
    - Entering the same task_id in dependencies indicates synchronous execution.
        
        ```python
        {
            "operator": write_ds_config,
            "dependencies": ["fetch_data_task"],
        },
        {
            "operator": write_dataset_info,
            "operator_kwargs": {
                "op_kwargs": {
                    "dest_subdir": DATA_DIR, 
                    "dataset_info_file": "dataset_info.json",               
                },
            },
            "dependencies": ["fetch_data_task"]
        },
        {
            "task_id": "end",
            "operator": EmptyOperator,
            "dependencies": ["prepare_ds_task", "write_datainfo_task"],
        },
        ```
         
    </details>

    <details><summary>Example: Branches determine which stage to execute next.</summary>

    - `choose_augmentation >> [augmentation_task, skip_augmentation] >> merge_augmentation`
    - After branching, a merge stage should be added to ensure that subsequent triggers can correctly notify success or failure.
         
        ```python
        def branch_data_augmentation(**context):
            return "augmentation_task" if context["params"].get("do_augmentation", True) else "skip_augmentation"
        
        stages = [
            {
                "task_id": "choose_augmentation",
                "operator": BranchPythonOperator,
                "operator_kwargs": {
                    "python_callable": branch_data_augmentation,
                },
            },
            {
                "task_id": "augmentation_task",
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
        
        dag = build_dag(
            dag_id="dag_example",
            default_args=default_args,
            schedule=None,
            params=dag_params,
            stages=stages,
        )
        ```

    </details>

 5. **Handle Finalizing & Cleanup Steps**
    
    ```python
    from common.operators import EmailNotificationOperator
    from common.tasks import clear_user_workspace, mark_dag_failed
    
    notify_tasks = {
        "success": EmailNotificationOperator(
            task_id="notify_success_task",
            success=True,
            trigger_rule="all_success",
        ),
        "failure": EmailNotificationOperator(
            task_id="notify_failure_task",
            success=False,
            trigger_rule="one_failed",
        ),
        "cleanup": clear_user_workspace(
            preserve_on_failure=False,
            upload_task_id="upload_model_task"
        ),
        "check_failure": mark_dag_failed()   
    }
    ```
     
 6. **Instantiating via `build_dag()`**

    ```python
    from dag_template import build_dag

    dag = build_dag(
        dag_id="dag_example",
        default_args=default_args,
        schedule=None,
        params=dag_params,
        stages=stages,
        notify_tasks=notify_tasks,
    )
    ```

## Deploying on Kubernetes
### Prerequisite

[airflow/helm-chart/1.20.0](https://airflow.apache.org/docs/helm-chart/1.20.0/index.html)
- Kubernetes 1.30+ cluster
- Helm 3.10+
    ```sh
    # install helm
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    # add Airflow helm repository
    helm repo add apache-airflow https://airflow.apache.org
    helm repo update
    ```

</details>

### Initial Setup & Execution Sequence
1. **clone repository**
    
    ```sh
    git clone https://github.com/yguo14542/Airflow-template-k8s.git
    cd Airflow-template-k8s/
    ```
    
2. **Configure and Set Target Namespace**
   
   Update the target configuration block inside `k8s-config/namespace.yaml`.
    
   ```sh
   kubectl apply -f k8s-config/namespace.yaml
   kubectl config set-context --current --namespace=<namespace-name>
   ```
    
3. **Provision Git Integration Credentials**

   Set valid access details in `k8s-config/git-credentials.yaml`.
   
   ```sh
   kubectl apply -f k8s-config/git-credentials.yaml
   ```
    
4. **Register Container Repository Credentials**
    
    ```sh
    kubectl create secret docker-registry regcred \
        --docker-server=<your-docker-server> \
        --docker-username='<your-docker-username>' \
        --docker-password='<your-docker-password>' \
        --dry-run=client -o yaml > k8s-config/regcred.yaml
    
    kubectl apply -f k8s-config/regcred.yaml
    ```
    
5. **Initialize Persistent Storage PVC**
   
   Define target storage size criteria within `k8s-config/airflow-data-pvc.yaml`.
    
   ```sh
   kubectl apply -f k8s-config/airflow-data-pvc.yaml
   ```
    
6. **(Optional) Bind Mailing System Secrets**
 
   Acquire access permissions via the mail-relay token platform. Apply parameters to `k8s-config/token.yaml`.
    
    ```sh
    kubectl apply -f k8s-config/token.yaml
    # update MAIL_API_URL in k8s-config/values-1.20.0_airflow.yaml
    ```
    
7. **(Optional) Pre-Pull Heavy Workload Images**

    ```sh
    kubectl apply -f k8s-config/daemonset-prepull-image.yaml
    ```
    
8. **Deploy the Architecture via Helm Chart Integration**
    
    ```sh
    helm install airflow apache-airflow/airflow --values k8s-config/values-1.20.0_airflow.yaml --debug
    # For post-deployment schema upgrades:
    helm upgrade airflow apache-airflow/airflow --values k8s-config/values-1.20.0_airflow.yaml --debug
    ```
    
9. **Launch the Web UI Dashboard Interface**

   ```sh
   kubectl port-forward svc/airflow-api-server 8080:8080
   ```

   Open your browser and navigate to `http://localhost:8080/` to test your new DAG pipeline configuration.