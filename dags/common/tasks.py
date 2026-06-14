from __future__ import annotations

import os
import shutil, tempfile, subprocess
from typing import Dict, List, Optional, Union
from airflow.sdk import task
from airflow.exceptions import AirflowFailException
from common.utils import *
from airflow.utils.state import State
from airflow.models.taskinstance import TaskInstance


logger = get_logger()

# ==========================
# 初始化 User Workspace
# ==========================
@task(task_id="prepare_workspace_task")
def prepare_user_workspace(
    workspace_dirs: List[str] = None, 
    root_dir: str = "/workspace", 
    **context
) -> str:
    """
    通用 workspace 初始化函式。
    初始化 user workspace = root_dir/{run_id}/
    並在 user workspace 下建立 workspace_dirs 中指定的資料夾。
    回傳：work_dir (會自動作為 XCom Key='return_value' 推送)
    """
    run_id = context['run_id'].replace(":", "-")
    work_dir = os.path.join(root_dir, run_id)

    try:
        init_workspace(work_dir)
        
        if workspace_dirs is None:
            workspace_dirs = [] # 預設不建立任何子資料夾
            
        # 建立子資料夾
        for d in workspace_dirs:
            dir_path = os.path.join(work_dir, d)
            init_workspace(dir_path)
            logger.info(f"Initialized folder: {dir_path}")

        logger.info(f"User workspace prepared: {work_dir}")
        
        # 使用 return 自動推送 XCom
        return work_dir 

    except Exception as e:
        logger.error(f"Failed to prepare workspace: {e}")
        raise AirflowFailException(f"Failed to prepare workspace: {e}")

# ==========================
# 使用者參數驗證
# ==========================
@task(task_id="validate_params_task")
def validate_user_params(validation_map: Dict, mlflow_default_experiment_name: str = None, **context):
    """
    通用參數驗證函式(Wandb, MLflow, Hugging Face, Git)。

    op_kwargs 可傳入 mapping。

    範例：
        validation_map = {
            "wandb": {"report_to": "report_to_wandb", "wandb_key": "wandb_api_key"},
            "mlflow": {"report_to": "report_to_mlflow", "uri": "mlflow_tracking_uri", "username": "mlflow_tracking_username", "password": "mlflow_tracking_password", "experiment_name": "mlflow_experiment_name"},
            "huggingface": {"model": "hf_model", "token": "hf_token"},
            "git": {"repo": "repo_url", "token": "git_token"}
        }
        mlflow_default_experiment_name: 設定預設 mlflow experiment name
    """
    params = context.get("params", {})
    # --- Wandb ---
    # 透過 xcom_push 參數預設關閉 report_to_wandb，後續透過ti.xcom_pull(key='report_to_wandb')決定是否啟用
    context['ti'].xcom_push(key='report_to_wandb', value=False)
    wandb_map = validation_map.get("wandb", {})
    report_to_wandb_name  = wandb_map.get("report_to")
    wandb_key_name = wandb_map.get("wandb_key")
    if not report_to_wandb_name:
        logger.warning(f"{wandb_map} Wandb 配置缺少 'report_to' 鍵，跳過 Wandb 驗證")
    elif params.get(report_to_wandb_name, False):  # 只有使用者開啟report_to_wandb才會執行驗證
        if not wandb_key_name:
            logger.warning(f"{wandb_map} params 缺少 'wandb_key' 鍵，跳過 Wandb 驗證")
        if wandb_key_name:
            wandb_key = params.get(wandb_key_name) # 取得使用者輸入wandb_api_key
            if wandb_key:
                if validate_wandb_API(wandb_key):
                    # 只有驗證成功才會啟用
                    context['ti'].xcom_push(key='report_to_wandb', value=True)
                    logger.info("Wandb 參數驗證完成")
            else:
                logger.warning(f"已啟用 Wandb 追蹤，但 Wandb Key 欄位為空，停用 Wandb")
    else:
        logger.info(f"未啟用 Wandb 追蹤，跳過 Wandb 驗證")

    # --- MLflow ---
    # 透過 xcom_push 參數預設關閉 report_to_mlflow，後續透過ti.xcom_pull(key='report_to_mlflow')決定是否啟用
    context['ti'].xcom_push(key='report_to_mlflow', value=False)
    mlflow_map = validation_map.get("mlflow", {})
    report_to_mlflow_key = mlflow_map.get("report_to")
    uri_key = mlflow_map.get("uri")
    username_key = mlflow_map.get("username")
    password_key = mlflow_map.get("password")
    experiment_name_key = mlflow_map.get("experiment_name")
    if not report_to_mlflow_key:
        logger.warning(f"{mlflow_map} MLflow 配置缺少 'report_to' 鍵，跳過 MLflow 驗證")
    elif params.get(report_to_mlflow_key, False): # 只有使用者開啟report_to_mlflow才會執行驗證
        if not uri_key:
            logger.warning(f"{mlflow_map} MLflow 配置缺少 'uri' 鍵，跳過 MLflow 驗證")
        mlflow_uri = params.get(uri_key) # 取得使用者輸入mlflow uri
        if mlflow_uri:
            uri_val = params.get(uri_key)
            username_val = params.get(username_key)
            password_val = params.get(password_key)
            experiment_name_val = params.get(experiment_name_key)
            if not experiment_name_val:
                # 如果使用者沒有設定experiment_name，預設使用"llamafactory"
                experiment_name_val = mlflow_default_experiment_name if mlflow_default_experiment_name else "Default"
            # 執行連線驗證
            if validate_mlflow_params(uri_val, username_val, password_val, experiment_name_val):
                # 只有驗證成功才會啟用
                context['ti'].xcom_push(key='report_to_mlflow', value=True)
                logger.info("MLflow 參數驗證完成")
        else:
            # 開啟report_to_mlflow但缺少mlflow uri，拋出錯誤
            logger.warning(f"已啟用 MLflow 追蹤，但追蹤 URI 欄位為空，停用 MLflow")
    else:
        logger.info("未啟用 MLflow 追蹤，跳過 MLflow 驗證")

    # --- Hugging Face ---
    hf_map = validation_map.get("huggingface", {})
    hf_model_name = hf_map.get("model")
    hf_token_name = hf_map.get("token")
    if hf_model_name and hf_token_name:
        hf_model = params.get(hf_model_name)
        hf_token = params.get(hf_token_name)
        if hf_model and hf_token:
            validate_hf_model(hf_model, hf_token)
            logger.info("HF 參數驗證完成")
        else:
            logger.warning(f"跳過 Hugging Face 驗證")

    # --- Git ---
    git_map = validation_map.get("git", {})
    repo_field = git_map.get("repo")
    token_field = git_map.get("token")
    if repo_field and token_field:
        repo_url = params.get(repo_field)
        git_token = params.get(token_field)
        if repo_url and git_token:
            validate_git_token(repo_url, git_token)
            logger.info("Git 參數驗證完成")
        else:
            logger.warning(f"跳過 Git 驗證")

    logger.info("使用者參數驗證完畢")

# ==========================
# 從 Git 下載資料
# ==========================
@task(task_id="fetch_data_task")
def fetch_data_from_git(
    repo_url: str,
    git_token: str,
    data_paths: Union[str, List[str], None] = None,
    dest_subdir: str = "data",
    **context
):
    """
    通用資料下載函式：
    - 如果指定 data_paths (檔案 / 資料夾 / 清單)，只複製那些內容
    - 如果 data_paths=None，則整個 repo 會被複製到 user_workspace/{dest_subdir}/
    - 如果 dest_subdir=None → 複製到 user_workspace/
    
    Args:
        repo_url (str): Git repo URL
        git_token (str): 存取 token
        data_paths (str | List[str] | None): Repo 內相對路徑
        dest_subdir (str): user_workspace 下要存放的子目錄
    """
    if not repo_url or not git_token:
        raise AirflowFailException("缺少 Git repo URL 或 token")

    # === workspace ===
    workspace = context['ti'].xcom_pull(task_ids='prepare_workspace_task')
    dest_dir = workspace if dest_subdir is None else os.path.join(workspace, dest_subdir)
    os.makedirs(dest_dir, exist_ok=True)
    # === tmp dir ===
    tmp_dir = tempfile.mkdtemp(dir=workspace)
    logger.info(f"建立暫存目錄: {tmp_dir} 於 workspace: {workspace}")

    auth_url = repo_url.replace("https://", f"https://oauth2:{git_token}@")

    copied = []

    try:
        # === clone repo ===
        logger.info("Cloning...")
        subprocess.run(["git", "clone", "--depth", "1", auth_url, tmp_dir], check=True)

        # === LFS ===
        subprocess.run(
            ["git", "config", "--local", "--remove-section", "filter.lfs"],
            cwd=tmp_dir,
            check=False
        )
        subprocess.run(["git", "lfs", "install", "--local"], cwd=tmp_dir, check=True)
        subprocess.run(["git", "lfs", "pull"], cwd=tmp_dir, check=True)

        # === 模式 1 & 2: 指定檔案/資料夾 ===
        if data_paths:
            if isinstance(data_paths, str):
                data_paths = [data_paths]

            for path in data_paths:
                target_path = os.path.join(tmp_dir, path)
                if not os.path.exists(target_path):
                    raise AirflowFailException(f"❌ 找不到: {path}")

                if os.path.isfile(target_path):
                    filename = os.path.basename(path)
                    dest_path = os.path.join(dest_dir, filename)
                    shutil.copy(target_path, dest_path)
                    logger.info(f"已複製檔案: {dest_path}")
                    copied.append(dest_path)

                elif os.path.isdir(target_path):
                    dest_path = os.path.join(dest_dir, os.path.basename(path))
                    if os.path.exists(dest_path):
                        shutil.rmtree(dest_path)
                    shutil.copytree(target_path, dest_path)
                    logger.info(f"已複製資料夾: {dest_path}")
                    copied.append(dest_path)

        # === 模式 3: 沒有指定 data_paths → 複製整個 repo ===
        else:
            dest_path = dest_dir
            if os.path.exists(dest_path):
                shutil.rmtree(dest_path)
            shutil.copytree(tmp_dir, dest_path, ignore=shutil.ignore_patterns(".git"))
            logger.info(f"已複製整個 repo 到: {dest_path}")
            copied.append(dest_path)

        logger.info("資料下載完成")
        return copied

    except Exception as e:
        logger.error(f"資料下載失敗: {e}")
        raise
    finally:
        logger.info(f"正在清理暫存目錄: {tmp_dir}")
        shutil.rmtree(tmp_dir, ignore_errors=True)

# ==========================
# 清理 User Workspace
# ==========================
@task(task_id="clear_workspace_task", trigger_rule="all_done")
def clear_user_workspace(
        preserve_on_failure: bool = True, 
        upload_task_id: str = None,
        **context
    ) -> None:
    """
    Clean up user workspace based on upload result.

    Args:
        preserve_on_failure (bool): whether to keep workspace if upload failed
        upload_task_id (str): task id of the upload task to get "upload_success" XCom
    """
    ti = context['ti']
    work_dir = ti.xcom_pull(task_ids='prepare_workspace_task')

    if not work_dir or not os.path.exists(work_dir):
        logger.info(f"[CLEANUP] No user workspace found or already deleted: {work_dir}")
        return

    if upload_task_id is None:
        logger.info("[CLEANUP] upload_task_id is None → assuming upload success.")
        clear_workspace(work_dir)
        logger.info(f"[CLEANUP] Deleted user workspace: {work_dir}")
        return

    upload_success = context['ti'].xcom_pull(task_ids=upload_task_id, key='upload_success')
    if upload_success is True:
        clear_workspace(work_dir)
        logger.info(f"[CLEANUP] Deleted user workspace: {work_dir}")
    elif preserve_on_failure:
        logger.info(f"[UPLOAD FAILED] Preserve data in user workspace: {work_dir}")
    else:
        clear_workspace(work_dir)
        logger.info(f"[CLEANUP] Deleted user workspace: {work_dir}")

# ==========================
# 上游任務失敗，標記 DAG 失敗
# ==========================
@task(task_id="mark_failed_task", trigger_rule="one_failed")
def mark_dag_failed():
    """
    檢查當前 DAG Run 是否有任何失敗的 task，
    如果有，raise Exception 讓該任務失敗，DAG 最終顯示失敗。
    """
    raise Exception(f"DAG contains failed tasks, marking DAG as failed.")