from __future__ import annotations

import os
import sys
import logging
import subprocess
import requests
import wandb
import mlflow
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Union
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from huggingface_hub import whoami

# ==========================
# Logger
# ==========================
def get_logger(name: str = "airflow_task") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.propagate = False 
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        logger.setLevel(logging.INFO)
        
    return logger


logger = get_logger()

# ==========================
# Initialize the working environment
# ==========================
def init_workspace(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        logger.info(f"Workspace created: {path}")
    else:
        logger.info(f"Workspace exists: {path}")

# ==========================
# Obtain DAG Trigger users
# ==========================
def get_trigger_user(**context):
    from airflow.models.log import Log
    from airflow.utils.session import create_session

    dr = context.get("dag_run")
    with create_session() as session:
        log = (
            session.query(Log.owner)
            .filter(
                Log.dag_id == dr.dag_id,
                Log.execution_date == dr.logical_date,
                Log.event == "trigger"
            )
            .order_by(Log.dttm.desc())
            .first()
        )
    user = log.owner if log else "unknown"
    logger.info(f"Triggered by user: {user}")
    return user

# ==========================
# Data verification
# ==========================
def validate_wandb_API(wandb_key: str):
    """Verify Wandb API key"""
    try:
        wandb.login(key=wandb_key, verify=True, relogin=True)
        api = wandb.Api()
        user = api.viewer
        logger.info(f"W&B 驗證成功，使用者：{user}")
        return True
    except Exception as e:
        logger.error(f"W&B 驗證失敗: {e}")
        raise AirflowFailException("W&B 驗證失敗")
    
def validate_mlflow_params(uri: str, username: str, password: str, experiment_name: str):
    """Verify MLflow Params"""
    logger.info(f"嘗試連線至 MLflow URI: {uri}")
    logger.info(f"目標實驗名稱: {experiment_name}")
    if username:
        os.environ['MLFLOW_TRACKING_USERNAME'] = username
    if password:
        os.environ['MLFLOW_TRACKING_PASSWORD'] = password
    try:
        mlflow.set_tracking_uri(uri)
        mlflow.set_experiment(experiment_name)
        with mlflow.start_run() as run:
            mlflow.log_param("validation_test", "success")
            logger.info(f"MLflow 連線測試成功。刪除測試 Run ID: {run.info.run_id}")
        # delete testing Run
        mlflow.delete_run(run.info.run_id) 
        return True
    except Exception as e:
        os.environ.pop('MLFLOW_TRACKING_USERNAME', None)
        os.environ.pop('MLFLOW_TRACKING_PASSWORD', None)
        
        error_message = (
            f"MLflow 連線或寫入測試失敗。請檢查 URI/實驗名稱/認證資訊: {e}"
        )
        logger.error(f"MLflow 驗證失敗: {error_message}")
        raise AirflowFailException("MLflow 驗證失敗")
    finally:
        os.environ.pop('MLFLOW_TRACKING_USERNAME', None)
        os.environ.pop('MLFLOW_TRACKING_PASSWORD', None)
    

def validate_hf_model(model_path: str, hf_token: str):
    """Verify Hugging Face model_path + token"""
    if not model_path or not hf_token:
        raise AirflowFailException("Hugging Face model/token 為必填")

    headers = {"Authorization": f"Bearer {hf_token}"}

    try:
        user_info = whoami(token=hf_token) 
        logger.info(f"Hugging Face token 驗證成功，使用者 ID: {user_info.get('name', 'N/A')}")   
    except Exception as e:
        error_msg = f"Hugging Face token 驗證失敗。請檢查您的 Token: {e}"
        logger.error(error_msg)
        raise AirflowFailException(error_msg)

    try:
        response = requests.get(f"https://huggingface.co/api/models/{model_path}", headers=headers)
        response.raise_for_status()
        logger.info(f"Hugging Face model `{model_path}` 驗證成功")
        return True
    except Exception as e:
        logger.error(f"無效的 Hugging Face model 或 token: {e}")
        raise AirflowFailException(f"無效的 Hugging Face model 或 token: {e}")
    

def validate_git_token(repo_url: str, git_token: str):
    """Verify GitLab/GitHub token"""
    import subprocess
    if not repo_url or not git_token:
        raise AirflowFailException("Git repo URL 與 token 為必填")
    auth_url = repo_url.replace("https://", f"https://oauth2:{git_token}@")
    try:
        subprocess.run(["git", "ls-remote", auth_url], check=True, capture_output=True)
        logger.info(f"Git token 驗證成功: {repo_url}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Git token 驗證失敗: {e.stderr.decode().strip()}")
        raise AirflowFailException(f"Git token 驗證失敗: {e.stderr.decode().strip()}")

# ==========================
# Download Git data
# ==========================
def clone_git_repo(repo_url: str, token: str, dest: str, branch: str = "main"):
    if os.path.exists(dest):
        logger.info(f"Removing existing repo: {dest}")
        shutil.rmtree(dest)

    cmd = f"git clone --branch {branch} https://oauth2:{token}@{repo_url.lstrip('https://')} {dest}"
    logger.info(f"Cloning repo: {repo_url} -> {dest}")
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Git clone failed: {e}")
        raise AirflowFailException(f"Git clone failed: {e}")

# ==========================
# Upload results to Git
# ==========================
def upload_results_to_git(
    workspace: str,
    git_repo: str,
    git_token: str,
    src_paths: List[str],
    dest_subdir,
    commit_msg: str = "Auto upload results",
    use_lfs: bool = False,
    lfs_extensions: Optional[List[str]] = None,
    branch: str = "main", 
):
    repo_url = git_repo.replace("https://", f"https://oauth2:{git_token}@")

    # === Create a temporary directory within user_workspace ===
    local_dir = tempfile.mkdtemp(dir=workspace)
    logger.info(f"建立暫存目錄: {local_dir} 於 workspace: {workspace}")
    
    try:
        env = os.environ.copy()
        env.update({ 
            "GIT_HTTP_POST_BUFFER": "524288000"
        })
        
        logger.info("===== Clone repository =====")
        subprocess.run(["git", "clone", "--depth", "1", repo_url, local_dir], check=True, env=env)
        os.chdir(local_dir)

        logger.info("===== 設定 Git LFS =====")
        for k, v in [
            ("lfs.concurrenttransfers", "3"),
            ("lfs.dialtimeout", "600"), 
            ("lfs.tlstimeout", "600"),
            ("lfs.activitytimeout", "300"), 
            ("lfs.keepalive", "1800")
        ]:
            subprocess.run(["git", "config", k, v], check=True, env=env)

        # === Setup Git LFS ===
        if use_lfs:
            subprocess.run(["git", "lfs", "install"], check=True, env=env)
            if lfs_extensions:
                for ext in lfs_extensions:
                    subprocess.run(["git", "lfs", "track", ext], check=True, env=env)
                subprocess.run(["git", "add", ".gitattributes"], check=True, env=env)
            logger.info(f"Git LFS enabled, tracking: {lfs_extensions or 'none'}")

        logger.info("===== 複製模型文件 =====")
        # === Prepare destination dir ===
        dest_dir = os.path.join(local_dir, dest_subdir) if dest_subdir else local_dir
        os.makedirs(dest_dir, exist_ok=True)

        # === Copy files ===
        for src in src_paths:
            if not os.path.exists(src):
                logger.warning(f"Source path does not exist, skipping: {src}")
                continue

            if os.path.isdir(src):
                shutil.copytree(src, os.path.join(dest_dir, os.path.basename(src)), dirs_exist_ok=True)
            else:
                shutil.copy(src, dest_dir)
            logger.info(f"已複製: {src} 到 {dest_dir}")

        logger.info("===== Git 設定與 commit =====")
        subprocess.run(["git", "config", "--global", "user.email", "airflow@example.com"], check=True, env=env)
        subprocess.run(["git", "config", "--global", "user.name",  "Airflow"], check=True, env=env)

        logger.info("檢查待上傳文件大小...")
        result = subprocess.run(
            ["du", "-sh", local_dir],
            capture_output=True,
            text=True
        )
        logger.info(f"待上傳總大小: {result.stdout.strip()}")


        # === Commit & Push ===
        subprocess.run(["git", "add", "."], check=True, env=env)
        subprocess.run(
            ["git", "commit", "-m", commit_msg], check=True, env=env
        )

        # Detect remote preset branches
        out = subprocess.run(
            ["git", "symbolic-ref", "refs/remotes/origin/HEAD"],
            capture_output=True,
            text=True,
            check=True,
            env=env
        ).stdout.strip()
        branch = out.split("/")[-1]  # e.g. refs/remotes/origin/main -> main
        logger.info(f"Pushing to branch `{branch}`")

        logger.info("===== Push 到遠端 =====")
        result = subprocess.run(
            ["git", "push", "origin", branch],
            capture_output=True,
            text=True,
            check=True,
            env=env
        )

        logger.info(f"成功上傳結果到 Git: {git_repo}@{branch}/{dest_subdir or ''}")

    except subprocess.CalledProcessError as e:
        logger.error("❌ 模型與日誌上傳失敗")
        logger.error(f"⚠️ 錯誤代碼: {e.returncode}")
        raise AirflowFailException("模型與日誌上傳失敗")
    finally:
        logger.info(f"正在清理暫存目錄: {local_dir}")
        shutil.rmtree(local_dir, ignore_errors=True)

# ==========================
# Clean up the work environment
# ==========================
def clear_workspace(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
        logger.info(f"Workspace cleared: {path}")

# ==========================
# Mail Relay
# ==========================
def send_mail(
    to_addrs: Union[str, List[str]],
    from_addr: str,
    subject: str,
    body: str,
    url: str,
    token: str,
    mime: str = "html"
) -> None:
    """Send email via internal mail-relay HTTP service."""
    
    if isinstance(to_addrs, (str, bytes)):
        to_list = [to_addrs]
    else:
        to_list = list(to_addrs or [])

    if not url:
        raise AirflowFailException(f"[send_mail] MAIL_API_URL missing. To={to_list} Subject={subject}")
    if not token:
        raise AirflowFailException("[send_mail] MAIL_API_TOKEN missing, cannot call relay.")

    headers = {
        "Content-Type": "application/json",
        "X-Auth-Token": token,
    }

    payload: Dict[str, Any] = {
        "to": to_list if len(to_list) > 1 else to_list[0],
        "from": from_addr,
        "subject": subject,
        "body": body,
        "mime": mime,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        if not (200 <= resp.status_code < 300):
            raise AirflowFailException(f"[send_mail] relay non-2xx: {resp.status_code} {resp.text}")
    except Exception as e:
        raise AirflowFailException(f"[send_mail] relay failed: {e}")