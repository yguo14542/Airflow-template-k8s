from airflow.sdk import Param

# -------------------------------
# Git / Repository
# -------------------------------
GIT = {
    "git_repo_url": Param("https://path/to/repo.git", type="string", description="資料放置 git repo 連結"),
    "git_token": Param("glpat-xxx", type="string", description="🔒 請輸入 git token（讀取/寫入 git repo 權限）"),
    "data_path": Param("data/example.jsonl", type="string", description="請輸入相對根目錄的資料路徑")
}

# -------------------------------
# Wandb / Logging
# -------------------------------
WANDB = {
    "report_to_wandb": Param(False, type="boolean", description="是否同步訓練 log 至 Wandb"),
    "wandb_api_key": Param("", description="🔒 如果開啟report_to_wandb，請輸入 Wandb API key")
}

# -------------------------------
# MLflow / Logging
# -------------------------------
MLFLOW = {
    "report_to_mlflow": Param(False, type="boolean", description="是否同步訓練 log 至 MLflow"),
    "mlflow_tracking_uri": Param("", description="如果開啟report_to_mlflow，請輸入 MLflow server uri"),
    "mlflow_tracking_username": Param("", description="如果有設定 MLflow Authentication，請輸入 username"),
    "mlflow_tracking_password": Param("", description="🔒 如果有設定 MLflow Authentication，請輸入 password"),
    "mlflow_experiment_name": Param("", description="可自行設定 MLflow experiment name，須確保有權限可寫入，若留空則使用預設experiment name"),
}

# -------------------------------
# Model / Training Checkpoint
# -------------------------------
MODEL = {
    "hf_model_path": Param("meta-llama/Llama-3.2-1B-Instruct", type="string", description="基礎模型 Hugging Face 路徑"),
    "hf_token": Param("hf_xxx", type="string", description="🔒 請輸入 Hugging Face token（基礎模型讀取權限）"), 
}

# -------------------------------
# GPU / Hardware
# -------------------------------
GPU = {
    "gpu_type": Param("v100", enum=["V100", "H100"], type="string", description="選擇 GPU 類型"),
    "gpu_counts": Param(1, enum=[1, 2, 4, 8], type="integer", description="選擇 GPU 數量"),
}

# -------------------------------
# Training / LoRA / Hyperparams
# -------------------------------
TRAIN = {
    "train_type": Param("lora", enum=["lora","full"], type="string", description="全參數或LoRA訓練"),
    "num_epochs": Param(1, enum=[1, 2, 3, 4, 5], type="number", description="最大訓練週期數 (1~5)"),
    "max_length": Param(512, enum=[512, 1024, 2048, 4096, 8192, 16384], type="integer", description="模型的上下文長度")
}

# -------------------------------
# Email / Notification
# -------------------------------
EMAIL = {
    "email_notify": Param(False, type="boolean", description="是否開啟 email 通知"),
    "email_recipient": Param("default@example.com", description="email 通知地址")
}