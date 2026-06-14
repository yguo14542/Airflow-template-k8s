from airflow.providers.standard.operators.python import PythonOperator
import os
from datetime import datetime
from common.utils import send_mail, get_logger

logger = get_logger()

class EmailNotificationOperator(PythonOperator):
    """
    The general email operator uses the internal mail-relay HTTP service to send notification emails.

    Initialization parameters:
    - success: Whether to indicate that this notification is a success. False indicates a failure.
    - task: Add the task name to the subject line of the letter.
    - upload_task_id: You can specify the TASK ID of the uploaded folder to be passed by XCOM.
    - upload_xcom_key: XCOM transmits the corresponding XCOM key for the uploaded folder.

    The following environmental variables are required:
    - MAIL_API_URL: Mail relay HTTP service URL
    - MAIL_API_TOKEN: Authentication Token
    - MAIL_FROM_ADDR: sender's email

    Example:
        "success": EmailNotificationOperator(
            task_id="notify_success",
            task="TEST-DAG",
            success=True,
            upload_task_id="upload_task",
            upload_xcom_key="upload_dest_subdir",
            trigger_rule="all_success",
        ),
    """

    def __init__(
        self, 
        success: bool = True, 
        task: str = "DAG", 
        upload_task_id: str = "upload_results_task",
        upload_xcom_key: str = "upload_dest_subdir",
        upload_branch: str = "main",
        **kwargs
    ):
        super().__init__(
            python_callable=self._notify,
            **kwargs
        )
        self.success = success
        self.task = task
        self.upload_task_id = upload_task_id
        self.upload_xcom_key = upload_xcom_key
        self.upload_branch = upload_branch

    def _notify(self, **context):
        logger.info(f"===== [EmailNotificationOperator] 開始執行通知 (Success={self.success}) =====")
        params = context.get("params", {})
        
        # Basic settings check
        email_notify = params.get("email_notify", True)
        if not email_notify:
            logger.info("[EmailNotificationOperator] email_notify disabled, skipping.")
            return

        to_addrs = params.get("email_recipient")
        if not to_addrs:
            logger.warning("[EmailNotificationOperator] 沒有設定收件人，跳過寄信")
            return

        # Obtaining basic variables
        ti = context['ti']
        run_id = context['run_id']
        dag_id = ti.dag_id
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        from_addr = os.environ.get("MAIL_FROM_ADDR")
        url = os.environ.get("MAIL_API_URL")
        token = os.environ.get("MAIL_API_TOKEN")

        # Securely retrieve XCom data
        upload_info_html = ""
        dest_subdir = None

        if self.upload_task_id and self.upload_xcom_key:
            try:
                dest_subdir = ti.xcom_pull(task_ids=self.upload_task_id, key=self.upload_xcom_key)
                logger.info(f"[EmailNotificationOperator] XCom Pull 結果: {dest_subdir}")
            except Exception as e:
                logger.error(f"[EmailNotificationOperator] 嘗試從 XCom 拉取資料時發生錯誤 (跳過): {e}")

            if dest_subdir:
                git_repo_raw = params.get("git_repo_url", "")
                if git_repo_raw:
                    base_web_url = git_repo_raw.replace(".git", "")
                    full_upload_path = f"{base_web_url}/-/tree/{self.upload_branch}/{dest_subdir}"
                    
                    # Combined upload path link
                    upload_info_html = f"""
                    <a href="{full_upload_path}">{full_upload_path}</a>
                    """

                else:
                    logger.warning("[EmailNotificationOperator] git_repo_url 為空，無法生成連結")

        #  Combined email content
        if self.success:
            subject = f"[SUCCESS] {self.task} (RUN {run_id})"
            status_msg = "任務執行成功"
            status_color = "#28a745"
        else:
            subject = f"[FAILED] {self.task} (RUN {run_id})"
            status_msg = "任務執行失敗"
            status_color = "#dc3545"

        upload_section = ""
        logger.info(f"upload_info_html: {upload_info_html}")
        if upload_info_html:
            upload_section = f"""
            <tr>
                <td style="padding: 8px 0; color: #777; vertical-align: top;">上傳路徑</td>
                <td style="padding: 8px 0; font-size: 13px; word-break: break-all; vertical-align: top;">
                    {upload_info_html}
                </td>
            </tr>
            """

        body = f"""
        <div style="font-family: 'Microsoft JhengHei', sans-serif; max-width: 600px; border: 1px solid #e0e0e0; border-radius: 10px; overflow: hidden;">
            <div style="background-color: {status_color}; color: white; padding: 15px; font-size: 18px; font-weight: bold; text-align: center;">
                {status_msg}
            </div>
            <div style="padding: 20px; color: #333;">
                <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                    <tr>
                        <td style="padding: 8px 0; color: #777; width: 100px; vertical-align: top;">執行 DAG</td>
                        <td style="padding: 8px 0; font-weight: bold; vertical-align: top;">{dag_id}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px 0; color: #777; vertical-align: top;">執行 ID</td>
                        <td style="padding: 8px 0; font-family: monospace; color: #555; vertical-align: top;">{run_id}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px 0; color: #777; vertical-align: top;">通知時間</td>
                        <td style="padding: 8px 0; vertical-align: top;">{now}</td>
                    </tr>
                    {upload_section}
                </table>
            </div>
            <div style="background-color: #f1f1f1; padding: 10px; font-size: 12px; text-align: center; color: #999;">
                此為系統自動發送通知，請勿直接回覆。
            </div>
        </div>
        """

        # Execute Send
        try:
            logger.info(f"subject: {subject}")
            send_mail(to_addrs, from_addr, subject, body, url, token, "html")
            logger.info(f"[EmailNotificationOperator] 郵件發送完成。")
        except Exception as e:
            logger.error(f"[EmailNotificationOperator] 調用 send_mail 發生異常: {e}")
            raise