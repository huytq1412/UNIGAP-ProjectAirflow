import html
import requests
from airflow.models import Variable

def send_telegram_error_alert(context):
    """Hàm Callback tự động gửi tin nhắn thông báo lỗi khi Task FAILED"""
    print("Đang gửi tin nhắn thông báo lỗi tới Telegram bot...")

    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = context.get('logical_date').strftime("%Y-%m-%d %H:%M:%S")
    log_url = context.get('task_instance').log_url

    # Dùng html.escape để vô hiệu hóa các ký tự lạ (<, >, &) trong log lỗi nếu có
    error_msg = html.escape(str(context.get('exception')))

    text_message = (
        f"🚨 <b>Airflow Task Failed</b> 🚨\n\n"
        f"❌ <b>Task Thất Bại:</b> <code>{task_id}</code>\n"
        f"📁 <b>DAG:</b> <code>{dag_id}</code>\n"
        f"⏳ <b>Thời gian:</b> {execution_date} UTC\n"
        f"📝 <b>Chi tiết lỗi:</b> <code>{error_msg}</code>"
    )

    bot_token = Variable.get("telegram_bot_token")
    chat_id = Variable.get("telegram_chat_id")
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": text_message,
        "parse_mode": "HTML",
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(api_url, json=payload, headers=headers, timeout=10)

    # Bắt lỗi nếu Telegram từ chối
    if response.status_code != 200:
        print(f"❌ TELEGRAM API ERROR: {response.text}")

    response.raise_for_status()


def send_telegram_success_alert(context):
    """Hàm Callback tự động gửi tin nhắn thành công khi DAG SUCCESS"""
    print("Đang gửi tin nhắn báo thành công qua Telegram bot...")

    dag_id = context.get('dag_run').dag_id
    execution_date = context.get('logical_date').strftime("%Y-%m-%d %H:%M:%S")

    text_message = (
        f"✅ <b>Pipeline run successfully</b> ✅\n\n"
        f"📁 <b>DAG:</b> <code>{dag_id}</code>\n"
        f"⏳ <b>Thời gian:</b> {execution_date} UTC\n"
    )

    bot_token = Variable.get("telegram_bot_token")
    chat_id = Variable.get("telegram_chat_id")
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": text_message,
        "parse_mode": "HTML"
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(api_url, json=payload, headers=headers, timeout=10)

    # Bắt lỗi nếu Telegram từ chối
    if response.status_code != 200:
        print(f"❌ TELEGRAM API ERROR: {response.text}")

    response.raise_for_status()