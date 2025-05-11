import os
from dotenv import load_dotenv
from airflow.providers.telegram.hooks.telegram import TelegramHook


load_dotenv()
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")

def send_telegram_success_message(**context):
    hook = TelegramHook(token=TG_CHAT_ID, chat_id=TG_BOT_TOKEN)
    dag = context['dag']
    run_id = context['run_id']
    message = f'✅ Исполнение DAG {dag} с id={run_id} прошло успешно!'
    hook.send_message(chat_id=TG_CHAT_ID, text=message)

def send_telegram_failure_message(**context):
    hook = TelegramHook(token=TG_BOT_TOKEN, chat_id=TG_CHAT_ID)
    dag = context['dag']
    run_id = context['run_id']
    task_instance = context['task_instance_key_str']
    message = (f'❌ Исполнение DAG {dag} с id={run_id} завершилось с ошибкой!\n'
               f'Неудачный task: {task_instance}')
    hook.send_message(chat_id=TG_CHAT_ID, text=message)