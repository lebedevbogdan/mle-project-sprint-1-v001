from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token='{ token_id }',
                        chat_id='{ chat_id }')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!'
    hook.send_message({
        'chat_id': '{ chat_id }',
        'text': message
    })

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                       token='{ token_id }',
                       chat_id='{ chat_id }'
                       )
    run_id = context['run_id']
    dag = context['dag']
    message = f'Исполнение DAG {dag} с id={run_id} провалено!'
    hook.send_message({
        'chat_id': '{ chat_id }',
        'text': message
    })
