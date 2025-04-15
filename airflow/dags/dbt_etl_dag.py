from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import requests, json


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
    'depends_on_past': False
}

def send_slack_alert(status, **context):
#     dag_id = context['dag'].dag_id
#     run_id = context['run_id']
#     execution_date = context['execution_date']
#     log_url = context['task_instance'].log_url
#
#     emojis = {
#         "started": "🚀",
#         "success": "✅",
#         "failed": "💥😢❌"
#     }
#
#     emoji = emojis.get(status, "🔔")
#     message = f"""
# {emoji} *DBT Pipeline {status.upper()}*
# *DAG:* `{dag_id}`
# *Run ID:* `{run_id}`
# *Execution Time:* `{execution_date}`
# 🔗 *<{log_url}|View Logs>*
#     """
#
#     return SlackWebhookOperator(
#         task_id=f'slack_notification_{status}',
#         slack_webhook_conn_id='slack_default',
#         message=message,
#         username='airflow 🤖',
#     ).execute(context=context)
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url

    emojis = {
        "started": "🚀",
        "success": "✅",
        "failed": "💥😢❌"
    }

    emoji = emojis.get(status, "🔔")
    message = f"""
        {emoji} *DBT Pipeline {status.upper()}*
        *DAG:* `{dag_id}`
        *Run ID:* `{run_id}`
        *Execution Time:* `{execution_date}`
        🔗 *<{log_url}|View Logs>*
            """

    webhook_token = Variable.get("slack_webhook_url")
    requests.post(webhook_token, json={"text": message})

with DAG(
    dag_id='global_econ_dbt_pipeline',
    default_args=default_args,
    schedule_interval='0 1 * * 2-6',  # 5 PM PST
    catchup=False,
    tags=['dbt', 'analytics']
) as dag:

    run_staging_models = BashOperator(
        task_id='run_staging_models',
        bash_command='cd /opt/global_economic_tracker && dbt run --select path:models/staging'
    )

    run_analytics_models = BashOperator(
        task_id='run_analytics_models',
        bash_command='cd /opt/global_economic_tracker && dbt run --select path:models/analytics'
    )

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/global_economic_tracker && dbt test'
    )

    start_alert = SimpleHttpOperator(
        task_id='notify_start',
        http_conn_id='slack_default',
        endpoint='',
        method='POST',
        headers={"Content-Type": "application/json"},
        data=json.dumps({"mytestkey": f"🚀 DAG ({dag.dag_id}) has started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}!"})
    )

    end_alert = SimpleHttpOperator(
        task_id='notify_complete',
        http_conn_id='slack_default',
        endpoint='',
        method='POST',
        headers={"Content-Type": "application/json"},
        data=json.dumps({"mytestkey": f"✅ DAG ({dag.dag_id}) completed successfully  at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}!"})
    )

    fail_alert = PythonOperator(
        task_id='notify_failure',
        python_callable=send_slack_alert,
        op_kwargs={"status": "failed"},
        trigger_rule=TriggerRule.ONE_FAILED
    )

    start_alert >> run_staging_models >> run_analytics_models >> run_dbt_tests >> end_alert
    [run_staging_models, run_analytics_models, run_dbt_tests] >> fail_alert

