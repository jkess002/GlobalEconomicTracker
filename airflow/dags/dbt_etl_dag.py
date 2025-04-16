from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import requests, json, os
from etl_tasks import save_to_postgres
from zoneinfo import ZoneInfo


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
    'depends_on_past': False
}

def send_slack_alert(status, **context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url

    local_time = execution_date.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("America/Los_Angeles"))
    formatted_time = local_time.strftime("%Y-%m-%d %I:%M:%S %p %Z")

    emojis = {
        "started": "🚀",
        "success": "✅",
        "failed": "💥😢❌"
    }

    emoji = emojis.get(status, "🔔")
    message = f"""
    {emoji} DBT Pipeline {status.upper()}
    DAG: {dag_id}
    Run ID: {run_id}
    Execution Time: {formatted_time}
    View Logs: {log_url}
    """

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    response = requests.post(
        webhook_url,
        json={"mytestkey": message},
        headers={"Content-Type": "application/json"}
    )
    response.raise_for_status()

with DAG(
    dag_id='global_econ_dbt_pipeline',
    default_args=default_args,
    schedule_interval='0 1 * * 2-6',  # 5 PM PST
    catchup=False,
    tags=['dbt', 'analytics']
) as dag:

    load_data = PythonOperator(
        task_id="load_market_data",
        python_callable=save_to_postgres,
        op_kwargs={'backfill': False}
    )

    run_staging_models = BashOperator(
        task_id='run_staging_models',
        bash_command='cd /opt/global_economic_tracker && dbt run --select path:models/staging --profiles-dir /home/airflow/.dbt'
    )

    run_analytics_models = BashOperator(
        task_id='run_analytics_models',
        bash_command='cd /opt/global_economic_tracker && dbt run --select path:models/analytics --profiles-dir /home/airflow/.dbt'
    )

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/global_economic_tracker && dbt test --profiles-dir /home/airflow/.dbt'
    )

    start_alert = SimpleHttpOperator(
        task_id='notify_start',
        http_conn_id='slack_default',
        endpoint='',
        method='POST',
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {"mytestkey": f"🚀 DAG ({dag.dag_id}) has started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}!"})
    )

    end_alert = SimpleHttpOperator(
        task_id='notify_complete',
        http_conn_id='slack_default',
        endpoint='',
        method='POST',
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {"mytestkey": f"✅ DAG ({dag.dag_id}) completed successfully  at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}!"})
    )

    fail_alert = PythonOperator(
        task_id='notify_failure',
        python_callable=send_slack_alert,
        op_kwargs={"status": "failed"},
        trigger_rule=TriggerRule.ONE_FAILED
    )

    start_alert >> load_data >> run_staging_models >> run_analytics_models >> run_dbt_tests >> end_alert
    [run_staging_models, run_analytics_models, run_dbt_tests] >> fail_alert


