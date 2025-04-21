from unittest.mock import patch
from airflow.dags.dbt_etl_dag import send_slack_alert

@patch("my_dag.SlackWebhookOperator")
def test_send_slack_alert(mock_slack):
    context = {
        'dag': type("DAG", (), {"dag_id": "test_dag"})(),
        'run_id': "manual__2025-04-16",
        'execution_date': "2025-04-16T00:00:00",
        'task_instance': type("TI", (), {"log_url": "http://localhost/logs"})()
    }
    send_slack_alert("failed", **context)
    mock_slack.assert_called_once()
