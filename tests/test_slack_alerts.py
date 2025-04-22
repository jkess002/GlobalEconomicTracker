import os
from datetime import datetime
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

from airflow_pipeline.dags.dbt_etl_dag import send_slack_alert


@patch.dict(os.environ, {"SLACK_WEBHOOK_URL": "http://example.com"})
@patch("airflow_pipeline.dags.dbt_etl_dag.requests.post")
def test_send_slack_alert(mock_post):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    context = {
        "dag": type("DAG", (), {"dag_id": "global_econ_dbt_pipeline"})(),
        "run_id": "manual__2024-04-22T20:00:00",
        "execution_date": datetime(2024, 4, 22, 20, 0, 0, tzinfo=ZoneInfo("UTC")),
        "task_instance": type("TI", (), {"log_url": "http://example.com/log"})(),
    }

    send_slack_alert("success", **context)

    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert "http" in args[0] or "url" in kwargs
