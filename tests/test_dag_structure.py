import pytest
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder="airflow/dags", include_examples=False)

def test_dag_loaded(dag_bag):
    dag = dag_bag.get_dag('global_econ_dbt_pipeline')
    assert dag is not None, "DAG global_econ_dbt_pipeline failed to load"

def test_task_count(dag_bag):
    dag = dag_bag.get_dag('global_econ_dbt_pipeline')
    assert len(dag.tasks) == 6  # Adjust depending on how many tasks you have

def test_task_dependencies(dag_bag):
    dag = dag_bag.get_dag('global_econ_dbt_pipeline')

    expected_deps = {
        'notify_start': ['run_staging_models'],
        'run_staging_models': ['run_analytics_models'],
        'run_analytics_models': ['run_dbt_tests'],
        'run_dbt_tests': ['notify_complete'],
        'run_dbt_tests': ['check_failure'],
        'check_failure': ['notify_failure']
    }

    for upstream, downstream_list in expected_deps.items():
        for downstream in downstream_list:
            assert dag.has_task(upstream), f"Missing task: {upstream}"
            assert dag.has_task(downstream), f"Missing task: {downstream}"
            assert downstream in dag.task_dict[upstream].downstream_task_ids
