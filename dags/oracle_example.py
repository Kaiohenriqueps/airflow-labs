from datetime import datetime
from airflow.decorators import dag, task


@dag(
    "oracle_example",
    start_date=datetime(2021, 12, 1),
    schedule="@daily",
    catchup=False,
)
def my_dag():
    @task(task_id="connect_to_db")
    def task_0():
        from airflow.providers.oracle.hooks.oracle import OracleHook

        hook = OracleHook(oracle_conn_id="oracle_conn_id")
        print(hook.get_conn())

    task_0()


my_dag()
