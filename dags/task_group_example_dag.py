from datetime import datetime
from airflow.decorators import dag, task, task_group


@dag(
    "task_group_example_dag",
    start_date=datetime(2021, 12, 1),
    schedule="@daily",
    catchup=False,
)
def my_dag():
    @task
    def test_0():
        my_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        chunk_size = 5
        list_chunked = [
            my_list[i : i + chunk_size] for i in range(0, len(my_list), chunk_size)
        ]
        return list_chunked

    @task
    def test(my_list: list, index: int):
        print(my_list[index])

    t0 = test_0()
    my_tasks = test.partial(my_list=t0).expand(index=[0, 1])
    t0 >> my_tasks


my_dag()
