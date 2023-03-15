from datetime import datetime
from airflow.decorators import dag, task, task_group

number_of_batches = 5


@dag(
    "multiple_tasks_dynamic_example",
    start_date=datetime(2021, 12, 1),
    schedule="@daily",
    catchup=False,
)
def my_dag():
    @task(task_id="list")
    def task_0():
        return [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    @task(task_id="split")
    def task_1(my_list):
        from utils.ops_list import split

        return split(my_list, number_of_batches)

    @task(task_id="print")
    def task_2(my_list: list, index: int):
        print(my_list[index])

    t0 = task_0()
    t1 = task_1(t0)
    t2 = task_2.partial(my_list=t1).expand(index=list(range(0, number_of_batches)))
    t0 >> t1 >> t2


my_dag()
