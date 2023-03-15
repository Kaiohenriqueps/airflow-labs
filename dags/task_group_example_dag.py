from datetime import datetime
from airflow.decorators import dag, task, task_group

number_of_batches = 5


def split(my_list, n):
    k, m = divmod(len(my_list), n)
    return list(
        (my_list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))
    )


@dag(
    "task_group_example_dag",
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
        return split(my_list, number_of_batches)

    @task(task_id="print")
    def task_2(my_list: list, index: int):
        print(my_list[index])

    t0 = task_0()
    t1 = task_1(t0)
    t2 = task_2.partial(my_list=t1).expand(index=list(range(0, number_of_batches)))
    t0 >> t1 >> t2


my_dag()
