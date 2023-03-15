from datetime import datetime
from airflow.decorators import dag, task, task_group

number_of_batches = 5


@dag(
    "task_group_example",
    start_date=datetime(2021, 12, 1),
    schedule="@daily",
    catchup=False,
)
def my_dag():
    @task(task_id="list")
    def task_0():
        from utils.ops_list import split

        my_list = list(range(1, 11))
        return split(my_list=my_list, n=number_of_batches)

    @task_group(group_id="group1")
    def tg1(my_list, n):
        @task
        def print_list(list, n):
            print(list[n])
            return list[n]

        @task
        def sum_list(list, n):
            print(sum(list[n]))
            return sum(list[n])

        [print_list(my_list, n), sum_list(my_list, n)]

    @task
    def pull_xcom(**context):
        pulled_xcom = context["ti"].xcom_pull(
            task_ids=["group1.sum_list"],
            map_indexes=[2, 3],
            key="return_value",
        )
        print(pulled_xcom)

    t0 = task_0()
    tg1_object = tg1.partial(my_list=t0).expand(n=list(range(0, number_of_batches)))
    t0 >> tg1_object >> pull_xcom()


my_dag()
