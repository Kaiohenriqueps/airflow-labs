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
    def test_1():
        my_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        chunk_size = 5
        list_chunked = [
            my_list[i : i + chunk_size] for i in range(0, len(my_list), chunk_size)
        ]
        return list_chunked

    @task_group
    def my_tg(my_list):
        for i, list in enumerate(my_list):

            @task(task_id=f"process_{i}")
            def process(list, i):
                for elem in list[i]:
                    print(elem)

    @task
    def test_2(my_list):
        print(my_list)

    t1 = test_1()
    mtg = my_tg(t1)
    t2 = test_2()
    t1 >> mtg >> t2
    # t1 >> t2


my_dag()
