from airflow.decorators import dag, task
from pendulum import datetime
import random

#@dag(start_date=datetime(2022, 12, 20), schedule="@daily", catchup=False)
@dag
def simple_xcom_dag():
    @task
    def show_xcom():
        from airflow.models.xcom import XCom
        print("XCOM NAME 1")
        print(XCom.__name__)
        
        from airflow.settings import conf
        print("XCOM NAME 2")
        xcom_bk = conf.get("core", "xcom_backend")
        print(xcom_bk)
    
    @task
    def pick_a_random_number():
        return random.randint(1, 10)  # push to XCom

    @task
    def print_a_number(num):  # retrieve from XCom
        print(num)

    show_xcom() >> print_a_number(pick_a_random_number())


simple_xcom_dag()
