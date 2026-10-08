"""HW2 Q2.1: q21_manual (manual trigger) and q21_every_30m (every 30 min)."""
import os
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SCRIPTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")

# Python callables (3 different kinds: sleep, print, count) 
count = 0


def correct_sleeping_function():
    """sleep"""
    time.sleep(2)


def print_function():
    """print"""
    print("This represents a python operator")
    time.sleep(1)


def count_function():
    """count"""
    global count
    count += 1
    print("output of count_increase: {}".format(count))
    total = sum(range(1, 101))
    print("sum of 1..100 = {}".format(total))
    time.sleep(1)


# Bash commands (echo, sleep, date, run a bash script, run a python file) 

BASH_ECHO = 'echo "Hello from a bash operator"'
BASH_SLEEP = "sleep 2"
BASH_DATE = "date"
BASH_SCRIPT = f"bash {SCRIPTS}/hello.sh "
BASH_PYFILE = f"{sys.executable} {SCRIPTS}/hello.py"

default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def build_dag(dag_id, schedule):
    with DAG(
        dag_id,
        default_args=default_args,
        description="HW2 Q2.1 DAG",
        schedule=schedule,
        # start_date is in the past + catchup=False:
        # Airflow skips the old intervals and runs the latest one immediately.
        start_date=datetime(2026, 10, 1),
        catchup=False,
        tags=["hw2"],
    ) as dag:
        # Bash operators (grey boxes)
        t1 = BashOperator(task_id="t1", bash_command=BASH_ECHO)
        t2 = BashOperator(task_id="t2", bash_command=BASH_SLEEP)
        t3 = BashOperator(task_id="t3", bash_command=BASH_DATE)
        t5 = BashOperator(task_id="t5", bash_command=BASH_SCRIPT)
        t6 = BashOperator(task_id="t6", bash_command=BASH_PYFILE)
        t9 = BashOperator(task_id="t9", bash_command=BASH_ECHO)
        t10 = BashOperator(task_id="t10", bash_command=BASH_SLEEP)
        t13 = BashOperator(task_id="t13", bash_command=BASH_SCRIPT)
        t14 = BashOperator(task_id="t14", bash_command=BASH_DATE)
        t17 = BashOperator(task_id="t17", bash_command=BASH_PYFILE)
        t18 = BashOperator(task_id="t18", bash_command=BASH_ECHO)

        # Python operators (yellow boxes)
        t4 = PythonOperator(task_id="t4", python_callable=print_function)
        t7 = PythonOperator(task_id="t7", python_callable=correct_sleeping_function)
        t8 = PythonOperator(task_id="t8", python_callable=count_function)
        t11 = PythonOperator(task_id="t11", python_callable=print_function)
        t12 = PythonOperator(task_id="t12", python_callable=count_function)
        t15 = PythonOperator(task_id="t15", python_callable=correct_sleeping_function)
        t16 = PythonOperator(task_id="t16", python_callable=print_function)
        t19 = PythonOperator(task_id="t19", python_callable=count_function)

        # Dependencies (read off the figure, left to right)
        t1 >> [t2, t3, t4, t5]
        t2 >> t6
        t3 >> [t7, t12]
        t5 >> [t8, t9]
        t7 >> [t13, t14, t18]
        t8 >> [t10, t15]
        t9 >> [t11, t12]
        [t10, t11, t12] >> t14
        t14 >> [t16, t17]
        [t13, t15, t17] >> t18
        [t16, t18] >> t19
    return dag


q21_manual = build_dag("q21_manual", schedule=None)
q21_every_30m = build_dag("q21_every_30m", schedule=timedelta(minutes=30))
