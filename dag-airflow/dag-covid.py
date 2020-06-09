# Importando as bibliotecas
from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.bash_operator import BashOperator
dag_id = 'consume-api-br-2'
default_args = {'owner': 'Lucas Mari',
                'start_date': datetime(2020, 6, 5)
               }
schedule = None
bash_command = "python3 /usr/local/airflow/dags/covid_analitico_new.py"
dag = DAG(dag_id, schedule_interval=schedule, default_args=default_args)
with dag:
    step_1 = BashOperator(task_id='consume-api-analitico',bash_command=bash_command)

step_1