# Importando as bibliotecas
from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.bash_operator import BashOperator
dag_id = 'consume-api-br'
default_args = {'owner': 'Lucas Mari',
                'start_date': datetime(2020, 6, 5)
               }
schedule = None
dag = DAG(dag_id, schedule_interval=schedule, default_args=default_args)
with dag:
    step_1 = BashOperator(task_id='consume-api-agregado',bash_command="""python3 /usr/local/airflow/dags/covid_agregado.py""")
    step_2 = BashOperator(task_id='consume-api-analitico',bash_command="""python3 /usr/local/airflow/dags/covid_analitico.py""")

step_1>>step_2

