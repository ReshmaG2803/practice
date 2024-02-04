import datetime


from datetime import datetime,timedelta

from airflow import models

from airflow.models import Variable

from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator

project_id = Variable.get('project_id')
region = Variable.get('region')

def generate_random_number(**kwargs):
    import random
    number=random.randint(1,5)
    kwargs['ti'].xcom_push(key='random_number',value='number')
    print(f'random number is {number}')

def square_number(**kwargs):
    ti=kwargs['ti']
    result=ti.xcom_pull(task_ids='generate_random_number',key='random_number')
    final_result= result ** 2
    print(f'square of {number} is {final_result}')

default_args={
    "depends_on_past":False,
    "start_date"       : datetime(2024,1,1),
    "email_id"        :['reshma.gowrabathini@gmail.com'],
    "email_on_failure" :False,
    "email_on_retry"   :False,
    "retries"           :1,
}


with models.DAG(
    "practice_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag :


     task_1 = PythonOperator(
         task_id = 'generate_random_number',
         python_callable = generate_random_number,
         provide_context =True,
         dag=dag ,
     )

     task_2 = PythonOperator(
         task_id='square_number',
         python_callable =square_number,
         provide_context=True,
         dag=dag,
     )

     task_1>>task_2
