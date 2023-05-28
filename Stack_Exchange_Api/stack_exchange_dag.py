from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    'run_jupyter_notebook_code',
    description='DAG to run Jupyter Notebook as a job for local machine',
    schedule_interval= '@daily',
    start_date=datetime(2023, 5, 29)
)

# Define the BashOperator to run the Jupyter Notebook command
run_jupyter_notebook = BashOperator(
    task_id='run_jupyter_notebook_code',
    bash_command='jupyter nbconvert --to notebook --execute C:\Users\DurgaprasadChou\Desktop\Stack_Exchange_Code.ipynb',
    dag=dag
)

# Set the task dependencies
run_jupyter_notebook

