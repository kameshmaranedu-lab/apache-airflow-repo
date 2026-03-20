from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag(
    dag_id="operators_dag"
)
def operators_dag():
    
    @task.python
    def first_task():
        print("This is the first task")
        
    @task.python
    def second_task():
        print("This is the second task")
        
    @task.python
    def third_task():
        print("This is the third task")
        
    @task.bash
    def run_after_loop() -> str:
        return "echo https://airflow.apache.org/"
      
    bash_task = BashOperator(
        task_id = "bash_task",
        bash_command = "echo https://airflow.apache.org/"
    )
        
    # Defining the dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    run_this = run_after_loop()
    
    first >> second >> third >> run_this >> bash_task
    
operators_dag()