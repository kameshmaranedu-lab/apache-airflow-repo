from airflow.sdk import dag, task

@dag(
    dag_id="versioned_dag"
)
def versioned_dag():
    
    @task.python
    def first_task():
        print("This is the first task")
        
    @task.python
    def second_task():
        print("This is the second task")
        
    @task.python
    def third_task():
        print("This is the third task")
        
    @task.python
    def version_task():
        print("This is the version dag. DAG version 2.0!")
        
    @task.python
    def fifth_task():
        print("This is the version dag. DAG version 3.0!")
    
    # Defining the dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    fourth = version_task()
    fifth = fifth_task()
    
    first >> second >> third >> fourth >> fifth
    
versioned_dag()