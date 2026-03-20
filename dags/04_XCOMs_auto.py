from airflow.sdk import dag, task

@dag(
    dag_id="xcoms_auto_dag"
)
def xcoms_auto_dag():
    
    @task.python
    def first_task():
        print("Extracting data from the source")
        fetched_data = {'data': [1, 2, 3], 'name': 'Airflow'}
        return fetched_data
        
    @task.python
    def second_task(data_from_first_task: dict):
        print("Transforming the data")
        transformed_data = [x * 2 for x in data_from_first_task['data']]
        transformed_data_dict = {
            'transformed_data': transformed_data,
            'name': data_from_first_task['name']
        }
        return transformed_data_dict

    @task.python
    def third_task(data_from_second_task: dict):
        print("Loading the data into the destination")
        return data_from_second_task
        
    # Defining the dependencies
    first = first_task()
    second = second_task(first)
    third = third_task(second)
    
    first >> second >> third
    
xcoms_auto_dag()