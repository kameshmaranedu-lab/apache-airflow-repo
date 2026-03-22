from airflow.sdk import dag, task

@dag(
    dag_id="xcoms_kwargs_dag"
)
def xcoms_kwargs_dag():
    
    @task.python
    def first_task(**kwargs):
        # Extracting 'ti' from the kwargs to demonstrate how to access it - task instance (ti) is used to push and pull XComs
        ti = kwargs['ti']
        ti.xcom_push(key='fetched_data', value={'data': [1, 2, 3], 'name': 'Airflow'})
        print("Data has been pushed to XComs")
        return "Data extraction complete"
        
    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']
        data_from_first_task = ti.xcom_pull(key='fetched_data', task_ids='first_task')
        print("Data pulled from XComs in second task:", data_from_first_task)
        transformed_data = [x * 2 for x in data_from_first_task['data']]
        transformed_data_dict = {
            'transformed_data': transformed_data,
            'name': data_from_first_task['name']
        }
        ti.xcom_push(key='transformed_data', value=transformed_data_dict)
        print("Transformed data has been pushed to XComs")
        return "Data transformation complete"
        

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        data_from_second_task = ti.xcom_pull(key='transformed_data', task_ids='second_task')
        print("Data pulled from XComs in third task:", data_from_second_task)
        return data_from_second_task
        
    # Defining the dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    
    first >> second >> third
    
xcoms_kwargs_dag()