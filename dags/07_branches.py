from airflow.sdk import dag, task

@dag(
    dag_id="branches_dag"
)
def branches_dag():
    
    @task.python
    def extract_task(**kwargs):
        print("Extracting data from the source")
        ti = kwargs['ti']
        extracted_data = {'data': [1, 2, 3], 'db_data': [4, 5, 6], 's3_data': [7, 8, 9], 'weekend_flag': False}
        ti.xcom_push(key='extracted_data', value=extracted_data)
        
    @task.python
    def transform_task(**kwargs):
        print("Transforming data")
        ti = kwargs['ti']
        db_extracted_date = ti.xcom_pull(key='extracted_data', task_ids = 'extract_task')['db_data']
        print("Data extracted from the database")
        transformed_db_data = [x * 10 for x in db_extracted_date]
        ti.xcom_push(key='transformed_db_data', value=transformed_db_data)
        

    @task.python
    def transform_s3_task(**kwargs):
        print("Transforming data from S3")
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(key='extracted_data', task_ids = 'extract_task')['s3_data']
        print("Data extracted from S3")
        transformed_s3_data = [x * 100 for x in s3_extracted_data]
        ti.xcom_push(key='transformed_s3_data', value=transformed_s3_data)
      
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(task_ids='extract_task', key='extracted_data')['weekend_flag']
        if weekend_flag:
            return 'no_load_task'
        else:            
            return 'load_task'
      
    @task.bash
    def load_task(**kwargs):
        print("Loading data into the destination")
        ti = kwargs['ti']
        transformed_db_data = ti.xcom_pull(key='transformed_db_data', task_ids = 'transform_task')
        transformed_s3_data = ti.xcom_pull(key='transformed_s3_data', task_ids = 'transform_s3_task')
        return f"echo 'Transformed DB Data: {transformed_db_data}, Transformed S3 Data: {transformed_s3_data}'"
      
    @task.bash
    def no_load_task(**kwargs):
        print("No loading needed for the weekend")
        return "echo 'No loading needed for the weekend'"  
      
    # Defining the dependencies
    first = extract_task()
    second = transform_task()
    third = transform_s3_task()
    fourth = load_task()
    fifth = no_load_task()
    decision = decider_task()
    
    first >> [second, third] >> decision >>[fourth, fifth]

branches_dag()