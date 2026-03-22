from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id="schedule_cron_dag",
    start_date=datetime(year=2026, month=3, day=20, tz='Asia/Kolkata'),
    schedule=CronTriggerTimetable("0 20 * * *", timezone="Asia/Kolkata"),
    end_date=datetime(year=2026, month=4, day=30, tz='Asia/Kolkata'),
    is_paused_upon_creation=False,
    catchup=True
)
def schedule_cron_dag():
    
    @task.python
    def first_task():
        print("This is the first task")
        
    @task.python
    def second_task():
        print("This is the second task")
        
    @task.python
    def third_task():
        print("This is the third task")
        
    # Defining the dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    
    first >> second >> third
    
schedule_cron_dag()