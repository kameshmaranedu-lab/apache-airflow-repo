from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable([
    datetime(year=2026, month=3, day=3, tz='Asia/Kolkata'),
    datetime(year=2026, month=6, day=10, tz='Asia/Kolkata'),
    datetime(year=2026, month=8, day=12, tz='Asia/Kolkata')
])

@dag(
    dag_id="special_dates_dag",
    schedule=special_dates,
    start_date=datetime(year=2026, month=3, day=1, tz='Asia/Kolkata'),
    end_date=datetime(year=2026, month=8, day=15, tz='Asia/Kolkata'),
    catchup=True,
    is_paused_upon_creation=False
)
def special_dates_dag():
    
    @task.python
    def print_special_date(**kwargs):
        execution_date = kwargs['logical_date']
        print(f"Executing task for special date: {execution_date}")
        
    print_special_date()
    
special_dates_dag()