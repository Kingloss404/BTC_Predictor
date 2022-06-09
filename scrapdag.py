from airflow.operators.dummy_operator    import DummyOperator
from airflow.operators.python_operator    import PythonOperator


from omega_plugin_file import OmegaFileSensor, ArchiveFileOperator


from airflow.providers.papermill.operators.papermill import PapermillOperator

import datetime
from datetime import date, timedelta
import airflow



default_args = {
    "depends_on_past" : False,
    "start_date"      : airflow.utils.dates.days_ago( 1 ),
    "retries"         : 1,
    "retry_delay"     : datetime.timedelta(hours=1 ),
}



task_name = 'check_file'


with airflow.DAG( "btc_predict", default_args= default_args, schedule_interval= "@daily"  ) as dag:
    start_task  = DummyOperator(  task_id= "start" )
    stop_task   = DummyOperator(  task_id= "stop"  )
    
    
    process_file_notebook1 = PapermillOperator(
        task_id="process_file_notebook1",
        input_nb="/home/adminbi/Predict/Scrapper.ipynb",
        output_nb="/home/adminbi/Predict/outs/out-Scrapper{{ execution_date }}.ipynb",
        parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"},
    )
   
    process_file_notebook2 = PapermillOperator(
        task_id="process_file_notebook2",
        input_nb="/home/adminbi/Predict/Blending.ipynb",
        output_nb="/home/adminbi/Predict/outs/out-Blending{{ execution_date }}.ipynb",
        parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"},
    )

    sensor_task = OmegaFileSensor(
      task_id='check_new_file', 
      filepath="/home/adminbi/Predict/raw_btc/", 
      filepattern=r"\b(\w*.csv)", 
      poke_interval=10, 
      dag=dag 
    )
    
    
   
    
   
start_task >> process_file_notebook1 >> sensor_task >> process_file_notebook2 >> stop_task