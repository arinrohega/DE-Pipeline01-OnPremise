#SPARK WITH SUBMITOPERATOR
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

#-----------------WAIT CONFIG----------------------------------------------------------- 
#    
def wait_30seconds():
    time.sleep(30)  

#-----------------DAG CONFIG-----------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 18, 6, 58),
    'email': ['arin.rohega@gmail.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='SPARK_SUBMITOPERATORS_3PYS_2WAITS_V4',
    default_args=default_args,
    schedule_interval=None,  #Correrá despues del DAG de nifi
    catchup=False,
    tags=['spark'],
) as dag:

#-----------------OPERATORS-----------------------------------------------------------

    run_sparkjob1 = SparkSubmitOperator(
        task_id='run_sparkjob1',
        application="/opt/airflow/spark-scripts/SPARKJOB1.py",  
        conn_id="spark-conn",
        jars="/opt/airflow/spark-jars/delta-spark_2.12-3.2.0.jar,"
            "/opt/airflow/spark-jars/delta-storage-3.2.0.jar,"
            "/opt/airflow/spark-jars/spark-avro_2.12-3.5.0.jar",  
        name="SPARKJOB1",
        execution_timeout=timedelta(minutes=6),
        verbose=True
)

    wait_1 = PythonOperator(
        task_id='wait_1',
        python_callable=wait_30seconds,

    )

    run_sparkjob2 = SparkSubmitOperator(
        task_id='run_sparkjob2',
        application="/opt/airflow/spark-scripts/SPARKJOB2.py",  
        conn_id="spark-conn",
        jars="/opt/airflow/spark-jars/delta-spark_2.12-3.2.0.jar,"
            "/opt/airflow/spark-jars/delta-storage-3.2.0.jar,"
            "/opt/airflow/spark-jars/spark-avro_2.12-3.5.0.jar",  
        name="SPARKJOB2",
        execution_timeout=timedelta(minutes=6),
        verbose=True
)

    wait_2 = PythonOperator(
        task_id='wait_2',
        python_callable=wait_30seconds,
    )

    run_sparkjob3 = SparkSubmitOperator(
        task_id='run_sparkjob3',
        application="/opt/airflow/spark-scripts/SPARKJOB3.py",  
        conn_id="spark-conn",
        jars="/opt/airflow/spark-jars/delta-spark_2.12-3.2.0.jar,"
            "/opt/airflow/spark-jars/delta-storage-3.2.0.jar,"
            "/opt/airflow/spark-jars/spark-avro_2.12-3.5.0.jar",  
        name="SPARKJOB3",
        execution_timeout=timedelta(minutes=6),
        verbose=True
)

#-----------------FLOW ORDER-----------------------------------------------------------

    run_sparkjob1 >> wait_1 >> run_sparkjob2 >> wait_2 >> run_sparkjob3






