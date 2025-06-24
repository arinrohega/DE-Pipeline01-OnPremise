#-----------------IMPORTS-----------------------------------------------------------

from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.bash import BashOperator
from hdfs import InsecureClient
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import time

#-----------------CUSTOM SENSOR CONFIG-----------------------------------------------------------
class NewFolderSensor(BaseSensorOperator):

    def __init__(self, hdfs_path, hdfs_conn_id='webhdfs_default', **kwargs):
        super().__init__(**kwargs)
        self.hdfs_path = hdfs_path
        self.hdfs_conn_id = hdfs_conn_id

    def poke(self, context):
        client = InsecureClient('http://hadoop-namenode:9870', user='usuario')
        
        
        if not hasattr(self, 'initial_folders'):
            self.initial_folders = set(client.list(self.hdfs_path))
            return False
        
        
        current_folders = set(client.list(self.hdfs_path))
        return not current_folders.issubset(self.initial_folders)
    
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
    dag_id='1_NIFI_EXECUTION_10PGS_RUNDAG3',
    default_args=default_args,
    schedule_interval='58 6 * * *', # Local: 11:45pm UTC-7 => 6:45am UTC
    catchup=False,
    tags=['nifi'],
) as dag:

#-----------------GET TOKEN-----------------------------------------------------------
    get_token = BashOperator(
        task_id='get_token',
        bash_command='curl -k -s -X POST https://nifi:8443/nifi-api/access/token '
                    '-d \'username=admin&password={{ var.value.get("PSS_ADMIN") }}\'',
        do_xcom_push=True,  # Xcom save
    )

#------------------RUN AND STOP PG 1-------------------------------------------------
    start_pg1 = BashOperator(
        task_id='start_pg1',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG1') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "RUNNING",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "RUNNING"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
    )

    folder_appeared_pg1 = NewFolderSensor(
        task_id='folder_appeared_pg1',
        hdfs_path='/user/kerberos/TEST_ETL/STAGING_BUCKET/sucursales',
        timeout=180,  
        poke_interval=15,
        mode='poke'
    )

    stop_pg1 = BashOperator(
        task_id='stop_pg1',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG1') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "STOPPED",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "STOPPED"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
        trigger_rule='all_done',
    )
#------------------RUN AND STOP PG 2-------------------------------------------------
    start_pg2 = BashOperator(
        task_id='start_pg2',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG2') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "RUNNING",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "RUNNING"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
    )

    folder_appeared_pg2 = NewFolderSensor(
        task_id='folder_appeared_pg2',
        hdfs_path='/user/kerberos/TEST_ETL/STAGING_BUCKET/regiones',
        timeout=180,  
        poke_interval=15,
        mode='poke'
    )

    stop_pg2 = BashOperator(
        task_id='stop_pg2',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG2') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "STOPPED",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "STOPPED"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
        trigger_rule='all_done',
    )
#------------------RUN AND STOP PG 3-------------------------------------------------
    start_pg3 = BashOperator(
        task_id='start_pg3',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG3') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "RUNNING",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "RUNNING"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
    )

    folder_appeared_pg3 = NewFolderSensor(
        task_id='folder_appeared_pg3',
        hdfs_path='/user/kerberos/TEST_ETL/STAGING_BUCKET/prospectos',
        timeout=180,  
        poke_interval=15,
        mode='poke'
    )

    stop_pg3 = BashOperator(
        task_id='stop_pg3',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG3') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "STOPPED",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "STOPPED"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
        trigger_rule='all_done',
    )
#------------------RUN AND STOP PG 4-------------------------------------------------
    start_pg4 = BashOperator(
        task_id='start_pg4',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG4') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "RUNNING",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "RUNNING"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
    )

    folder_appeared_pg4 = NewFolderSensor(
        task_id='folder_appeared_pg4',
        hdfs_path='/user/kerberos/TEST_ETL/STAGING_BUCKET/zonas',
        timeout=180,  
        poke_interval=15,
        mode='poke'
    )

    stop_pg4 = BashOperator(
        task_id='stop_pg4',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG4') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "STOPPED",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "STOPPED"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
        trigger_rule='all_done',
    )
#------------------RUN AND STOP PG 5-------------------------------------------------
    start_pg5 = BashOperator(
        task_id='start_pg5',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG5') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "RUNNING",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "RUNNING"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
    )

    folder_appeared_pg5 = NewFolderSensor(
        task_id='folder_appeared_pg5',
        hdfs_path='/user/kerberos/TEST_ETL/STAGING_BUCKET/referidos_origenes',
        timeout=180,  
        poke_interval=15,
        mode='poke'
    )

    stop_pg5 = BashOperator(
        task_id='stop_pg5',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG5') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "STOPPED",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "STOPPED"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
        trigger_rule='all_done',
    )
#------------------RUN AND STOP PG 6-------------------------------------------------
    start_pg6 = BashOperator(
        task_id='start_pg6',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG6') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "RUNNING",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "RUNNING"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
    )

    folder_appeared_pg6 = NewFolderSensor(
        task_id='folder_appeared_pg6',
        hdfs_path='/user/kerberos/TEST_ETL/STAGING_BUCKET/referidos_asignaciones',
        timeout=180,  
        poke_interval=15,
        mode='poke'
    )

    stop_pg6 = BashOperator(
        task_id='stop_pg6',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG6') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "STOPPED",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "STOPPED"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
        trigger_rule='all_done',
    )
#------------------RUN AND STOP PG 7-------------------------------------------------
    start_pg7 = BashOperator(
        task_id='start_pg7',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG7') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "RUNNING",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "RUNNING"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
    )

    folder_appeared_pg7 = NewFolderSensor(
        task_id='folder_appeared_pg7',
        hdfs_path='/user/kerberos/TEST_ETL/STAGING_BUCKET/distribuidores',
        timeout=180,  
        poke_interval=15,
        mode='poke'
    )

    stop_pg7 = BashOperator(
        task_id='stop_pg7',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG7') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "STOPPED",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "STOPPED"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
        trigger_rule='all_done',
    )
#------------------RUN AND STOP PG 8-------------------------------------------------
    start_pg8 = BashOperator(
        task_id='start_pg8',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG8') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "RUNNING",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "RUNNING"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
    )

    folder_appeared_pg8 = NewFolderSensor(
        task_id='folder_appeared_pg8',
        hdfs_path='/user/kerberos/TEST_ETL/STAGING_BUCKET/distribuidores_perfiles',
        timeout=180,  
        poke_interval=15,
        mode='poke'
    )

    stop_pg8 = BashOperator(
        task_id='stop_pg8',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG8') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "STOPPED",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "STOPPED"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
        trigger_rule='all_done',
    )
#------------------RUN AND STOP PG 9-------------------------------------------------
    start_pg9 = BashOperator(
        task_id='start_pg9',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG9') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "RUNNING",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "RUNNING"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
    )

    folder_appeared_pg9 = NewFolderSensor(
        task_id='folder_appeared_pg9',
        hdfs_path='/user/kerberos/TEST_ETL/STAGING_BUCKET/ventas',
        timeout=180,  
        poke_interval=15,
        mode='poke'
    )

    stop_pg9 = BashOperator(
        task_id='stop_pg9',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG9') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "STOPPED",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "STOPPED"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
        trigger_rule='all_done',
    )
#------------------RUN AND STOP PG 10-------------------------------------------------
    start_pg10 = BashOperator(
        task_id='start_pg10',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG10') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "RUNNING",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "RUNNING"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
    )

    folder_appeared_pg10 = NewFolderSensor(
        task_id='folder_appeared_pg10',
        hdfs_path='/user/kerberos/TEST_ETL/STAGING_BUCKET/afiliacion_usuarios',
        timeout=180,  
        poke_interval=15,
        mode='poke'
    )

    stop_pg10 = BashOperator(
        task_id='stop_pg10',
        bash_command="""
        TOKEN="{{ ti.xcom_pull(task_ids='get_token') }}"
        PG_ID="{{ var.value.get('ID_PG10') }}"
        
        curl -k -X PUT "https://nifi:8443/nifi-api/flow/process-groups/${PG_ID}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d '{
            "id": "'"${PG_ID}"'",
            "state": "STOPPED",
            "component": {
                "id": "'"${PG_ID}"'",
                "state": "STOPPED"
            },
            "revision": {
                "clientId": "Airflow_dag"
            }
        }'
        """,
        trigger_rule='all_done',
    )
#-----------------TRIGGER NEXT DAG-----------------------------------------------------------
    run_dag2 = TriggerDagRunOperator(
        task_id='run_dag2',
        trigger_dag_id='SPARK_SUBMITOPERATORS_3PYS_2WAITS_V4',
        execution_date='2025-06-18T06:58:00+00:00',
        reset_dag_run=True,
        trigger_rule='all_success',
        retries= 3,
        retry_delay=timedelta(seconds=30)
    )
#-----------------FLOW-----------------------------------------------------------

    stop_tasks = [stop_pg1, stop_pg2, stop_pg3, stop_pg4, stop_pg5,
             stop_pg6, stop_pg7, stop_pg8, stop_pg9, stop_pg10]

    folder_appeared_pg1 >> stop_pg1  
    folder_appeared_pg2 >> stop_pg2
    folder_appeared_pg3 >> stop_pg3
    folder_appeared_pg4 >> stop_pg4
    folder_appeared_pg5 >> stop_pg5
    folder_appeared_pg6 >> stop_pg6
    folder_appeared_pg7 >> stop_pg7
    folder_appeared_pg8 >> stop_pg8
    folder_appeared_pg9 >> stop_pg9
    folder_appeared_pg10 >> stop_pg10

    get_token >> [start_pg1, start_pg2, start_pg3, start_pg4, start_pg5,
                 start_pg6, start_pg7, start_pg8, start_pg9, start_pg10]

    stop_tasks >> run_dag2







