from airflow.operators.dummy import DummyOperator
from airflow import DAG, settings
import os
import json
from datetime import timedelta
from datetime import datetime

#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG = 'mapfre-from-oracle-to-azure-gen-2'
owner = 'mapfre'
email = ['miguel.peteiro@evolutio.com']
#######################################################################################

def create_dag(dag_id,
               schedule,
               query,
               args):
    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=args)

    with dag:
        t_begin = DummyOperator(task_id="begin")
        t_end = DummyOperator(task_id="end")

        t_begin >> t_end

    return dag


config_filepath = 'dags/repo/dag-config/'
        
for filename in os.listdir(config_filepath):
    f = open(config_filepath + filename)
    config = json.load(f)

    default_args = {
        'owner': owner,  # Propietario de la tarea
        'depends_on_past': False,  # Las tareas no dependen de tareas pasadas
        'email': email,
        'start_date': datetime(2021, 5, 13),
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 0,  # Numero de veces a reintentar la tarea
        'retry_delay': timedelta(minutes=1)  # Time between retries
    }

    globals()[config['DagId']] = create_dag(config['DagId'],
                                            config['Schedule'],
                                            config['Query'],
                                            default_args)