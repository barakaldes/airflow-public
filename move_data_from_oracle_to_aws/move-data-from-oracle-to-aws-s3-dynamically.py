import csv
import json
import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.models import Variable
from airflow.operators.email import EmailOperator

from operators.autonomous_oracle_to_azure_datalake.autonomous_oracle_to_azure_dataLake_operator import AutonomousOracleToAzureDataLakeOperator
from operators.autonomus_oracle_to_aws_s3.autonomous_oracle_to_aws_s3_operator import AutonomousOracleToAwsS3Operator

#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG = 'mapfre-from-oracle-to-aws-s3-dynamically'
owner = 'mapfre'
email = ['miguel.peteiro@evolutio.com']
#######################################################################################
html_email_content = """
<hr />
<p>¡Felicidades! el DAG: <strong>{{ params.name_dag }}</strong> se ha ejecutado correctamente</p>
<p>Más detalles:</p>
<p>
    Fecha de ejecucion: {{ ds }}
    <br />Siguiente ejecución programada para: {{ next_ds }}
    <br />Clave de la tarea: {{ task_instance_key_str }}
    <br />Modo test: {{ test_mode }}
    <br />Propietario: {{ task.owner}}
    <br />Hostname: {{ ti.hostname }}</p>
<hr />

¡SALUDOS!
"""


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

        t_send_email_OK = EmailOperator(
            task_id='send_email_ok',
            to=email,
            params={'name_dag': nameDAG},
            subject=f'Tarea {nameDAG} ejecutada con éxito',
            html_content=html_email_content
        )

        t_move_data_from_oracle_to_aws = AutonomousOracleToAwsS3Operator(
            task_id="move_data",

            aws__id="ORACLE_TO_AWS__AWS_CONNECTION",
            s3_bucket=Variable.get("ORACLE_TO_AWS__BUCKET"),
            bucket_path=Variable.get("ORACLE_TO_AWS__BUCKET_PATH"),

            oracle_conn_id="ORACLE_TO_AWS__ORACLE_CONNECTION",
            filename=Variable.get("ORACLE_TO_AWS__FILENAME"),
            sql=query,
            sql_params=None,
            delimiter=";",
            encoding="utf-8",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL
        )

        t_begin >> t_move_data_from_oracle_to_aws >> t_send_email_OK >> t_end

    return dag


config_filepath = 'dags/repo/move_data_from_oracle_to_aws/dag_config/'

for filename in os.listdir(config_filepath):
    f = open(config_filepath + filename)
    config = json.load(f)

    default_args = {
        'owner': owner,  # Propietario de la tarea
        'depends_on_past': False,  # Las tareas no dependen de tareas pasadas
        'email': email,
        'start_date': datetime(2021, 9, 21),
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 0,  # Numero de veces a reintentar la tarea
        'retry_delay': timedelta(minutes=1)  # Time between retries
    }

    globals()[config['DagId']] = create_dag(config['DagId'],
                                            config['Schedule'],
                                            config['Query'],
                                            default_args)
