from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Em um ambiente real, o script path apontaria para um volume montado no Airflow ou no S3.
# Aqui assumimos que os scripts estao em um diretorio acessivel.
# Exemplo local: c:/Users/afaqu/OneDrive/Documentos/pai/projetos/gx_teste
PROJECT_DIR = os.getenv("PROJECT_DIR", "c:/Users/afaqu/OneDrive/Documentos/pai/projetos/gx_teste")
SPARK_SCRIPTS_DIR = f"{PROJECT_DIR}/spark_scripts"

with DAG(
    'partition_validation_dag',
    default_args=default_args,
    description='DAG para validacao de particoes Hive e Iceberg com Great Expectations',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['data_quality', 'great_expectations', 'pyspark'],
) as dag:

    # Task para validar a tabela Hive (Particoes: year, month, day)
    validate_hive = SparkSubmitOperator(
        task_id='validate_hive_partition',
        application=f'{SPARK_SCRIPTS_DIR}/validate_hive_sot.py',
        conn_id='spark_default',
        name='airflow-gx-hive',
        verbose=True
    )

    # Task para validar a tabela Iceberg (Particao: anomesdia)
    validate_iceberg = SparkSubmitOperator(
        task_id='validate_iceberg_partition',
        application=f'{SPARK_SCRIPTS_DIR}/validate_iceberg_sot.py',
        conn_id='spark_default',
        name='airflow-gx-iceberg',
        verbose=True
    )

    # Fluxo de execucao: Roda as validacoes em paralelo
    [validate_hive, validate_iceberg]
