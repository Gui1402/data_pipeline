  
from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3
from io import BytesIO
import requests
import os
import zipfile

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
glue = boto3.client('glue', region_name='us-east-2',
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)

from airflow.utils.dates import days_ago


def trigger_crawler_final_func():
        glue.start_crawler(Name='enade_table')

def download_data() -> None:

    os.makedirs('./enade', exist_ok=True) ## if exists just pass
    url = 'https://download.inep.gov.br/microdados/Enade_Microdados/microdados_Enade_2017_portal_2018.10.09.zip'
    ## do download content
    print('\nDownloading files ...')
    file_bytes = BytesIO(requests.get(url).content)
    myzip = zipfile.ZipFile(file_bytes) ## python understand file as zip
    myzip.extractall('./enade')
    print('\nDone')


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    os.makedirs('./enade', exist_ok=True) ## if exists just pass
    url = 'https://download.inep.gov.br/microdados/Enade_Microdados/microdados_Enade_2017_portal_2018.10.09.zip'
    ## do download content
    print('\nDownloading files ...')
    file_bytes = BytesIO(requests.get(url).content)
    myzip = zipfile.ZipFile(file_bytes) ## python understand file as zip
    myzip.extractall('./enade')
    print('\nDone\n')
    print(os.listdir())
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(
                    's3', 
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)
    response = s3_client.upload_file(file_name, bucket, object_name)
    return response

with DAG(
    'enem_batch_spark_k8s',
    default_args={
        'owner': 'Guilherme',
        'depends_on_past': False,
        'email': ['guilherme.lopes@engenharia.ufjf.br'],
        'email_on_failure': False,
        'email_on_retry': False,
        'max_active_runs': 1,
    },
    description='submit spark-pi as sparkApplication on kubernetes',
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'kubernetes', 'batch', 'enade'],
) as dag:

    # download_data = PythonOperator(
    #     task_id='download_enade',
    #     python_callable=download_data
    # )

    upload_file = PythonOperator(
        task_id='upload_s3',
        python_callable=upload_file,
        op_kwargs={
            'file_name': './enade/3.DADOS/MICRODADOS_ENADE_2017.txt',
            'bucket': 'bootcamp-igti-gui-enade',
            'object_name': 'raw/enade/year=2017/enade.csv'}
    )

    converte_parquet = SparkKubernetesOperator(
        task_id='converte_parquet',
        namespace="airflow",
        application_file="convert_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_enade = PythonOperator(
        task_id='trigger_crawler_enade',
        python_callable=trigger_crawler_final_func,
    )

    

upload_file >> converte_parquet >> converte_parquet_monitor
converte_parquet_monitor >> trigger_crawler_enade