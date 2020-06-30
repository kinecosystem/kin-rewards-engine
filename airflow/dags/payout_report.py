import os
from datetime import timedelta, datetime, date

from airflow import DAG
from airflow import models
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email_operator import EmailOperator
from jobs.csvwriter import CSVGenerator
from jobs.jsonwriter import JSONGenerator
from jobs.xdrwriter import XDRGenerator

import pandas as pd
import asyncio

TEMP_DIR = os.environ['TEMP_DIR']
MAX_BAD_RECORDS = 10

last_week_day = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
write_directory = os.environ['TEMP_DIR'] + "/KRE/payout_files/"
gcs_directory = 'kre_payouts/' 

def get_gcs_directory():
    return gcs_directory

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'max_active_runs': 1,
    'start_date': last_week_day,
    'email': ['***'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def create_csv(ds, **context):
    start_date = datetime.strptime(ds, '%Y-%m-%d') - timedelta(21)
    end_date = datetime.strptime(ds, '%Y-%m-%d') - timedelta(14)
  
    write_directory = os.environ['TEMP_DIR'] + "/KRE/payout_files/"  + ds + '/'
    if not os.path.exists(write_directory):
        os.makedirs(write_directory)
    payout_info = pd.read_gbq('select * from kre_2.get_total_payout_information_tmp', 'kin-bi', dialect='standard')
    payout_info_date = pd.read_gbq('select * from kre_2.get_total_payout_information_by_date_tmp', 'kin-bi', dialect='standard')
    spender_info = pd.read_gbq('select * from kre_2.get_spender_information_tmp', 'kin-bi', dialect='standard')
    holding_info = pd.read_gbq('select * from kre_2.get_holding_information_tmp', 'kin-bi', dialect='standard')
    buying_info = pd.read_gbq('select * from kre_2.get_buying_information_tmp', 'kin-bi', dialect='standard')
    prior_payout_info = pd.read_gbq('select * from kre_2.get_prior_information_tmp', 'kin-bi', dialect='standard')
    CSVGenerator.create_payout_csv(payout_info, payout_info_date, spender_info, holding_info, buying_info, prior_payout_info, start_date, end_date, write_directory)

def create_json(ds, **context):
    write_directory = os.environ['TEMP_DIR'] + "/KRE/payout_files/"  + ds + '/'
    start_date = (datetime.strptime(ds, '%Y-%m-%d') - timedelta(21)).strftime("%Y-%m-%d")
    end_date_incl = (datetime.strptime(ds, '%Y-%m-%d') - timedelta(15)).strftime("%Y-%m-%d")
    payout_info = pd.read_gbq('select * from kre_2.get_total_payout_information_tmp', 'kin-bi', dialect='standard')  
    JSONGenerator.create_payout_json(payout_info, start_date, end_date_incl, write_directory)

def create_xdr(ds, **context):
    write_directory = os.environ['TEMP_DIR'] + "/KRE/payout_files/"  + ds + '/'
    src_wallet = 'GD2YFOMTV424PS3XKOF7IRAPHK36K4I3PGE6JNXA3OYYQTTX5X5CO5JN'
    start_date = (datetime.strptime(ds, '%Y-%m-%d') - timedelta(21)).strftime("%Y-%m-%d")
    end_date = (datetime.strptime(ds, '%Y-%m-%d') - timedelta(14)).strftime("%Y-%m-%d")
    payout_info = pd.read_gbq('select * from kre_2.get_total_payout_information_tmp', 'kin-bi', dialect='standard')
    loop1 = asyncio.get_event_loop()
    loop1.run_until_complete(XDRGenerator.create_payout_xdr(payout_info[:25], start_date, end_date, src_wallet, write_directory, digital_service_id_kin_2))

    loop2 = asyncio.get_event_loop()
    loop2.run_until_complete(XDRGenerator.create_payout_xdr(payout_info[-(len(payout_info.index)-25):], start_date, end_date, src_wallet, write_directory, digital_service_id_kin_2, kin_2_amount, 1))

with DAG('payout_report', schedule_interval='0 0 * * 0',
         default_args=default_args) as dag:

    get_total_payout_information = BigQueryOperator(
        task_id='get_total_payout_information',
        bql='sqls/get_total_payout_information.sql',
        destination_dataset_table='kre_2.get_total_payout_information_tmp',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    get_total_payout_information_by_date = BigQueryOperator(
        task_id='get_total_payout_information_by_date',
        bql='sqls/get_total_payout_information_by_date.sql',
        destination_dataset_table='kre_2.get_total_payout_information_by_date_tmp',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    get_spender_information = BigQueryOperator(
        task_id='get_spender_information',
        bql='sqls/get_spender_information.sql',
        destination_dataset_table='kre_2.get_spender_information_tmp',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    get_holding_information = BigQueryOperator(
        task_id='get_holding_information',
        bql='sqls/get_holding_information.sql',
        destination_dataset_table='kre_2.get_holding_information_tmp',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    get_buying_information = BigQueryOperator(
        task_id='get_buying_information',
        bql='sqls/get_buying_information.sql',
        destination_dataset_table='kre_2.get_buying_information_tmp',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    get_prior_information = BigQueryOperator(
        task_id='get_prior_information',
        bql='sqls/get_prior_information.sql',
        destination_dataset_table='kre_2.get_prior_information_tmp',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    create_csv = PythonOperator(
        task_id='create_csv',
        python_callable=create_csv,
        provide_context=True
    )

    create_json = PythonOperator(
        task_id='create_json',
        python_callable=create_json,
        provide_context=True
    )

    create_xdr = PythonOperator(
        task_id='create_xdr',
        python_callable=create_xdr,
        provide_context=True
    )

    push = {}
    for k,v in {'json': 'kre_payout_leaderboard.json', 'csv': 'payout.csv', 'xlsx': 'payout.xlsx', 'xdr0': 'xdr0.xdr', 'xdr1': 'xdr1.xdr'}.items():
        push[k] = FileToGoogleCloudStorageOperator(
            task_id=k,
            src=write_directory + '/{{ ds }}/' + v,
            dst=gcs_directory + '{{ ds }}/' + v,
            bucket='kin-kre-payouts',
            google_cloud_storage_conn_id='google_cloud_default',
        )

    get_total_payout_information >> create_csv 
    get_total_payout_information_by_date >> create_csv
    get_spender_information >> create_csv
    get_holding_information >> create_csv
    get_buying_information >> create_csv
    get_prior_information >> create_csv

    create_csv >> create_json

    create_csv >> create_xdr 

    create_csv >> push['csv']
    create_csv >> push['xlsx']
    create_json >> push['json']
    create_xdr >> push['xdr0']
    create_xdr >> push['xdr1']
