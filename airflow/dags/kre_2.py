import os
from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email_operator import EmailOperator

TEMP_DIR = os.environ['TEMP_DIR']
MAX_BAD_RECORDS = 10

yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'max_active_runs': 1,
    'start_date': yesterday,
    'email': ['data-guild-alerts@kin.org'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('kre_2', schedule_interval='0 12 * * *',
         default_args=default_args) as dag:

    previous_run_waiter = ExternalTaskSensor(
        task_id='wait_for_previous',
        external_dag_id=dag.dag_id,
        # execution_delta - used to get the instance of the
        # previous DAG execution; if task is croned every minute,
        # it should be a minute. Unless you've dependencies on other
        # tasks from the past
        execution_delta=timedelta(minutes=1),
        external_task_id='backfill_kre_payouts_post_monopoly')

    kre_2_delete = BigQueryOperator(
        task_id='kre_2_delete',
        bql='sqls/kre_2_delete.sql',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    backfill_spenders = BigQueryOperator(
        task_id='backfill_spenders',
        bql='sqls/backfill_spenders.sql',
        destination_dataset_table='kre_2.spender_buckets',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    backfill_holdings = BigQueryOperator(
        task_id='backfill_holdings',
        bql='sqls/backfill_holdings.sql',
        destination_dataset_table='kre_2.holding',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    backfill_buy_demand = BigQueryOperator(
        task_id='backfill_buy_demand',
        bql='sqls/backfill_buy_demand.sql',
        destination_dataset_table='kre_2.buy_demand',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    backfill_kre_payouts_pre_monopoly = BigQueryOperator(
        task_id='backfill_kre_payouts_pre_monopoly',
        bql='sqls/backfill_kre_payouts_pre_monopoly.sql',
        destination_dataset_table='kre_2.kre_2_payouts_pre_monopoly',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    backfill_kre_payouts_post_monopoly = BigQueryOperator(
        task_id='backfill_kre_payouts_post_monopoly',
        bql='sqls/backfill_kre_payouts_post_monopoly.sql',
        destination_dataset_table='kre_2.kre_2_payouts',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False
    )

    previous_run_waiter >> kre_2_delete >> backfill_spenders >> backfill_holdings >> backfill_buy_demand >> backfill_kre_payouts_pre_monopoly >> backfill_kre_payouts_post_monopoly
