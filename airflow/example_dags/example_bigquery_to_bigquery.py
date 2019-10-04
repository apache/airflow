import airflow
from airflow.gcp.operators.bigquery import BigQueryTableDeleteOperator
from airflow.models import DAG
from airflow.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

with DAG(
    dag_id='example_bigquery_to_bigquery',
    default_args=args,
    schedule_interval=None,
) as dag:
    # [START howto_operator_bq_to_bq]
    copy_table = BigQueryToBigQueryOperator(
        task_id='copy_table',
        source_project_dataset_tables='bigquery-public-data.baseball.schedules',
        destination_project_dataset_table='bigquery-public-data.test.schedules',
        write_disposition='WRITE_EMPTY',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='bigquery_default')
    # [END howto_operator_bq_to_bq]

    delete_test_dataset = BigQueryTableDeleteOperator(
        task_id='delete_test_dataset',
        deletion_dataset_table='bigquery-public-data.test.schedules')
