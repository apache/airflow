import airflow
from airflow.models import DAG
from airflow.operators import bash_operator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='example_bigquery_to_bigquery',
    default_args=args,
    schedule_interval="@daily",
)

# [START howto_operator_bq_to_bq]
copy_table = BigQueryToBigQueryOperator(
    task_id='bq_to_bq_example',
    source_project_dataset_tables='bigquery-public-data.baseball.schedules',
    destination_project_dataset_table='bigquery-public-data.test.schedules',
    write_disposition='WRITE_EMPTY',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='bigquery_default',
    encryption_configuration = {
        "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
    },
    dag=dag)
# [END howto_operator_bq_to_bq]

delete_test_dataset = bash_operator.BashOperator(
    task_id='delete_airflow_test_dataset',
    bash_command='bq rm -f -t bigquery-public-data.test.schedules',
    dag=dag)