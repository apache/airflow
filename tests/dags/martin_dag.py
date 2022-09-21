from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

with DAG(dag_id='trigger_airbyte_job_pipedrive',
         default_args={'owner': 'martin'},
         schedule_interval='@once',
         start_date=days_ago(1)
    ) as dag:

    money_to_json = AirbyteTriggerSyncOperator(
        task_id='airbyte_restack_sandbox-aws_pipedrive',
        airbyte_conn_id='airbyte_restack_sandbox-aws',
        connection_id='35c4a220-ad73-406a-8a97-019f7112e46d',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )
