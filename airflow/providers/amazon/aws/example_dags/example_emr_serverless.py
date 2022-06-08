from datetime import datetime
from os import getenv
from tracemalloc import start


from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.emr import EmrServerlessCreateApplicationOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessDeleteApplicationOperator
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessApplicationSensor
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor


EXECUTION_ROLE_ARN = getenv('EXECUTION_ROLE_ARN', 'execution_role_arn')
EMR_EXAMPLE_BUCKET = getenv('EMR_EXAMPLE_BUCKET', 'emr_example_bucket')
SPARK_JOB_DRIVER = {
    "sparkSubmit": {
        "entryPoint": "s3://us-east-1.elasticmapreduce/emr-containers/samples/wordcount/scripts/wordcount.py",
        "entryPointArguments": [f"s3://{EMR_EXAMPLE_BUCKET}/output"],
        "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1"
    }
}

SPARK_CONFIGURATION_OVERRIDES = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": f"s3://{EMR_EXAMPLE_BUCKET}/logs"
        }
    }
}

with DAG(
    dag_id='example_emr_serverless',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as emr_serverless_dag:

    # [START howto_operator_emr_serverless_create_application]
    emr_serverless_app = EmrServerlessCreateApplicationOperator(
        task_id = 'create-emr-serverless-task',
        release_label='emr-6.5.0-preview',
        job_type="SPARK",
        config={
            'name': 'new-application'
        }
    )
    # [END howto_operator_emr_serverless_create_application]

    # [START howto_sensor_emr_serverless_application]
    wait_for_app_creation = EmrServerlessApplicationSensor(task_id='wait_for_app_creation', application_id=emr_serverless_app.output)
    # [END howto_sensor_emr_serverless_application]

    # [START howto_operator_emr_serverless_start_job]
    start_job = EmrServerlessStartJobOperator(
        task_id='start_emr_serverless_job',
        application_id=emr_serverless_app.output,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver=SPARK_JOB_DRIVER,
        configuration_overrides=SPARK_CONFIGURATION_OVERRIDES
    )
    # [END howto_operator_emr_serverless_start_job]

    # [START howto_sensor_emr_serverless_job]
    wait_for_job = EmrServerlessJobSensor(task_id='wait-for-job', application_id=emr_serverless_app.output, job_run_id=start_job.output)
    # [END howto_sensor_emr_serverless_job]

    # [START howto_operator_emr_serverless_delete_application]
    delete_app = EmrServerlessDeleteApplicationOperator(task_id='delete-application', application_id=emr_serverless_app.output, trigger_rule="all_done")
    # [END howto_operator_emr_serverless_delete_application]
    
    chain(emr_serverless_app, wait_for_app_creation, start_job, wait_for_job, delete_app)