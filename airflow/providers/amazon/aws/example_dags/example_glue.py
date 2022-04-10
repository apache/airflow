# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor

GLUE_DATABASE_NAME = getenv('GLUE_DATABASE_NAME', 'glue_database_name')
GLUE_EXAMPLE_S3_BUCKET = getenv('GLUE_EXAMPLE_S3_BUCKET', 'glue_example_s3_bucket')

# Role needs putobject/getobject access to the above bucket as well as the glue
# service role, see docs here: https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html
GLUE_CRAWLER_ROLE = getenv('GLUE_CRAWLER_ROLE', 'glue_crawler_role')
GLUE_CRAWLER_NAME = 'example_crawler'
GLUE_CRAWLER_CONFIG = {
    'Name': GLUE_CRAWLER_NAME,
    'Role': GLUE_CRAWLER_ROLE,
    'DatabaseName': GLUE_DATABASE_NAME,
    'Targets': {
        'S3Targets': [
            {
                'Path': f'{GLUE_EXAMPLE_S3_BUCKET}/input',
            }
        ]
    },
}

# Example csv data used as input to the example AWS Glue Job.
EXAMPLE_CSV = '''
apple,0.5
milk,2.5
bread,4.0
'''

# Example Spark script to operate on the above sample csv data.
EXAMPLE_SCRIPT = f'''
from pyspark.context import SparkContext
from awsglue.context import GlueContext

glueContext = GlueContext(SparkContext.getOrCreate())
datasource = glueContext.create_dynamic_frame.from_catalog(
             database='{GLUE_DATABASE_NAME}', table_name='input')
print('There are %s items in the table' % datasource.count())

datasource.toDF().write.format('csv').mode("append").save('s3://{GLUE_EXAMPLE_S3_BUCKET}/output')
'''


@task(task_id='setup__upload_artifacts_to_s3')
def upload_artifacts_to_s3():
    '''Upload example CSV input data and an example Spark script to be used by the Glue Job'''
    s3_hook = S3Hook()
    s3_load_kwargs = {"replace": True, "bucket_name": GLUE_EXAMPLE_S3_BUCKET}
    s3_hook.load_string(string_data=EXAMPLE_CSV, key='input/input.csv', **s3_load_kwargs)
    s3_hook.load_string(string_data=EXAMPLE_SCRIPT, key='etl_script.py', **s3_load_kwargs)


with DAG(
    dag_id='example_glue',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as glue_dag:

    setup_upload_artifacts_to_s3 = upload_artifacts_to_s3()

    # [START howto_operator_glue_crawler]
    crawl_s3 = GlueCrawlerOperator(
        task_id='crawl_s3',
        config=GLUE_CRAWLER_CONFIG,
        wait_for_completion=False,
    )
    # [END howto_operator_glue_crawler]

    # [START howto_sensor_glue_crawler]
    wait_for_crawl = GlueCrawlerSensor(task_id='wait_for_crawl', crawler_name=GLUE_CRAWLER_NAME)
    # [END howto_sensor_glue_crawler]

    # [START howto_operator_glue]
    job_name = 'example_glue_job'
    submit_glue_job = GlueJobOperator(
        task_id='submit_glue_job',
        job_name=job_name,
        wait_for_completion=False,
        script_location=f's3://{GLUE_EXAMPLE_S3_BUCKET}/etl_script.py',
        s3_bucket=GLUE_EXAMPLE_S3_BUCKET,
        iam_role_name=GLUE_CRAWLER_ROLE.split('/')[-1],
        create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 2, 'WorkerType': 'G.1X'},
    )
    # [END howto_operator_glue]

    # [START howto_sensor_glue]
    wait_for_job = GlueJobSensor(
        task_id='wait_for_job',
        job_name=job_name,
        # Job ID extracted from previous Glue Job Operator task
        run_id=submit_glue_job.output,
    )
    # [END howto_sensor_glue]

    chain(setup_upload_artifacts_to_s3, crawl_s3, wait_for_crawl, submit_glue_job, wait_for_job)
