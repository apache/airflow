# -*- coding: utf-8 -*-
import os
import sys

from typing import List
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr_serverless_spark20230808.client import Client
from alibabacloud_emr_serverless_spark20230808.models import (
    StartJobRunRequest,
    StartJobRunResponse,
    GetJobRunRequest,
    GetJobRunResponse,
    CancelJobRunRequest,
    CancelJobRunResponse,
    Tag,
    JobDriver,
    JobDriverSparkSubmit,
)

from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient


def create_client() -> Client:
    config = open_api_models.Config(
        access_key_id=os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'],
        access_key_secret=os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET']
    )
    config.endpoint = f'emr-serverless-spark.cn-hangzhou.aliyuncs.com'
    return Client(config)


def example():
    print("Let's run a simple test...")
    client = create_client()
    tags: List[Tag] = [Tag("environment", "production"), Tag("workflow", "true")]
    job_driver_spark_submit = JobDriverSparkSubmit(
        "oss://datadev-oss-hdfs-test/spark-resource/examples/jars/spark-examples_2.12-3.3.1.jar",
        ["1"],
        "--class org.apache.spark.examples.SparkPi --conf spark.executor.cores=4 --conf spark.executor.memory=20g --conf spark.driver.cores=4 --conf spark.driver.memory=8g --conf spark.executor.instances=1"
    )

    job_driver = JobDriver(job_driver_spark_submit)
    start_job_run_request = StartJobRunRequest(
        region_id="cn-hangzhou",
        resource_queue_id="root_queue",
        code_type="JAR",
        name="airflow-test",
        release_version="esr-2.1-native (Spark 3.3.1, Scala 2.12, Native Runtime)",
        tags=tags,
        job_driver=job_driver
    )
    runtime = util_models.RuntimeOptions()
    headers = {}
    try:
        response = client.start_job_run_with_options('w-ae42e9c929275cc5', start_job_run_request, headers,
                                                     runtime)
        print(response.body.to_map())
    except Exception as error:
        print(error.message)
        print(error.data.get("Recommend"))
        UtilClient.assert_as_string(error.message)


example()
