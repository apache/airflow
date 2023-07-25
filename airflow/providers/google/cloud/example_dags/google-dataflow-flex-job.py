from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.dataflow import CheckJobRunning
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "google-dataflow-flex-job"

with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataflow"],
) as dag:
    start_template_job = DataflowStartFlexTemplateOperator(
        task_id="start_template_job",
        project_id="google.com:clouddfe",
        location="us-central1",
        body={
          "launchParameter": {
             "jobName": "wordcount-airflow-test",
             "containerSpecGcsPath": "gs://xianhualiu-bucket-1/templates/word-count.json",
             "parameters": {
                 "inputFile": "gs://xianhualiu-bucket-1/examples/kinglear.txt", 
                 "output": "gs://xianhualiu-bucket-1/results/kinglear"
              }
           }
        },
        wait_until_finished=False
    )    
    
    start_template_job
