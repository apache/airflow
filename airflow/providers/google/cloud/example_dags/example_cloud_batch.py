from __future__ import annotations

import os
from airflow.providers.google.cloud.operators.cloud_batch import CloudBatchSubmitJobOperator, CloudBatchDeleteJobOperator, CloudBatchListJobsOperator, CloudBatchListTasksOperator
from airflow.operators.python import PythonOperator
from airflow import models

from datetime import datetime

from google.cloud import batch_v1


PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
DAG_ID = "example_gcp_cloud_batch"

region = "us-central1"
job_name_prefix = 'batch-system-test-job'
job1_name = f"{job_name_prefix}-1"
job2_name = f"{job_name_prefix}-2"

submit1_task_name = 'submit-job-1'
submit2_task_name = 'submit-job-2'

delete1_task_name = 'delete_job-1'
delete2_task_name = 'delete_job-2'

list_jobs_task_name = 'list-jobs'
list_tasks_task_name = 'list-tasks'

clean1_task_name = 'clean_job_1'
clean2_task_name = 'clean_job_2'


def _get_jobs_names(ti):
    pulled = ti.xcom_pull(task_ids=[list_jobs_task_name], key='return_value')
    print("FREDDY", pulled)
    name1 = pulled[0][0]['name'].split('/')[-1]
    name2 = pulled[0][1]['name'].split('/')[-1]
    return [name1, name2]


def _assert_tasks(ti):
    pulled = ti.xcom_pull(task_ids=[list_tasks_task_name], key='return_value')

    assert (len(pulled[0]) == 2)
    assert 'tasks/0' in pulled[0][0]['name']
    assert 'tasks/1' in pulled[0][1]['name']


def _create_job():
    runnable = batch_v1.Runnable()
    runnable.container = batch_v1.Runnable.Container()
    runnable.container.image_uri = "gcr.io/google-containers/busybox"
    runnable.container.entrypoint = "/bin/sh"
    runnable.container.commands = [
        "-c", "echo Hello world! This is task ${BATCH_TASK_INDEX}. This job has a total of ${BATCH_TASK_COUNT} tasks."]

    # Jobs can be divided into tasks. In this case, we have only one task.
    task = batch_v1.TaskSpec()
    task.runnables = [runnable]

    # We can specify what resources are requested by each task.
    resources = batch_v1.ComputeResource()
    # in milliseconds per cpu-second. This means the task requires 2 whole CPUs.
    resources.cpu_milli = 2000
    resources.memory_mib = 16  # in MiB
    task.compute_resource = resources

    task.max_retry_count = 2

    # Tasks are grouped inside a job using TaskGroups.
    # Currently, it's possible to have only one task group.
    group = batch_v1.TaskGroup()
    group.task_count = 2
    group.task_spec = task

    # Policies are used to define on what kind of virtual machines the tasks will run on.
    # In this case, we tell the system to use "e2-standard-4" machine type.
    # Read more about machine types here: https://cloud.google.com/compute/docs/machine-types
    policy = batch_v1.AllocationPolicy.InstancePolicy()
    policy.machine_type = "e2-standard-4"
    instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = policy
    allocation_policy = batch_v1.AllocationPolicy()
    allocation_policy.instances = [instances]

    job = batch_v1.Job()
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.labels = {"env": "testing", "type": "container"}

    # We use Cloud Logging as it's an out of the box available option
    job.logs_policy = batch_v1.LogsPolicy()
    job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    return job


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    submit1 = CloudBatchSubmitJobOperator(
        task_id=submit1_task_name,
        project_id=PROJECT_ID,
        region=region,
        job_name=job1_name,
        job=_create_job(),
        dag=dag,
        deferrable=False
    )

    submit2 = CloudBatchSubmitJobOperator(
        task_id=submit2_task_name,
        project_id=PROJECT_ID,
        region=region,
        job_name=job2_name,
        job=_create_job(),
        dag=dag,
        deferrable=False
    )

    list_tasks = CloudBatchListTasksOperator(
        task_id=list_tasks_task_name,
        project_id=PROJECT_ID,
        region=region,
        job_name=job1_name,
        dag=dag

    )

    assert_tasks = PythonOperator(
        task_id="assert-tasks-python",
        python_callable=_assert_tasks,
        dag=dag
    )

    list_jobs = CloudBatchListJobsOperator(
        task_id=list_jobs_task_name,
        project_id=PROJECT_ID,
        region=region,
        limit=2,
        filter=f'name:projects/{PROJECT_ID}/locations/{region}/jobs/{job_name_prefix}*',
        dag=dag
    )

    get_name = PythonOperator(
        task_id="get-names-python",
        python_callable=_get_jobs_names,
        dag=dag
    )

    delete_job1 = CloudBatchDeleteJobOperator(
        task_id='delete2',
        project_id=PROJECT_ID,
        region=region,
        job_name="{{ti.xcom_pull(task_ids=['get-names-python'], key='return_value')[0][0]}}",
        dag=dag,

    )

    delete_job2 = CloudBatchDeleteJobOperator(
        task_id='delete3',
        project_id=PROJECT_ID,
        region=region,
        job_name="{{ti.xcom_pull(task_ids=['get-names-python'], key='return_value')[0][1]}}",
        dag=dag,

    )

    # clean_job1 = CloudBatchDeleteJobOperator(
    #     task_id=clean1_task_name,
    #     project_id=PROJECT_ID,
    #     region=region,
    #     job_name=job1_name,
    #     dag=dag,
    #     trigger_rule="one_failed"
    # )

    # clean_job2 = CloudBatchDeleteJobOperator(
    #     task_id=clean2_task_name,
    #     project_id=PROJECT_ID,
    #     region=region,
    #     job_name=job2_name,
    #     dag=dag,
    #     trigger_rule="one_failed"
    #  )

    # submit1>>submit2>>list_tasks>>assert_tasks>>list_jobs>>get_name>>delete_job1>>delete_job2

    (submit1, submit2) >> list_tasks >> assert_tasks >> list_jobs >> get_name >> (delete_job1, delete_job2)
    # delete_job1 >> clean_job1
    # delete_job2 >> clean_job2

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

