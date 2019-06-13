# -*- coding: utf-8 -*-
#
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
import itertools
import unittest
from copy import deepcopy
from datetime import date, time
from typing import Dict

from parameterized import parameterized
from botocore.credentials import Credentials

from airflow import AirflowException, configuration
from airflow.contrib.hooks.gcp_transfer_hook import (
    FILTER_JOB_NAMES,
    SCHEDULE_START_DATE,
    SCHEDULE_END_DATE,
    START_TIME_OF_DAY,
    STATUS,
    NAME,
    AWS_S3_DATA_SOURCE,
    GCS_DATA_SOURCE,
    GCS_DATA_SINK,
    AWS_ACCESS_KEY,
    ACCESS_KEY_ID,
    SECRET_ACCESS_KEY,
    BUCKET_NAME,
    SCHEDULE,
    TRANSFER_SPEC,
    HTTP_DATA_SOURCE,
    LIST_URL,
)
from airflow.contrib.operators.gcp_transfer_operator import (
    GcpTransferServiceOperationCancelOperator,
    GcpTransferServiceOperationResumeOperator,
    GcpTransferServiceOperationsListOperator,
    TransferJobValidator,
    TransferJobPreprocessor,
    GcpTransferServiceJobCreateOperator,
    GcpTransferServiceJobUpdateOperator,
    GcpTransferServiceOperationGetOperator,
    GcpTransferServiceOperationPauseOperator,
    S3ToGoogleCloudStorageTransferOperator,
    GoogleCloudStorageToGoogleCloudStorageTransferOperator,
    GcpTransferServiceJobDeleteOperator,
)
from airflow.models import TaskInstance, DAG
from airflow.utils import timezone
from tests.compat import mock

try:
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None

GCP_PROJECT_ID = 'project-id'
TASK_ID = 'task-id'

JOB_NAME = "job-name"
OPERATION_NAME = "operation-name"
AWS_BUCKET_NAME = "aws-bucket-name"
GCS_BUCKET_NAME = "gcp-bucket-name"
DESCRIPTION = "description"

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

TEST_FILTER = {FILTER_JOB_NAMES: [JOB_NAME]}

TEST_AWS_ACCESS_KEY_ID = "test-key-1"
TEST_AWS_ACCESS_SECRET = "test-secret-1"
TEST_AWS_ACCESS_KEY = {ACCESS_KEY_ID: TEST_AWS_ACCESS_KEY_ID, SECRET_ACCESS_KEY: TEST_AWS_ACCESS_SECRET}

NATIVE_DATE = date(2018, 10, 15)
DICT_DATE = {'day': 15, 'month': 10, 'year': 2018}
NATIVE_TIME = time(hour=11, minute=42, second=43)
DICT_TIME = {'hours': 11, 'minutes': 42, 'seconds': 43}
SCHEDULE_NATIVE = {
    SCHEDULE_START_DATE: NATIVE_DATE,
    SCHEDULE_END_DATE: NATIVE_DATE,
    START_TIME_OF_DAY: NATIVE_TIME,
}

SCHEDULE_DICT = {
    SCHEDULE_START_DATE: {'day': 15, 'month': 10, 'year': 2018},
    SCHEDULE_END_DATE: {'day': 15, 'month': 10, 'year': 2018},
    START_TIME_OF_DAY: {'hours': 11, 'minutes': 42, 'seconds': 43},
}

SOURCE_AWS = {AWS_S3_DATA_SOURCE: {BUCKET_NAME: AWS_BUCKET_NAME}}
SOURCE_GCS = {GCS_DATA_SOURCE: {BUCKET_NAME: GCS_BUCKET_NAME}}
SOURCE_HTTP = {HTTP_DATA_SOURCE: {LIST_URL: "http://example.com"}}

VALID_TRANSFER_JOB_BASE = {
    NAME: JOB_NAME,
    DESCRIPTION: DESCRIPTION,
    STATUS: 'ENABLED',
    SCHEDULE: SCHEDULE_DICT,
    TRANSFER_SPEC: {GCS_DATA_SINK: {BUCKET_NAME: GCS_BUCKET_NAME}},
}  # type: Dict
VALID_TRANSFER_JOB_GCS = deepcopy(VALID_TRANSFER_JOB_BASE)
VALID_TRANSFER_JOB_GCS[TRANSFER_SPEC].update(deepcopy(SOURCE_GCS))
VALID_TRANSFER_JOB_AWS = deepcopy(VALID_TRANSFER_JOB_BASE)
VALID_TRANSFER_JOB_AWS[TRANSFER_SPEC].update(deepcopy(SOURCE_AWS))

VALID_TRANSFER_JOB_GCS = {
    NAME: JOB_NAME,
    DESCRIPTION: DESCRIPTION,
    STATUS: 'ENABLED',
    SCHEDULE: SCHEDULE_NATIVE,
    TRANSFER_SPEC: {
        GCS_DATA_SOURCE: {BUCKET_NAME: GCS_BUCKET_NAME},
        GCS_DATA_SINK: {BUCKET_NAME: GCS_BUCKET_NAME},
    },
}

VALID_TRANSFER_JOB_RAW = {
    DESCRIPTION: DESCRIPTION,
    STATUS: 'ENABLED',
    SCHEDULE: SCHEDULE_DICT,
    TRANSFER_SPEC: {GCS_DATA_SINK: {BUCKET_NAME: GCS_BUCKET_NAME}},
}  # type: Dict

VALID_TRANSFER_JOB_GCS_RAW = deepcopy(VALID_TRANSFER_JOB_RAW)
VALID_TRANSFER_JOB_GCS_RAW[TRANSFER_SPEC].update(SOURCE_GCS)
VALID_TRANSFER_JOB_AWS_RAW = deepcopy(VALID_TRANSFER_JOB_RAW)
VALID_TRANSFER_JOB_AWS_RAW[TRANSFER_SPEC].update(deepcopy(SOURCE_AWS))
VALID_TRANSFER_JOB_AWS_RAW[TRANSFER_SPEC][AWS_S3_DATA_SOURCE][AWS_ACCESS_KEY] = TEST_AWS_ACCESS_KEY


VALID_OPERATION = {NAME: "operation-name"}


class TransferJobPreprocessorTest(unittest.TestCase):
    def test_should_do_nothing_on_empty(self):
        body = {}
        TransferJobPreprocessor(body=body).process_body()
        self.assertEqual(body, {})

    @unittest.skipIf(boto3 is None, "Skipping test because boto3 is not available")
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.AwsHook')
    def test_should_inject_aws_credentials(self, mock_hook):
        mock_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )

        body = {TRANSFER_SPEC: deepcopy(SOURCE_AWS)}
        body = TransferJobPreprocessor(body=body).process_body()
        self.assertEqual(body[TRANSFER_SPEC][AWS_S3_DATA_SOURCE][AWS_ACCESS_KEY], TEST_AWS_ACCESS_KEY)

    @parameterized.expand([(SCHEDULE_START_DATE,), (SCHEDULE_END_DATE,)])
    def test_should_format_date_from_python_to_dict(self, field_attr):
        body = {SCHEDULE: {field_attr: NATIVE_DATE}}
        TransferJobPreprocessor(body=body).process_body()
        self.assertEqual(body[SCHEDULE][field_attr], DICT_DATE)

    def test_should_format_time_from_python_to_dict(self):
        body = {SCHEDULE: {START_TIME_OF_DAY: NATIVE_TIME}}
        TransferJobPreprocessor(body=body).process_body()
        self.assertEqual(body[SCHEDULE][START_TIME_OF_DAY], DICT_TIME)

    @parameterized.expand([(SCHEDULE_START_DATE,), (SCHEDULE_END_DATE,)])
    def test_should_not_change_date_for_dict(self, field_attr):
        body = {SCHEDULE: {field_attr: DICT_DATE}}
        TransferJobPreprocessor(body=body).process_body()
        self.assertEqual(body[SCHEDULE][field_attr], DICT_DATE)

    def test_should_not_change_time_for_dict(self):
        body = {SCHEDULE: {START_TIME_OF_DAY: DICT_TIME}}
        TransferJobPreprocessor(body=body).process_body()
        self.assertEqual(body[SCHEDULE][START_TIME_OF_DAY], DICT_TIME)


class TransferJobValidatorTest(unittest.TestCase):
    def test_should_raise_exception_when_encounters_aws_credentials(self):
        body = {"transferSpec": {"awsS3DataSource": {"awsAccessKey": TEST_AWS_ACCESS_KEY}}}
        with self.assertRaises(AirflowException) as cm:
            TransferJobValidator(body=body).validate_body()
        err = cm.exception
        self.assertIn(
            "AWS credentials detected inside the body parameter (awsAccessKey). This is not allowed, please "
            "use Airflow connections to store credentials.",
            str(err),
        )

    def test_should_raise_exception_when_body_empty(self):
        body = None
        with self.assertRaises(AirflowException) as cm:
            TransferJobValidator(body=body).validate_body()
        err = cm.exception
        self.assertIn("The required parameter 'body' is empty or None", str(err))

    @parameterized.expand(
        [
            (dict(itertools.chain(SOURCE_AWS.items(), SOURCE_GCS.items(), SOURCE_HTTP.items())),),
            (dict(itertools.chain(SOURCE_AWS.items(), SOURCE_GCS.items())),),
            (dict(itertools.chain(SOURCE_AWS.items(), SOURCE_HTTP.items())),),
            (dict(itertools.chain(SOURCE_GCS.items(), SOURCE_HTTP.items())),),
        ]
    )
    def test_verify_data_source(self, transferSpec):
        body = {TRANSFER_SPEC: transferSpec}

        with self.assertRaises(AirflowException) as cm:
            TransferJobValidator(body=body).validate_body()
        err = cm.exception
        self.assertIn(
            "More than one data source detected. Please choose exactly one data source from: "
            "gcsDataSource, awsS3DataSource and httpDataSource.",
            str(err),
        )

    @parameterized.expand([(VALID_TRANSFER_JOB_GCS,), (VALID_TRANSFER_JOB_AWS,)])
    def test_verify_success(self, body):

        TransferJobValidator(body=body).validate_body()

        self.assertTrue(True)


class GcpStorageTransferJobCreateOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_job_create_gcs(self, mock_hook):
        mock_hook.return_value.create_transfer_job.return_value = VALID_TRANSFER_JOB_GCS_RAW
        body = deepcopy(VALID_TRANSFER_JOB_GCS)
        del (body['name'])
        op = GcpTransferServiceJobCreateOperator(body=body, task_id=TASK_ID)
        result = op.execute(None)

        mock_hook.assert_called_once_with(api_version='v1', gcp_conn_id='google_cloud_default')

        mock_hook.return_value.create_transfer_job.assert_called_once_with(body=VALID_TRANSFER_JOB_GCS_RAW)

        self.assertEqual(result, VALID_TRANSFER_JOB_GCS_RAW)

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.AwsHook')
    def test_job_create_aws(self, aws_hook, mock_hook):
        mock_hook.return_value.create_transfer_job.return_value = VALID_TRANSFER_JOB_AWS_RAW
        aws_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )
        body = deepcopy(VALID_TRANSFER_JOB_AWS)
        del (body['name'])
        op = GcpTransferServiceJobCreateOperator(body=body, task_id=TASK_ID)

        result = op.execute(None)

        mock_hook.assert_called_once_with(api_version='v1', gcp_conn_id='google_cloud_default')

        mock_hook.return_value.create_transfer_job.assert_called_once_with(body=VALID_TRANSFER_JOB_AWS_RAW)

        self.assertEqual(result, VALID_TRANSFER_JOB_AWS_RAW)

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.AwsHook')
    def test_job_create_multiple(self, aws_hook, gcp_hook):
        aws_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )
        gcp_hook.return_value.create_transfer_job.return_value = VALID_TRANSFER_JOB_AWS_RAW
        body = deepcopy(VALID_TRANSFER_JOB_AWS)

        op = GcpTransferServiceJobCreateOperator(body=body, task_id=TASK_ID)
        result = op.execute(None)
        self.assertEqual(result, VALID_TRANSFER_JOB_AWS_RAW)

        op = GcpTransferServiceJobCreateOperator(body=body, task_id=TASK_ID)
        result = op.execute(None)
        self.assertEqual(result, VALID_TRANSFER_JOB_AWS_RAW)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        self.dag = DAG(dag_id, default_args={'start_date': DEFAULT_DATE})
        op = GcpTransferServiceJobCreateOperator(
            body={"description": "{{ dag.dag_id }}"},
            gcp_conn_id='{{ dag.dag_id }}',
            aws_conn_id='{{ dag.dag_id }}',
            task_id='task-id',
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'body')[DESCRIPTION])
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'aws_conn_id'))


class GcpStorageTransferJobUpdateOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_job_update(self, mock_hook):
        mock_hook.return_value.update_transfer_job.return_value = VALID_TRANSFER_JOB_GCS
        body = {'transferJob': {'description': 'example-name'}, 'updateTransferJobFieldMask': DESCRIPTION}

        op = GcpTransferServiceJobUpdateOperator(job_name=JOB_NAME, body=body, task_id=TASK_ID)
        result = op.execute(None)

        mock_hook.assert_called_once_with(api_version='v1', gcp_conn_id='google_cloud_default')
        mock_hook.return_value.update_transfer_job.assert_called_once_with(job_name=JOB_NAME, body=body)
        self.assertEqual(result, VALID_TRANSFER_JOB_GCS)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceJobUpdateOperator(
            job_name='{{ dag.dag_id }}',
            body={'transferJob': {"name": "{{ dag.dag_id }}"}},
            task_id='task-id',
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'body')['transferJob']['name'])
        self.assertEqual(dag_id, getattr(op, 'job_name'))


class GcpStorageTransferJobDeleteOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_job_delete(self, mock_hook):
        op = GcpTransferServiceJobDeleteOperator(
            job_name=JOB_NAME, project_id=GCP_PROJECT_ID, task_id='task-id'
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1', gcp_conn_id='google_cloud_default')
        mock_hook.return_value.delete_transfer_job.assert_called_once_with(
            job_name=JOB_NAME, project_id=GCP_PROJECT_ID
        )

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_job_delete_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceJobDeleteOperator(
            job_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'job_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_job_delete_should_throw_ex_when_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceJobDeleteOperator(job_name="", task_id='task-id')
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'job_name' is empty or None", str(err))
        mock_hook.assert_not_called()


class GpcStorageTransferOperationsGetOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_get(self, mock_hook):
        mock_hook.return_value.get_transfer_operation.return_value = VALID_OPERATION
        op = GcpTransferServiceOperationGetOperator(operation_name=OPERATION_NAME, task_id=TASK_ID)
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1', gcp_conn_id='google_cloud_default')
        mock_hook.return_value.get_transfer_operation.assert_called_once_with(operation_name=OPERATION_NAME)
        self.assertEqual(result, VALID_OPERATION)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_get_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationGetOperator(
            operation_name='{{ dag.dag_id }}', task_id='task-id', dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_get_should_throw_ex_when_operation_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationGetOperator(operation_name="", task_id=TASK_ID)
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()


class GcpStorageTransferOperationListOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_list(self, mock_hook):
        mock_hook.return_value.list_transfer_operations.return_value = [VALID_TRANSFER_JOB_GCS]
        op = GcpTransferServiceOperationsListOperator(request_filter=TEST_FILTER, task_id=TASK_ID)
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1', gcp_conn_id='google_cloud_default')
        mock_hook.return_value.list_transfer_operations.assert_called_once_with(filter=TEST_FILTER)
        self.assertEqual(result, [VALID_TRANSFER_JOB_GCS])

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationsListOperator(
            request_filter={"job_names": ['{{ dag.dag_id }}']},
            gcp_conn_id='{{ dag.dag_id }}',
            task_id='task-id',
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'filter')['job_names'][0])
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))


class GcpStorageTransferOperationsPauseOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause(self, mock_hook):
        op = GcpTransferServiceOperationPauseOperator(operation_name=OPERATION_NAME, task_id='task-id')
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1', gcp_conn_id='google_cloud_default')
        mock_hook.return_value.pause_transfer_operation.assert_called_once_with(operation_name=OPERATION_NAME)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationPauseOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause_should_throw_ex_when_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationPauseOperator(operation_name="", task_id='task-id')
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()


class GcpStorageTransferOperationsResumeOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume(self, mock_hook):
        op = GcpTransferServiceOperationResumeOperator(operation_name=OPERATION_NAME, task_id=TASK_ID)
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1', gcp_conn_id='google_cloud_default')
        mock_hook.return_value.resume_transfer_operation.assert_called_once_with(
            operation_name=OPERATION_NAME
        )
        self.assertIsNone(result)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationResumeOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume_should_throw_ex_when_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationResumeOperator(operation_name="", task_id=TASK_ID)
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()


class GcpStorageTransferOperationsCancelOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel(self, mock_hook):
        op = GcpTransferServiceOperationCancelOperator(operation_name=OPERATION_NAME, task_id=TASK_ID)
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1', gcp_conn_id='google_cloud_default')
        mock_hook.return_value.cancel_transfer_operation.assert_called_once_with(
            operation_name=OPERATION_NAME
        )
        self.assertIsNone(result)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationCancelOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel_should_throw_ex_when_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationCancelOperator(operation_name="", task_id=TASK_ID)
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()


class S3ToGoogleCloudStorageTransferOperatorTest(unittest.TestCase):
    def test_constructor(self):
        operator = S3ToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            s3_bucket=AWS_BUCKET_NAME,
            gcs_bucket=GCS_BUCKET_NAME,
            project_id=GCP_PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        self.assertEqual(operator.task_id, TASK_ID)
        self.assertEqual(operator.s3_bucket, AWS_BUCKET_NAME)
        self.assertEqual(operator.gcs_bucket, GCS_BUCKET_NAME)
        self.assertEqual(operator.project_id, GCP_PROJECT_ID)
        self.assertEqual(operator.description, DESCRIPTION)
        self.assertEqual(operator.schedule, SCHEDULE_DICT)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)
        op = S3ToGoogleCloudStorageTransferOperator(
            s3_bucket='{{ dag.dag_id }}',
            gcs_bucket='{{ dag.dag_id }}',
            description='{{ dag.dag_id }}',
            object_conditions={'exclude_prefixes': ['{{ dag.dag_id }}']},
            gcp_conn_id='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 's3_bucket'))
        self.assertEqual(dag_id, getattr(op, 'gcs_bucket'))
        self.assertEqual(dag_id, getattr(op, 'description'))
        self.assertEqual(dag_id, getattr(op, 'object_conditions')['exclude_prefixes'][0])
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.AwsHook')
    def test_execute(self, mock_aws_hook, mock_transfer_hook):
        mock_aws_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )

        operator = S3ToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            s3_bucket=AWS_BUCKET_NAME,
            gcs_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_AWS_RAW
        )

        self.assertTrue(mock_transfer_hook.return_value.wait_for_transfer_job.called)

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.AwsHook')
    def test_execute_skip_wait(self, mock_aws_hook, mock_transfer_hook):
        mock_aws_hook.return_value.get_credentials.return_value = Credentials(
            TEST_AWS_ACCESS_KEY_ID, TEST_AWS_ACCESS_SECRET, None
        )

        operator = S3ToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            s3_bucket=AWS_BUCKET_NAME,
            gcs_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
            wait=False,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_AWS_RAW
        )

        self.assertFalse(mock_transfer_hook.return_value.wait_for_transfer_job.called)


class GoogleCloudStorageToGoogleCloudStorageTransferOperatorTest(unittest.TestCase):
    def test_constructor(self):
        operator = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            source_bucket=GCS_BUCKET_NAME,
            destination_bucket=GCS_BUCKET_NAME,
            project_id=GCP_PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        self.assertEqual(operator.task_id, TASK_ID)
        self.assertEqual(operator.source_bucket, GCS_BUCKET_NAME)
        self.assertEqual(operator.destination_bucket, GCS_BUCKET_NAME)
        self.assertEqual(operator.project_id, GCP_PROJECT_ID)
        self.assertEqual(operator.description, DESCRIPTION)
        self.assertEqual(operator.schedule, SCHEDULE_DICT)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {'start_date': DEFAULT_DATE}
        self.dag = DAG(dag_id, default_args=args)
        op = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            source_bucket='{{ dag.dag_id }}',
            destination_bucket='{{ dag.dag_id }}',
            description='{{ dag.dag_id }}',
            object_conditions={'exclude_prefixes': ['{{ dag.dag_id }}']},
            gcp_conn_id='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag,
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'source_bucket'))
        self.assertEqual(dag_id, getattr(op, 'destination_bucket'))
        self.assertEqual(dag_id, getattr(op, 'description'))
        self.assertEqual(dag_id, getattr(op, 'object_conditions')['exclude_prefixes'][0])
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_execute(self, mock_transfer_hook):
        operator = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            source_bucket=GCS_BUCKET_NAME,
            destination_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_GCS_RAW
        )
        self.assertTrue(mock_transfer_hook.return_value.wait_for_transfer_job.called)

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_execute_skip_wait(self, mock_transfer_hook):
        operator = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            source_bucket=GCS_BUCKET_NAME,
            destination_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            wait=False,
            schedule=SCHEDULE_DICT,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_GCS_RAW
        )
        self.assertFalse(mock_transfer_hook.return_value.wait_for_transfer_job.called)
