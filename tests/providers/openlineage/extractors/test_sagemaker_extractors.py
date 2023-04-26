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
from __future__ import annotations

import logging
import random
from datetime import datetime
from unittest import mock

import pytz
from pkg_resources import parse_version

from airflow.models import DAG, TaskInstance
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
)
from airflow.providers.openlineage.extractors.sagemaker import (
    SageMakerProcessingExtractor,
    SageMakerTrainingExtractor,
    SageMakerTransformExtractor,
)
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.version import version as AIRFLOW_VERSION

log = logging.getLogger(__name__)


class TestSageMakerProcessingExtractor:
    def setup_class(self):
        log.debug("TestRedshiftDataExtractor.setup(): ")
        self.task = TestSageMakerProcessingExtractor._get_processing_task()
        self.ti = TestSageMakerProcessingExtractor._get_ti(task=self.task)
        self.extractor = SageMakerProcessingExtractor(operator=self.task)

    @mock.patch("airflow.models.TaskInstance.xcom_pull")
    def test_extract_on_complete(self, mock_xcom_pull):
        self.extractor._get_s3_datasets = mock.MagicMock(return_value=([], []))

        self.extractor.extract_on_complete(self.ti)

        mock_xcom_pull.assert_called_once_with(task_ids=self.ti.task_id)
        self.extractor._get_s3_datasets.assert_called_once()

    @mock.patch("airflow.models.TaskInstance.xcom_pull", return_value={})
    @mock.patch("airflow.providers.openlineage.extractors.sagemaker.generate_s3_dataset")
    def test_missing_inputs_output(self, mock_generate_s3_dataset, mock_xcom_pull):
        self.extractor._get_s3_datasets = mock.MagicMock(return_value=([], []))

        self.extractor.extract_on_complete(self.ti)

        self.extractor._get_s3_datasets.assert_not_called()
        mock_generate_s3_dataset.assert_not_called()

    @staticmethod
    def _get_processing_task():
        dag = DAG(dag_id="TestSagemakerProcessingExtractor")
        task = SageMakerProcessingOperator(
            task_id="task_id",
            config={},
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
        )

        return task

    @staticmethod
    def _get_ti(task):
        kwargs = {}
        if parse_version(AIRFLOW_VERSION) > parse_version("2.2.0"):
            kwargs["run_id"] = "test_run_id"  # change in 2.2.0
        task_instance = TaskInstance(
            task=task,
            execution_date=datetime.utcnow().replace(tzinfo=pytz.utc),
            state=State.SUCCESS,
            **kwargs,
        )
        task_instance.job_id = random.randrange(10000)

        return task_instance


class TestSageMakerTransformExtractor:
    def setup_class(self):
        log.debug("TestRedshiftDataExtractor.setup(): ")
        self.task = TestSageMakerTransformExtractor._get_transform_task()
        self.ti = TestSageMakerTransformExtractor._get_ti(task=self.task)
        self.extractor = SageMakerTransformExtractor(operator=self.task)

    @mock.patch("airflow.models.TaskInstance.xcom_pull")
    def test_extract_on_complete(self, mock_xcom_pull):
        self.extractor._get_model_data_urls = mock.MagicMock(return_value=([]))

        self.extractor.extract_on_complete(self.ti)

        mock_xcom_pull.assert_called_once_with(task_ids=self.ti.task_id)
        self.extractor._get_model_data_urls.assert_called_once()

    @mock.patch("airflow.models.TaskInstance.xcom_pull", return_value={})
    @mock.patch("airflow.providers.openlineage.extractors.sagemaker.generate_s3_dataset")
    def test_missing_inputs_output(self, mock_generate_s3_dataset, mock_xcom_pull):
        self.extractor._get_model_data_urls = mock.MagicMock(return_value=([]))

        self.extractor.extract_on_complete(self.ti)

        self.extractor._get_model_data_urls.assert_not_called()
        mock_generate_s3_dataset.assert_not_called()

    @staticmethod
    def _get_transform_task():
        dag = DAG(dag_id="TestSageMakerTransformExtractor")
        task = SageMakerTransformOperator(
            task_id="task_id",
            config={},
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
        )

        return task

    @staticmethod
    def _get_ti(task):
        kwargs = {}
        if parse_version(AIRFLOW_VERSION) > parse_version("2.2.0"):
            kwargs["run_id"] = "test_run_id"  # change in 2.2.0
        task_instance = TaskInstance(
            task=task,
            execution_date=datetime.utcnow().replace(tzinfo=pytz.utc),
            state=State.SUCCESS,
            **kwargs,
        )
        task_instance.job_id = random.randrange(10000)

        return task_instance


class TestSageMakerTrainingExtractor:
    def setup_class(self):
        log.debug("TestSageMakerTrainingExtractor.setup(): ")
        self.task = TestSageMakerTrainingExtractor._get_training_task()
        self.ti = TestSageMakerTrainingExtractor._get_ti(task=self.task)
        self.extractor = SageMakerTrainingExtractor(operator=self.task)

    @mock.patch("airflow.models.TaskInstance.xcom_pull")
    @mock.patch("airflow.providers.openlineage.extractors.sagemaker.generate_s3_dataset")
    def test_extract_on_complete(self, mock_generate_s3_dataset, mock_xcom_pull):
        self.extractor.extract_on_complete(self.ti)

        mock_xcom_pull.assert_called_once_with(task_ids=self.ti.task_id)
        mock_generate_s3_dataset.assert_called()

    @mock.patch("airflow.models.TaskInstance.xcom_pull", return_value={})
    @mock.patch("airflow.providers.openlineage.extractors.sagemaker.generate_s3_dataset")
    def test_generate_s3_dataset_missing_inputs_output(self, mock_generate_s3_dataset, mock_xcom_pull):
        self.extractor.extract_on_complete(self.ti)

        mock_generate_s3_dataset.assert_not_called()

    @staticmethod
    def _get_training_task():
        dag = DAG(dag_id="TestSagemakerTrainingExtractor")
        task = SageMakerTrainingOperator(
            task_id="task_id",
            config={},
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
        )

        return task

    @staticmethod
    def _get_ti(task):
        kwargs = {}
        if parse_version(AIRFLOW_VERSION) > parse_version("2.2.0"):
            kwargs["run_id"] = "test_run_id"  # change in 2.2.0
        task_instance = TaskInstance(
            task=task,
            execution_date=datetime.utcnow().replace(tzinfo=pytz.utc),
            state=State.SUCCESS,
            **kwargs,
        )
        task_instance.job_id = random.randrange(10000)

        return task_instance
