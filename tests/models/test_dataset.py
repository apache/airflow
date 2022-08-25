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

from unittest import mock

import pytest

from airflow.datasets import Dataset
from airflow.models.dataset import DatasetModel
from airflow.operators.empty import EmptyOperator


class TestDataset:
    def test_uri_without_scheme(self, dag_maker):
        dataset = Dataset(uri="example_dataset")
        with dag_maker(dag_id="example_dataset"):
            EmptyOperator(task_id="task1", outlets=[dataset])

    def test_uri_with_scheme(self, dag_maker, session):
        dataset = Dataset(uri="s3://example_dataset")
        with dag_maker(dag_id="example_dataset"):
            EmptyOperator(task_id="task1", outlets=[dataset])

    def test_uri_with_invalid_characters(self, dag_maker, session):
        dataset = Dataset(uri="èxample_datašet")
        with pytest.raises(ValueError, match='URI must be ascii'):
            with dag_maker(dag_id="example_dataset"):
                EmptyOperator(task_id="task1", outlets=[dataset])


class TestDatasetModel:
    @pytest.mark.parametrize(
        "conn_uri, dataset_uri, expected_canonical_uri",
        [
            ("postgres://somehost/", "airflow://testconn/", "postgres://somehost/"),
            ("postgres://somehost:111/base", "airflow://testconn", "postgres://somehost:111/base"),
            ("postgres://somehost:111/base", "airflow+foo://testconn", "foo://somehost:111/base"),
            (
                "postgres://somehost:111/base",
                "airflow://testconn/extra",
                "postgres://somehost:111/base/extra",
            ),
            ("postgres://somehost:111", "airflow://testconn/?foo=bar", "postgres://somehost:111/?foo=bar"),
            (
                "postgres://somehost?biz=baz",
                "airflow://testconn/?foo=bar",
                "postgres://somehost/?biz=baz&foo=bar",
            ),
        ],
    )
    def test_canonical_uri(self, conn_uri, dataset_uri, expected_canonical_uri):
        with mock.patch.dict('os.environ', AIRFLOW_CONN_TESTCONN=conn_uri):
            dataset = DatasetModel(uri=dataset_uri)
            assert dataset.canonical_uri == expected_canonical_uri
