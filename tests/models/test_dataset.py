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

import pytest

from airflow.datasets import Dataset
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

    def test_uri_with_airflow_scheme_restricted(self, dag_maker, session):
        dataset = Dataset(uri="airflow://example_dataset")
        with pytest.raises(ValueError, match='Scheme `airflow` is reserved'):
            with dag_maker(dag_id="example_dataset"):
                EmptyOperator(task_id="task1", outlets=[dataset])

    def test_uri_with_invalid_characters(self, dag_maker, session):
        dataset = Dataset(uri="èxample_datašet")
        with pytest.raises(ValueError, match='URI must be ascii'):
            with dag_maker(dag_id="example_dataset"):
                EmptyOperator(task_id="task1", outlets=[dataset])
