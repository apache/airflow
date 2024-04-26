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

from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.custom_object_launcher import (
    SparkJobSpec,
    SparkResources,
)


class TestSparkJobSpec:
    @patch("airflow.providers.cncf.kubernetes.operators.custom_object_launcher.SparkJobSpec.update_resources")
    @patch("airflow.providers.cncf.kubernetes.operators.custom_object_launcher.SparkJobSpec.validate")
    def test_spark_job_spec_initialization(self, mock_validate, mock_update_resources):
        entries = {
            "spec": {
                "dynamicAllocation": {
                    "enabled": True,
                    "initialExecutors": 1,
                    "minExecutors": 1,
                    "maxExecutors": 2,
                },
                "driver": {},
                "executor": {},
            }
        }
        SparkJobSpec(**entries)
        mock_validate.assert_called_once()
        mock_update_resources.assert_called_once()

    def test_spark_job_spec_dynamicAllocation_enabled(self):
        entries = {
            "spec": {
                "dynamicAllocation": {
                    "enabled": True,
                    "initialExecutors": 1,
                    "minExecutors": 1,
                    "maxExecutors": 2,
                },
                "driver": {},
                "executor": {},
            }
        }
        spark_job_spec = SparkJobSpec(**entries)

        assert spark_job_spec.spec["dynamicAllocation"]["enabled"]

    def test_spark_job_spec_dynamicAllocation_enabled_with_default_initial_executors(self):
        entries = {
            "spec": {
                "dynamicAllocation": {
                    "enabled": True,
                    "minExecutors": 1,
                    "maxExecutors": 2,
                },
                "driver": {},
                "executor": {},
            }
        }
        spark_job_spec = SparkJobSpec(**entries)

        assert spark_job_spec.spec["dynamicAllocation"]["enabled"]

    def test_spark_job_spec_dynamicAllocation_enabled_with_invalid_config(self):
        entries = {
            "spec": {
                "dynamicAllocation": {
                    "enabled": True,
                    "initialExecutors": 1,
                    "minExecutors": 1,
                    "maxExecutors": 2,
                },
                "driver": {},
                "executor": {},
            }
        }

        cloned_entries = entries.copy()
        cloned_entries["spec"]["dynamicAllocation"]["minExecutors"] = None
        with pytest.raises(
            AirflowException,
            match="Make sure min/max value for dynamic allocation is passed",
        ):
            SparkJobSpec(**cloned_entries)

        cloned_entries = entries.copy()
        cloned_entries["spec"]["dynamicAllocation"]["maxExecutors"] = None
        with pytest.raises(
            AirflowException,
            match="Make sure min/max value for dynamic allocation is passed",
        ):
            SparkJobSpec(**cloned_entries)


class TestSparkResources:
    @patch(
        "airflow.providers.cncf.kubernetes.operators.custom_object_launcher.SparkResources.convert_resources"
    )
    def test_spark_resources_initialization(self, mock_convert_resources):
        driver = {
            "gpu": {"name": "nvidia", "quantity": 1},
            "cpu": {"request": "1", "limit": "2"},
            "memory": {"request": "1Gi", "limit": "2Gi"},
        }
        executor = {
            "gpu": {"name": "nvidia", "quantity": 2},
            "cpu": {"request": "2", "limit": "4"},
            "memory": {"request": "2Gi", "limit": "4Gi"},
        }
        SparkResources(driver=driver, executor=executor)
        mock_convert_resources.assert_called_once()

    def test_spark_resources_conversion(self):
        driver = {
            "gpu": {"name": "nvidia", "quantity": 1},
            "cpu": {"request": "1", "limit": "2"},
            "memory": {"request": "1Gi", "limit": "2Gi"},
        }
        executor = {
            "gpu": {"name": "nvidia", "quantity": 2},
            "cpu": {"request": "2", "limit": "4"},
            "memory": {"request": "2Gi", "limit": "4Gi"},
        }
        spark_resources = SparkResources(driver=driver, executor=executor)

        assert spark_resources.driver["memory"]["limit"] == "1462m"
        assert spark_resources.executor["memory"]["limit"] == "2925m"
        assert spark_resources.driver["cpu"]["request"] == 1
        assert spark_resources.driver["cpu"]["limit"] == "2"
        assert spark_resources.executor["cpu"]["request"] == 2
        assert spark_resources.executor["cpu"]["limit"] == "4"
        assert spark_resources.driver["gpu"]["quantity"] == 1
        assert spark_resources.executor["gpu"]["quantity"] == 2
