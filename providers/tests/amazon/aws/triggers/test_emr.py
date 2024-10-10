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

import sys

from airflow.providers.amazon.aws.triggers.emr import (
    EmrAddStepsTrigger,
    EmrContainerTrigger,
    EmrCreateJobFlowTrigger,
    EmrServerlessCancelJobsTrigger,
    EmrServerlessCreateApplicationTrigger,
    EmrServerlessDeleteApplicationTrigger,
    EmrServerlessStartApplicationTrigger,
    EmrServerlessStartJobTrigger,
    EmrServerlessStopApplicationTrigger,
    EmrStepSensorTrigger,
    EmrTerminateJobFlowTrigger,
)


class TestEmrAddStepsTrigger:
    def test_serialization(self):
        job_flow_id = "test_job_flow_id"
        step_ids = ["step1", "step2"]
        waiter_delay = 10
        waiter_max_attempts = 5

        trigger = EmrAddStepsTrigger(
            job_flow_id=job_flow_id,
            step_ids=step_ids,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrAddStepsTrigger"
        assert kwargs == {
            "job_flow_id": "test_job_flow_id",
            "step_ids": ["step1", "step2"],
            "waiter_delay": 10,
            "waiter_max_attempts": 5,
            "aws_conn_id": "aws_default",
        }


class TestEmrCreateJobFlowTrigger:
    def test_serialization(self):
        job_flow_id = "test_job_flow_id"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"

        trigger = EmrCreateJobFlowTrigger(
            job_flow_id=job_flow_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrCreateJobFlowTrigger"
        assert kwargs == {
            "job_flow_id": "test_job_flow_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
        }


class TestEmrTerminateJobFlowTrigger:
    def test_serialization(self):
        job_flow_id = "test_job_flow_id"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"

        trigger = EmrTerminateJobFlowTrigger(
            job_flow_id=job_flow_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrTerminateJobFlowTrigger"
        assert kwargs == {
            "job_flow_id": "test_job_flow_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
        }


class TestEmrContainerTrigger:
    def test_serialization(self):
        virtual_cluster_id = "test_virtual_cluster_id"
        job_id = "test_job_id"
        waiter_delay = 30
        waiter_max_attempts = 600
        aws_conn_id = "aws_default"

        trigger = EmrContainerTrigger(
            virtual_cluster_id=virtual_cluster_id,
            job_id=job_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrContainerTrigger"
        assert kwargs == {
            "virtual_cluster_id": "test_virtual_cluster_id",
            "job_id": "test_job_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 600,
            "aws_conn_id": "aws_default",
        }

    def test_serialization_default_max_attempts(self):
        virtual_cluster_id = "test_virtual_cluster_id"
        job_id = "test_job_id"
        waiter_delay = 30
        aws_conn_id = "aws_default"

        trigger = EmrContainerTrigger(
            virtual_cluster_id=virtual_cluster_id,
            job_id=job_id,
            waiter_delay=waiter_delay,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrContainerTrigger"
        assert kwargs == {
            "virtual_cluster_id": "test_virtual_cluster_id",
            "job_id": "test_job_id",
            "waiter_delay": 30,
            "waiter_max_attempts": sys.maxsize,
            "aws_conn_id": "aws_default",
        }


class TestEmrStepSensorTrigger:
    def test_serialization(self):
        job_flow_id = "test_job_flow_id"
        step_id = "test_step_id"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"

        trigger = EmrStepSensorTrigger(
            job_flow_id=job_flow_id,
            step_id=step_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrStepSensorTrigger"
        assert kwargs == {
            "job_flow_id": "test_job_flow_id",
            "step_id": "test_step_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
        }


class TestEmrServerlessCreateApplicationTrigger:
    def test_serialization(self):
        application_id = "test_application_id"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"

        trigger = EmrServerlessCreateApplicationTrigger(
            application_id=application_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrServerlessCreateApplicationTrigger"
        assert kwargs == {
            "application_id": "test_application_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
        }


class TestEmrServerlessStartApplicationTrigger:
    def test_serialization(self):
        application_id = "test_application_id"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"

        trigger = EmrServerlessStartApplicationTrigger(
            application_id=application_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrServerlessStartApplicationTrigger"
        assert kwargs == {
            "application_id": "test_application_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
        }


class TestEmrServerlessStopApplicationTrigger:
    def test_serialization(self):
        application_id = "test_application_id"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"

        trigger = EmrServerlessStopApplicationTrigger(
            application_id=application_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrServerlessStopApplicationTrigger"
        assert kwargs == {
            "application_id": "test_application_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
        }


class TestEmrServerlessStartJobTrigger:
    def test_serialization(self):
        application_id = "test_application_id"
        waiter_delay = 30
        waiter_max_attempts = 60
        job_id = "job_id"
        aws_conn_id = "aws_default"

        trigger = EmrServerlessStartJobTrigger(
            application_id=application_id,
            waiter_delay=waiter_delay,
            job_id=job_id,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrServerlessStartJobTrigger"
        assert kwargs == {
            "application_id": "test_application_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "job_id": "job_id",
            "aws_conn_id": "aws_default",
        }


class TestEmrServerlessDeleteApplicationTrigger:
    def test_serialization(self):
        application_id = "test_application_id"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"

        trigger = EmrServerlessDeleteApplicationTrigger(
            application_id=application_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrServerlessDeleteApplicationTrigger"
        assert kwargs == {
            "application_id": "test_application_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
        }


class TestEmrServerlessCancelJobsTrigger:
    def test_serialization(self):
        application_id = "test_application_id"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"

        trigger = EmrServerlessCancelJobsTrigger(
            application_id=application_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.emr.EmrServerlessCancelJobsTrigger"
        assert kwargs == {
            "application_id": "test_application_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
        }
