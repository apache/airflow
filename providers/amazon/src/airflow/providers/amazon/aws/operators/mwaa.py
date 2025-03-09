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
"""This module contains AWS MWAA operators."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MwaaTriggerDagRunOperator(AwsBaseOperator[MwaaHook]):
    """
    Trigger a Dag Run for a Dag in an Amazon MWAA environment.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MwaaTriggerDagRunOperator`

    :param env_name: The MWAA environment name (templated)
    :param trigger_dag_id: The ID of the DAG to be triggered (templated)
    :param trigger_run_id: The Run ID. This together with trigger_dag_id are a unique key. (templated)
    :param logical_date: The logical date (previously called execution date). This is the time or interval
        covered by this DAG run, according to the DAG definition. This together with trigger_dag_id are a
        unique key. (templated)
    :param data_interval_start: The beginning of the interval the DAG run covers
    :param data_interval_end: The end of the interval the DAG run covers
    :param conf: Additional configuration parameters. The value of this field can be set only when creating
        the object. (templated)
    :param note: Contains manually entered notes by the user about the DagRun. (templated)
    """

    aws_hook_class = MwaaHook
    template_fields: Sequence[str] = aws_template_fields(
        "env_name",
        "trigger_dag_id",
        "trigger_run_id",
        "logical_date",
        "data_interval_start",
        "data_interval_end",
        "conf",
        "note",
    )
    template_fields_renderers = {"conf": "json"}

    def __init__(
        self,
        *,
        env_name: str,
        trigger_dag_id: str,
        trigger_run_id: str | None = None,
        logical_date: str | None = None,
        data_interval_start: str | None = None,
        data_interval_end: str | None = None,
        conf: dict | None = None,
        note: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.env_name = env_name
        self.trigger_dag_id = trigger_dag_id
        self.trigger_run_id = trigger_run_id
        self.logical_date = logical_date
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        self.conf = conf if conf else {}
        self.note = note

    def execute(self, context: Context) -> dict:
        """
        Trigger a Dag Run for the Dag in the Amazon MWAA environment.

        :param context: the Context object
        :return: dict with information about the Dag run
            For details of the returned dict, see :py:meth:`botocore.client.MWAA.invoke_rest_api`
        """
        return self.hook.invoke_rest_api(
            env_name=self.env_name,
            path=f"/dags/{self.trigger_dag_id}/dagRuns",
            method="POST",
            body={
                "dag_run_id": self.trigger_run_id,
                "logical_date": self.logical_date,
                "data_interval_start": self.data_interval_start,
                "data_interval_end": self.data_interval_end,
                "conf": self.conf,
                "note": self.note,
            },
        )
