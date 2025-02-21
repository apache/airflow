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

"""
AWS Batch Executor configuration.

This is the configuration for calling the Batch ``submit_job`` function. The AWS Batch Executor calls
Boto3's ``submit_job(**kwargs)`` function with the kwargs templated by this dictionary. See the URL
below for documentation on the parameters accepted by the Boto3 submit_job function.

.. seealso::
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/submit_job.html

"""

from __future__ import annotations

import json
from json import JSONDecodeError
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.providers.amazon.aws.executors.batch.utils import (
    CONFIG_GROUP_NAME,
    AllBatchConfigKeys,
    BatchSubmitJobKwargsConfigKeys,
)
from airflow.providers.amazon.aws.executors.ecs.utils import camelize_dict_keys
from airflow.utils.helpers import prune_dict


def _fetch_templated_kwargs() -> dict[str, str]:
    submit_job_kwargs_value = conf.get(
        CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS, fallback=dict()
    )
    return json.loads(str(submit_job_kwargs_value))


def _fetch_config_values() -> dict[str, str]:
    return prune_dict(
        {key: conf.get(CONFIG_GROUP_NAME, key, fallback=None) for key in BatchSubmitJobKwargsConfigKeys()}
    )


def build_submit_kwargs() -> dict:
    job_kwargs = _fetch_config_values()
    job_kwargs.update(_fetch_templated_kwargs())

    if "containerOverrides" not in job_kwargs:
        job_kwargs["containerOverrides"] = {}  # type: ignore
    job_kwargs["containerOverrides"]["command"] = []  # type: ignore

    if "nodeOverrides" in job_kwargs:
        raise KeyError("Multi-node jobs are not currently supported.")
    if "eksPropertiesOverride" in job_kwargs:
        raise KeyError("Eks jobs are not currently supported.")

    if TYPE_CHECKING:
        assert isinstance(job_kwargs, dict)
    # some checks with some helpful errors
    if "containerOverrides" not in job_kwargs or "command" not in job_kwargs["containerOverrides"]:
        raise KeyError(
            'SubmitJob API needs kwargs["containerOverrides"]["command"] field,'
            " and value should be NULL or empty."
        )
    job_kwargs = camelize_dict_keys(job_kwargs)

    try:
        json.loads(json.dumps(job_kwargs))
    except JSONDecodeError:
        raise ValueError("AWS Batch Executor config values must be JSON serializable.")

    return job_kwargs
