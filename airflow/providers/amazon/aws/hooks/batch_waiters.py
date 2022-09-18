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
"""
AWS Batch service waiters

.. seealso::

    - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#waiters
    - https://github.com/boto/botocore/blob/develop/botocore/waiter.py
"""
from __future__ import annotations

import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Dict, Optional

import botocore.client
import botocore.exceptions
import botocore.waiter

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook


class BatchWaitersHook(BatchClientHook):
    """
    A utility to manage waiters for AWS Batch services.

    .. code-block:: python

        import random
        from airflow.providers.amazon.aws.operators.batch_waiters import BatchWaiters

        # to inspect default waiters
        waiters = BatchWaiters()
        config = waiters.default_config  # type: Dict
        waiter_names = waiters.list_waiters()  # -> ["JobComplete", "JobExists", "JobRunning"]

        # The default_config is a useful stepping stone to creating custom waiters, e.g.
        custom_config = waiters.default_config  # this is a deepcopy
        # modify custom_config['waiters'] as necessary and get a new instance:
        waiters = BatchWaiters(waiter_config=custom_config)
        waiters.waiter_config  # check the custom configuration (this is a deepcopy)
        waiters.list_waiters()  # names of custom waiters

        # During the init for BatchWaiters, the waiter_config is used to build a waiter_model;
        # and note that this only occurs during the class init, to avoid any accidental mutations
        # of waiter_config leaking into the waiter_model.
        waiters.waiter_model  # -> botocore.waiter.WaiterModel object

        # The waiter_model is combined with the waiters.client to get a specific waiter
        # and the details of the config on that waiter can be further modified without any
        # accidental impact on the generation of new waiters from the defined waiter_model, e.g.
        waiters.get_waiter("JobExists").config.delay  # -> 5
        waiter = waiters.get_waiter("JobExists")  # -> botocore.waiter.Batch.Waiter.JobExists object
        waiter.config.delay = 10
        waiters.get_waiter("JobExists").config.delay  # -> 5 as defined by waiter_model

        # To use a specific waiter, update the config and call the `wait()` method for jobId, e.g.
        waiter = waiters.get_waiter("JobExists")  # -> botocore.waiter.Batch.Waiter.JobExists object
        waiter.config.delay = random.uniform(1, 10)  # seconds
        waiter.config.max_attempts = 10
        waiter.wait(jobs=[jobId])

    .. seealso::

        - https://www.2ndwatch.com/blog/use-waiters-boto3-write/
        - https://github.com/boto/botocore/blob/develop/botocore/waiter.py
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#waiters
        - https://github.com/boto/botocore/tree/develop/botocore/data/ec2/2016-11-15
        - https://github.com/boto/botocore/issues/1915

    :param waiter_config:  a custom waiter configuration for AWS Batch services

    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
        (http://boto3.readthedocs.io/en/latest/guide/configuration.html).

    :param region_name: region name to use in AWS client.
        Override the AWS region in connection (if provided)
    """

    def __init__(self, *args, waiter_config: dict | None = None, **kwargs) -> None:

        super().__init__(*args, **kwargs)

        self._default_config = None  # type: Optional[Dict]
        self._waiter_config = waiter_config or self.default_config
        self._waiter_model = botocore.waiter.WaiterModel(self._waiter_config)

    @property
    def default_config(self) -> dict:
        """
        An immutable default waiter configuration

        :return: a waiter configuration for AWS Batch services
        :rtype: Dict
        """
        if self._default_config is None:
            config_path = Path(__file__).with_name("batch_waiters.json").resolve()
            with open(config_path) as config_file:
                self._default_config = json.load(config_file)
        return deepcopy(self._default_config)  # avoid accidental mutation

    @property
    def waiter_config(self) -> dict:
        """
        An immutable waiter configuration for this instance; a ``deepcopy`` is returned by this
        property. During the init for BatchWaiters, the waiter_config is used to build a
        waiter_model and this only occurs during the class init, to avoid any accidental
        mutations of waiter_config leaking into the waiter_model.

        :return: a waiter configuration for AWS Batch services
        :rtype: Dict
        """
        return deepcopy(self._waiter_config)  # avoid accidental mutation

    @property
    def waiter_model(self) -> botocore.waiter.WaiterModel:
        """
        A configured waiter model used to generate waiters on AWS Batch services.

        :return: a waiter model for AWS Batch services
        :rtype: botocore.waiter.WaiterModel
        """
        return self._waiter_model

    def get_waiter(self, waiter_name: str) -> botocore.waiter.Waiter:
        """
        Get an AWS Batch service waiter, using the configured ``.waiter_model``.

        The ``.waiter_model`` is combined with the ``.client`` to get a specific waiter and
        the properties of that waiter can be modified without any accidental impact on the
        generation of new waiters from the ``.waiter_model``, e.g.

        .. code-block:: python

            waiters.get_waiter("JobExists").config.delay  # -> 5
            waiter = waiters.get_waiter("JobExists")  # a new waiter object
            waiter.config.delay = 10
            waiters.get_waiter("JobExists").config.delay  # -> 5 as defined by waiter_model

        To use a specific waiter, update the config and call the `wait()` method for jobId, e.g.

        .. code-block:: python

            import random

            waiter = waiters.get_waiter("JobExists")  # a new waiter object
            waiter.config.delay = random.uniform(1, 10)  # seconds
            waiter.config.max_attempts = 10
            waiter.wait(jobs=[jobId])

        :param waiter_name: The name of the waiter. The name should match
            the name (including the casing) of the key name in the waiter
            model file (typically this is CamelCasing); see ``.list_waiters``.

        :return: a waiter object for the named AWS Batch service
        :rtype: botocore.waiter.Waiter
        """
        return botocore.waiter.create_waiter_with_client(waiter_name, self.waiter_model, self.client)

    def list_waiters(self) -> list[str]:
        """
        List the waiters in a waiter configuration for AWS Batch services.

        :return: waiter names for AWS Batch services
        :rtype: List[str]
        """
        return self.waiter_model.waiter_names

    def wait_for_job(self, job_id: str, delay: int | float | None = None) -> None:
        """
        Wait for Batch job to complete.  This assumes that the ``.waiter_model`` is configured
        using some variation of the ``.default_config`` so that it can generate waiters with the
        following names: "JobExists", "JobRunning" and "JobComplete".

        :param job_id: a Batch job ID

        :param delay:  A delay before polling for job status

        :raises: AirflowException

        .. note::
            This method adds a small random jitter to the ``delay`` (+/- 2 sec, >= 1 sec).
            Using a random interval helps to avoid AWS API throttle limits when many
            concurrent tasks request job-descriptions.

            It also modifies the ``max_attempts`` to use the ``sys.maxsize``,
            which allows Airflow to manage the timeout on waiting.
        """
        self.delay(delay)
        try:
            waiter = self.get_waiter("JobExists")
            waiter.config.delay = self.add_jitter(waiter.config.delay, width=2, minima=1)
            waiter.config.max_attempts = sys.maxsize  # timeout is managed by Airflow
            waiter.wait(jobs=[job_id])

            waiter = self.get_waiter("JobRunning")
            waiter.config.delay = self.add_jitter(waiter.config.delay, width=2, minima=1)
            waiter.config.max_attempts = sys.maxsize  # timeout is managed by Airflow
            waiter.wait(jobs=[job_id])

            waiter = self.get_waiter("JobComplete")
            waiter.config.delay = self.add_jitter(waiter.config.delay, width=2, minima=1)
            waiter.config.max_attempts = sys.maxsize  # timeout is managed by Airflow
            waiter.wait(jobs=[job_id])

        except (botocore.exceptions.ClientError, botocore.exceptions.WaiterError) as err:
            raise AirflowException(err)
