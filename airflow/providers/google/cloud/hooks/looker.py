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
#
"""This module contains a Google Cloud Looker hook."""

import json
import time
from enum import Enum
from typing import Optional, Dict

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.version import version
# from looker_sdk.sdk.api40.models import MaterializePDT

from looker_sdk.rtl import api_settings, auth_session, requests_transport, serialize
from looker_sdk.sdk.api40 import methods as methods40
from airflow.models.connection import Connection

# Temporary imports for API response stub
from looker_sdk.rtl.model import Model
import attr


@attr.s(auto_attribs=True, init=False)
class MaterializePDT(Model):
    """
    TODO: Temporary API response stub. Use Looker API class once it's released.
    Attributes:
        materialization_id: The ID of the enqueued materialization job, if any
        resp_text: The string response
    """
    materialization_id: Optional[str] = None
    resp_text: Optional[str] = None

    def __init__(self, *,
        materialization_id: Optional[str] = None,
        resp_text: Optional[str] = None):
        self.materialization_id = materialization_id
        self.resp_text = resp_text


class LookerApiSettings(api_settings.ApiSettings):
    """Custom implementation of Looker SDK's `ApiSettings` class."""

    def __init__(
        self,
        conn: Connection,
    ) -> None:
        self.conn = conn  # need to init before `read_config` is called in super
        super().__init__()

    def read_config(self) -> Dict[str, str]:
        """
        Overrides the default logic of getting connection settings. Fetches
        the connection settings from Airflow's connection object.
        """

        config = {}

        if self.conn.host is None:
            raise AirflowException(
                f'No `host` was supplied in connection: {self.looker_conn_id}.'
            )

        if self.conn.port:
            config["base_url"] = f"{self.conn.host}:{self.conn.port}"  # port is optional
        else:
            config["base_url"] = self.conn.host

        if self.conn.login:
            config["client_id"] = self.conn.login
        else:
            raise AirflowException(
                f'No `login` was supplied in connection: {self.looker_conn_id}.'
            )

        if self.conn.password:
            config["client_secret"] = self.conn.password
        else:
            raise AirflowException(
                f'No `password` was supplied in connection: {self.looker_conn_id}.'
            )

        extras = self.conn.extra_dejson  # type: Dict

        if 'verify_ssl' in extras:
            config["verify_ssl"] = extras["verify_ssl"]  # optional

        if 'timeout' in extras:
            config["timeout"] = extras["timeout"]  # optional

        return config


class LookerHook(BaseHook):
    """
    Hook for Looker APIs.
    """

    def __init__(
        self,
        looker_conn_id: str,
    ) -> None:
        super().__init__()
        self.looker_conn_id = looker_conn_id

        conn = self.get_connection(self.looker_conn_id)
        settings = LookerApiSettings(conn)

        transport = requests_transport.RequestsTransport.configure(settings)
        self.sdk = methods40.Looker40SDK(
            auth_session.AuthSession(settings, transport, serialize.deserialize40, "4.0"),
            serialize.deserialize40,
            serialize.serialize,
            transport,
            "4.0",
        )

        self.log.debug("Got the following version for connection id: '%s'. Version: '%s'.", self.looker_conn_id,
                       version)
        # Airflow version example: "2.1.4"
        # Composer version example: "2.1.4+composer"
        self.source = 'composer' if 'composer' in version else 'airflow'

    # resp
    # MaterializePDT(materialization_id='366', resp_text=None)
    def start_pdt_build(
        self,
        model: str,
        view: str,
        query_params: Optional[Dict] = None,
    ) -> MaterializePDT:
        """
        Submits a PDT materialization job to Looker.

        :param model: Required. The model of the PDT to start building.
        :type model: str
        :param view: Required. The view of the PDT to start building.
        :type view: str
        :param query_params: Optional. Additional materialization parameters.
        :type query_params: Dict
        """
        self.log.info("Submitting PDT materialization job. Model: '%s', view: '%s'.", model, view)
        self.log.debug("PDT materialization job source: '%s'.", self.source)

        # resp = self.sdk.start_pdt_build(model, view, **query_params)  # unpack query_params dict into kwargs
        # resp = self.sdk.start_pdt_build(model, view, source=self.source, **query_params) # unpack query_params dict into kwargs (if not None)
        # TODO: Temporary API response stub. Use Looker API once it's released.
        resp = MaterializePDT(materialization_id='366', resp_text=None)

        self.log.info("Start PDT build response: '%s'.", resp)

        return resp

    # resp
    # MaterializePDT(materialization_id='366', resp_text='{"status":"running","runtime":null,"message":"Materialize 100m_sum_aggregate_sdt, ","task_slug":"f522424e00f0039a8c8f2d53d773f310"}')
    def check_pdt_build(
        self,
        materialization_id: str,
    ) -> MaterializePDT:
        """
        Gets the PDT materialization job status from Looker.

        :param materialization_id: Required. The materialization id to check status for.
        :type materialization_id: str
        """
        self.log.info("Requesting PDT materialization job status. Job id: %s.", materialization_id)

        # resp = self.sdk.check_pdt_build(materialization_id)
        # TODO: Temporary API response stub. Use Looker API once it's released.
        resp = MaterializePDT(materialization_id='366', resp_text='{"status":"running","runtime":null,"message":"Materialize 100m_sum_aggregate_sdt, ","task_slug":"f522424e00f0039a8c8f2d53d773f310"}')

        self.log.info("Check PDT build response: '%s'.", resp)
        return resp

    def pdt_build_status(
        self,
        materialization_id: str,
    ) -> str:
        """
        Gets the PDT materialization job status as a string.

        :param materialization_id: Required. The materialization id to check status for.
        :type materialization_id: str
        """
        resp = self.check_pdt_build(materialization_id)

        resp_text_json = resp['resp_text']
        resp_dict = json.loads(resp_text_json)

        status = resp_dict['status']
        self.log.info("PDT materialization job id: %s. Status: '%s'.", materialization_id, status)

        return status

    # resp:
    # MaterializePDT(materialization_id='366', resp_text='{"success":true,"connection":"incremental_pdts_test","statusment":"KILL 810852","task_slug":"6bad8184f94407134251be8fd18af834"}')
    def stop_pdt_build(
        self,
        materialization_id: str,
    ) -> MaterializePDT:
        """
        Starts a PDT materialization job cancellation request.

        :param materialization_id: Required. The materialization id to stop.
        :type materialization_id: str
        """
        self.log.info("Stopping PDT materialization. Job id: %s.", materialization_id)
        self.log.debug("PDT materialization job source: '%s'.", self.source)

        # resp = self.sdk.stop_pdt_build(materialization_id)
        # resp = self.sdk.stop_pdt_build(materialization_id, source=self.source)
        # TODO: Temporary API response stub. Use Looker API once it's released.
        resp = MaterializePDT(materialization_id='366', resp_text='{"success":true,"connection":"incremental_pdts_test","statusment":"KILL 810852","task_slug":"6bad8184f94407134251be8fd18af834"}')

        self.log.info("Stop PDT build response: '%s'.", resp)
        return resp

    def wait_for_job(
        self,
        materialization_id: str,
        wait_time: int = 10,
        timeout: Optional[int] = None,
    ) -> None:
        """
        Helper method which polls a PDT materialization job to check if it finishes.

        :param materialization_id: Required. The materialization id to wait for.
        :type materialization_id: str
        :param wait_time: Optional. Number of seconds between checks.
        :type wait_time: int
        :param timeout: Optional. How many seconds wait for job to be ready. Used only if ``asynchronous`` is False.
        :type timeout: int
        """
        self.log.info('Waiting for PDT materialization job to complete. Job id: %s.', materialization_id)

        status = None
        start = time.monotonic()

        while status not in (JobStatus.DONE.value, JobStatus.ERROR.value, JobStatus.CANCELLED.value):

            if timeout and start + timeout < time.monotonic():
                raise AirflowException(
                    f"Timeout: PDT materialization job is not ready after {timeout}s. Job id: {materialization_id}.")

            time.sleep(wait_time)

            status = self.pdt_build_status(materialization_id=materialization_id)

        if status == JobStatus.ERROR.value:
            raise AirflowException(f'PDT materialization job failed. Job id: {materialization_id}.')
        if status == JobStatus.CANCELLED.value:
            raise AirflowException(f'PDT materialization job was cancelled. Job id: {materialization_id}.')

        self.log.info('PDT materialization job completed successfully. Job id: %s.', materialization_id)


class JobStatus(Enum):
    """
    The job status string.
    """
    PENDING = 'new'
    RUNNING = 'running'
    CANCELLED = 'killed'
    DONE = 'complete'
    ERROR = 'error'
