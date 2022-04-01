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
import re
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.delta_sharing.hooks.delta_sharing import DeltaSharingHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DeltaSharingSensor(BaseSensorOperator):
    """
    Sensor for checking updates to an existing Delta Sharing table.

    :param share: name of the share in which check will be performed
        This field will be templated.
    :param schema: name of the schema (database) in which check will be performed
        This field will be templated.
    :param table: name of the table to check
        This field will be templated.
    :param delta_sharing_conn_id: Reference to the
        :ref:`Delta Sharing connection <howto/connection:delta_sharing>`.
        By default and in the common case this will be ``delta_sharing_default``. To use
        token based authentication, provide the bearer token in the password field for the
        connection and put the base URL in the ``host`` field.
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
    :param retry_limit: Amount of times retry if the Delta Sharing backend is
        unreachable. Its value must be greater than or equal to 1.
    :param retry_delay: Number of seconds for initial wait between retries (it
            might be a floating point number).
    :param retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    """

    template_fields: Sequence[str] = ('share', 'schema', 'table')

    def __init__(
        self,
        *,
        share: str,
        schema: str,
        table: str,
        delta_sharing_conn_id: str = 'delta_sharing_default',
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 2.0,
        retry_args: Optional[Dict[Any, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.share = share
        self.schema = schema
        self.table = table

        self.hook = DeltaSharingHook(
            delta_sharing_conn_id=delta_sharing_conn_id,
            retry_args=retry_args,
            retry_delay=retry_delay,
            retry_limit=retry_limit,
            timeout_seconds=timeout_seconds,
        )

    @staticmethod
    def get_previous_version(context: 'Context', lookup_key):
        return context['ti'].xcom_pull(key=lookup_key, include_prior_dates=True)

    @staticmethod
    def set_version(context: 'Context', lookup_key, version):
        context['ti'].xcom_push(key=lookup_key, value=version)

    def poke(self, context: 'Context') -> bool:
        table_full_name = f'{self.share}.{self.schema}.{self.table}'
        self.log.info(
            'Poking Delta Sharing server for %s at %s', table_full_name, self.hook.delta_sharing_endpoint
        )
        try:
            version = self.hook.get_table_version(self.share, self.schema, self.table)
            self.log.info("Version for %s is '%s'", table_full_name, version)
            prev_version = -1
            if context is not None:
                lookup_key = re.sub("[^[a-zA-Z0-9]+", "_", self.hook.delta_sharing_endpoint + table_full_name)
                prev_data = self.get_previous_version(context, lookup_key)
                self.log.debug("prev_data: %s, type=%s", str(prev_data), type(prev_data))
                if isinstance(prev_data, int):
                    prev_version = prev_data
                elif prev_data is not None:
                    raise AirflowException(f"Incorrect type for previous XCom Data: {type(prev_data)}")
                if prev_version != version:
                    self.set_version(context, lookup_key, version)

            return prev_version < version
        except AirflowException as exc:
            if str(exc).__contains__("Status Code: 404"):
                return False

            raise exc
