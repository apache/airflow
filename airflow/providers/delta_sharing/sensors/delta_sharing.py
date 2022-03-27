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
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Sequence
import re

from airflow.exceptions import AirflowException
from airflow.providers.delta_sharing.hooks.delta_sharing import DeltaSharingHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DeltaSharingSensor(BaseSensorOperator):
    """
    """

    template_fields: Sequence[str] = ('share', 'schema', 'table')

    def __init__(
        self,
        *,
        share: str,
        schema: str,
        table: str,
        delta_sharing_conn_id: str = 'delta_sharing_default',
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.share = share
        self.schema = schema
        self.table = table

        self.hook = DeltaSharingHook(delta_sharing_conn_id = delta_sharing_conn_id)

    def poke(self, context: 'Context') -> bool:
        table_full_name = f'{self.share}.{self.schema}.{self.table}'
        self.log.info('Poking Delta Sharing server for %s at %s', table_full_name, self.hook.delta_sharing_endpoint)
        try:
            version = self.hook.get_table_version(self.share, self.schema, self.table)
            self.log.info("Version for %s is '%s'", table_full_name, version)
            prev_version = ""
            if context is not None:
                lookup_key = re.sub("[^[a-zA-Z0-9]+", "_", self.hook.delta_sharing_endpoint + table_full_name)
                prev_data = context['ti'].xcom_pull(key=lookup_key, include_prior_dates = True)
                self.log.info("prev_data: %s, type=%s", str(prev_data), type(prev_data))
                if isinstance(prev_data, str):
                    prev_version = prev_data
                elif prev_data is not None:
                    raise AirflowException(f"Incorrect type for previous XCom Data: {type(prev_data)}")
                if prev_version != version:
                    context['ti'].xcom_push(key=lookup_key, value=version)

            return prev_version != version
        except AirflowException as exc:
            if str(exc).__contains__("Status Code: 404"):
                return False

            raise exc
