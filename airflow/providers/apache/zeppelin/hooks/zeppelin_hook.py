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

from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin

from ..pyzeppelin.config import ClientConfig
from ..pyzeppelin.zeppelin_client import ZeppelinClient


class ZeppelinHook(BaseHook, LoggingMixin):
    def __init__(self, conn_id: str = 'zeppelin_default') -> None:
        super().__init__()
        self.z_conn = self.get_connection(conn_id)
        zeppelin_url = "http://" + self.z_conn.host + ":" + str(self.z_conn.port)
        extra_config = self.z_conn.extra_dejson
        self.knox_sso = extra_config.get('KNOX_SSO', None)
        self.log.info("KNOX_SSO: " + str(self.knox_sso))
        if self.knox_sso:
            zeppelin_url = extra_config.get('REST_URL', None)
            self.client_config = ClientConfig(zeppelin_url, 5, self.knox_sso)
        else:
            self.client_config = ClientConfig(zeppelin_url, query_interval=5)
        self.z_client = ZeppelinClient(self.client_config)

    def login(self) -> None:
        if self.knox_sso:
            self.log.info("Knox is enabled, self.login via knox sso")
            user = self.z_conn.self.login
            password = self.z_conn.password
            self.z_client.login(user, password)

    def run_note(self, note_id: str, parameters: Optional[Dict[str, str]] = {}) -> None:
        note_result = self.z_client.execute_note(note_id, parameters)
        self.log.info("Note result:\n" + note_result.get_content())
        if not note_result.is_success():
            raise AirflowException("Fail to run note")

    def reload_note_list(self) -> None:
        self.z_client.reload_note_list()

    def refresh_note(self, note_id: str) -> None:
        self.z_client.get_note(note_id, True)

    def get_note(self, note_id: str) -> Any:
        return self.z_client.get_note(note_id)

    def clone_note(self, note_id: str, dest_note_path: str) -> str:
        cloned_note_id = self.z_client.clone_note(note_id, dest_note_path)
        self.log.info(
            "clone note: "
            + note_id
            + ", cloned_note_path: "
            + dest_note_path
            + ", cloned_note_id: "
            + cloned_note_id
        )
        return cloned_note_id

    def delete_note(self, note_id: str) -> None:
        self.z_client.delete_note(note_id)

    def stop_note(self, note_id: str) -> None:
        self.z_client.cancel_note(note_id)
