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

import time
from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from airflow.providers.apache.zeppelin.hooks.zeppelin_hook import ZeppelinHook
from airflow.settings import WEB_COLORS
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.operator_helpers import context_to_airflow_vars


class ZeppelinOperator(BaseOperator, LoggingMixin):

    template_fields = ('parameters',)
    ui_color = WEB_COLORS['LIGHTORANGE']

    def __init__(
        self,
        conn_id: str,
        note_id: str,
        parameters: Optional[Dict[str, Any]] = {},
        clone_note: Optional[bool] = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.note_id = note_id
        self.exec_note_id = self.note_id
        self.parameters = parameters
        self.clone_note = clone_note
        self.z_hook = ZeppelinHook(conn_id)

    def execute(self, context: Dict[str, Any]) -> None:
        self.log.info("context:" + str(context['dag'].default_args))
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)

        try:
            self.z_hook.login()
            if self.clone_note:
                note_json = self.z_hook.get_note(self.note_id)
                note_name = note_json['name']
                dest_note_path = (
                    '/Airflow_Jobs/'
                    + note_name
                    + "/"
                    + note_name
                    + '_'
                    + airflow_context_vars['AIRFLOW_CTX_EXECUTION_DATE'].replace(':', '-')
                    + '_'
                    + str(int(time.time() * 1000))
                )
                self.exec_note_id = self.z_hook.clone_note(self.note_id, dest_note_path)
                self.log.info("Clone a new note: " + dest_note_path + ", note_id: " + self.exec_note_id)
            self.z_hook.run_note(self.exec_note_id, self.parameters)
        finally:
            if self.clone_note and self.exec_note_id != self.note_id:
                self.z_hook.delete_note(self.exec_note_id)
                self.log.info("Deleted cloned note: " + self.exec_note_id)

    def on_kill(self) -> None:
        self.log.info("Kill task of note_id: " + self.exec_note_id)
        self.z_hook.stop_note(self.exec_note_id)
