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
from airflow.models import BaseOperator
from airflow.providers.google.leveldb.hooks.leveldb import LevelDBHook
from airflow.utils.decorators import apply_defaults


class LevelDBOperator(BaseOperator):
    """
    Execute command in LevelDB
    """

    @apply_defaults
    def __init__(
        self,
        *,
        command: str,
        key: bytes,
        value: bytes = None,
        leveldb_conn_id: str = 'leveldb_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.key = key
        self.value = value
        self.leveldb_conn_id = leveldb_conn_id

    def execute(self, context) -> None:
        """
        Execute command in LevelDB
        """
        leveldb_hook = LevelDBHook(leveldb_conn_id=self.leveldb_conn_id)
        leveldb_hook.get_conn()
        leveldb_hook.run(command=self.command, key=self.key, value=self.value)
