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
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.providers.google.leveldb.hooks.leveldb import LevelDBHook
from airflow.utils.decorators import apply_defaults


class LevelDBOperator(BaseOperator):
    """
    Execute command in LevelDB

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LevelDBOperator`
    """

    @apply_defaults
    def __init__(
        self,
        *,
        command: str,
        key: bytes,
        value: bytes = None,
        keys: List[bytes] = None,
        values: List[bytes] = None,
        leveldb_conn_id: str = 'leveldb_default',
        name: str = '/tmp/testdb/',
        create_if_missing: bool = False,
        create_db_extra_options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.key = key
        self.value = value
        self.keys = keys
        self.values = values
        self.leveldb_conn_id = leveldb_conn_id
        self.name = name
        self.create_if_missing = create_if_missing
        self.create_db_extra_options = create_db_extra_options or {}

    def execute(self, context) -> Optional[bytes]:
        """
        Execute command in LevelDB

        :returns: value from get or None(Optional[bytes])
        :rtype: Optional[bytes]
        """
        leveldb_hook = LevelDBHook(leveldb_conn_id=self.leveldb_conn_id)
        leveldb_hook.get_conn(
            name=self.name, create_if_missing=self.create_if_missing, **self.create_db_extra_options
        )
        value = leveldb_hook.run(
            command=self.command,
            key=self.key,
            value=self.value,
            keys=self.keys,
            values=self.values,
        )
        self.log.info("Done. Returned value was: %s", str(value))
        leveldb_hook.close_conn()
        return value
