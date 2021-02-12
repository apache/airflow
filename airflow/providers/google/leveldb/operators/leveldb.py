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
from typing import Callable, List, Optional

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
        error_if_exists: bool = False,
        paranoid_checks: bool = None,
        write_buffer_size: bool = None,
        max_open_files: int = None,
        lru_cache_size: int = None,
        block_size: int = None,
        block_restart_interval: int = None,
        max_file_size: bool = None,
        compression: str = 'snappy',
        bloom_filter_bits: int = 0,
        comparator: Callable = None,
        comparator_name: bytes = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.key = key
        self.value = value
        self.keys = keys
        self.values = values
        self.leveldb_conn_id = leveldb_conn_id
        # below params for pylevel DB
        self.name = name
        self.create_if_missing = create_if_missing
        self.error_if_exists = error_if_exists
        self.paranoid_checks = paranoid_checks
        self.write_buffer_size = write_buffer_size
        self.max_open_files = max_open_files
        self.lru_cache_size = lru_cache_size
        self.block_size = block_size
        self.block_restart_interval = block_restart_interval
        self.max_file_size = max_file_size
        self.compression = compression
        self.bloom_filter_bits = bloom_filter_bits
        self.comparator = comparator
        self.comparator_name = comparator_name

    def execute(self, context) -> Optional[bytes]:
        """
        Execute command in LevelDB
        :returns value from get or None(Optional[bytes])
        """
        leveldb_hook = LevelDBHook(leveldb_conn_id=self.leveldb_conn_id)
        leveldb_hook.get_conn(
            name=self.name,
            create_if_missing=self.create_if_missing,
            error_if_exists=self.error_if_exists,
            paranoid_checks=self.paranoid_checks,
            write_buffer_size=self.write_buffer_size,
            max_open_files=self.max_open_files,
            lru_cache_size=self.lru_cache_size,
            block_size=self.block_size,
            block_restart_interval=self.block_restart_interval,
            max_file_size=self.max_file_size,
            compression=self.compression,
            bloom_filter_bits=self.bloom_filter_bits,
            comparator=self.comparator,
            comparator_name=self.comparator_name
        )
        value = leveldb_hook.run(
            command=self.command,
            key=self.key,
            value=self.value,
            keys=self.keys,
            values=self.values
        )
        self.log.info("Done. Returned value was: %s", str(value))
        leveldb_hook.close_conn()
        return value
