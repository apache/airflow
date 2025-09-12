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

from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from typing import Any

from airflow.models.base import ID_LEN, Base
from airflow.utils.log.logging_mixin import LoggingMixin


class ProcessState(Base, LoggingMixin):
    """A generic way to store and retrieve arbitrary process state as a simple key/value store."""

    __tablename__ = "process_state"
    __NO_DEFAULT_SENTINEL = object()

    id = Column(Integer, primary_key=True)
    process_name = Column(String(ID_LEN), unique=False)
    key = Column(String(ID_LEN), unique=False)
    value = Column(Text().with_variant(MEDIUMTEXT, "mysql"))

    def __init__(self, process_name=None, key=None, value=None, description=None):
        super().__init__()
        self.process_name = process_name
        self.key = key
        self.value = value
        self.description = description

    @classmethod
    def get(cls, process_name: str, key: str) -> Any:
        from airflow.sdk.execution_time.context import _get_process_state

        return _get_process_state(process_name, key)

    @classmethod
    def set(cls, process_name: str, key: str, value: Any = None) -> Any:
        from airflow.sdk.execution_time.context import _set_process_state

        return _set_process_state(process_name, key, value)

    @classmethod
    def delete(cls, process_name: str, key: str) -> Any:
        from airflow.sdk.execution_time.context import _delete_process_state

        return _delete_process_state(process_name, key)
