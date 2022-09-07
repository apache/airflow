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

from importlib import import_module

from sqlalchemy import Column, Integer, String

from airflow.callbacks.callback_requests import CallbackRequest
from airflow.models.base import Base
from airflow.utils import timezone
from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime


class DbCallbackRequest(Base):
    """Used to handle callbacks through database."""

    __tablename__ = "callback_request"

    id = Column(Integer(), nullable=False, primary_key=True)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    priority_weight = Column(Integer(), nullable=False)
    callback_data = Column(ExtendedJSON, nullable=False)
    callback_type = Column(String(20), nullable=False)
    processor_subdir = Column(String(2000), nullable=True)

    def __init__(self, priority_weight: int, callback: CallbackRequest):
        self.created_at = timezone.utcnow()
        self.priority_weight = priority_weight
        self.processor_subdir = callback.processor_subdir
        self.callback_data = callback.to_json()
        self.callback_type = callback.__class__.__name__

    def get_callback_request(self) -> CallbackRequest:
        module = import_module("airflow.callbacks.callback_requests")
        callback_class = getattr(module, self.callback_type)
        # Get the function (from the instance) that we need to call
        from_json = getattr(callback_class, 'from_json')
        return from_json(self.callback_data)
