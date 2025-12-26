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
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.callbacks.base_callback_sink import BaseCallbackSink
from airflow.models.db_callback_request import DbCallbackRequest
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.callbacks.callback_requests import CallbackRequest


class DatabaseCallbackSink(BaseCallbackSink):
    """Sends callbacks to database."""

    @provide_session
    def send(self, callback: CallbackRequest, session: Session = NEW_SESSION) -> None:
        """Send callback for execution."""
        db_callback = DbCallbackRequest(callback=callback, priority_weight=1)
        session.add(db_callback)
