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

# ~~~~~~~~~~~~~~~~~~~~~~
#
# Server-side Sessions and SessionInterfaces.
#
# Originally copied from flask_session.sessions
# Flask-Session
# Copyright (C) 2014 by Shipeng Feng and other contributors
# BSD, see LICENSE for more details.
from datetime import datetime
from typing import Any, Dict

from sqlalchemy import Column, DateTime, Integer, LargeBinary, String

from airflow.models.base import Base
from airflow.utils.log.logging_mixin import LoggingMixin


# This simply matches the model in `fask_session.sessions.SqlAlchemySessionInterface`
class Session(Base, LoggingMixin):
    """A table to store webserver sessions."""

    __tablename__ = 'sessions'

    id = Column(Integer, primary_key=True)
    session_id = Column(String(255), unique=True)
    data = Column(LargeBinary)
    expiry = Column(DateTime)

    def __init__(self, session_id: str, data: Dict[str, Any], expiry: datetime):
        self.session_id = session_id
        self.data = data
        self.expiry = expiry

    def __repr__(self):
        return f'<Session data {self.data}>'
