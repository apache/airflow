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

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
)
from airflow.models.base import Base
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.sqlalchemy import ExtendedJSON


class Session(Base, LoggingMixin):
    __tablename__ = 'sessions'

    id = Column(Integer, primary_key=True)
    session_id = Column(String(255), unique=True)
    data = Column(ExtendedJSON)
    expiry = Column(DateTime)

    def __init__(self, session_id: str, data: Dict[str, Any], expiry: datetime):
        self.session_id = session_id
        self.data = data
        self.expiry = expiry

    def __repr__(self):
        return '<Session data %s>' % self.data
