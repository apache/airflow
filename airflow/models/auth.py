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

from sqlalchemy import Boolean, Column, DateTime, Integer, Interval, String, Text, func

from airflow.models.base import Base
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session


class Token(Base, LoggingMixin):
    """Token list"""

    __tablename__ = 'token'
    id = Column(Integer, primary_key=True)
    jti = Column(Text(), nullable=False)
    refresh = Column(Boolean(name="refresh"), default=False)
    is_revoked = Column(Boolean(name='revoked'), default=False)
    revoke_reason = Column(String(100))
    revoked_by = Column(String(50))
    date_revoked = Column(DateTime)
    expiry_delta = Column(Interval, nullable=False)
    created_at = Column(DateTime, default=func.now())

    def __init__(
        self,
        jti,
        expiry_delta,
        refresh=False,
        is_revoked=False,
        revoked_reason=None,
        revoked_by=None,
        date_revoked=None,
        created_at=None,
    ):
        super().__init__()
        self.jti = jti
        self.expiry_delta = expiry_delta
        self.refresh = refresh
        self.is_revoked = is_revoked
        self.revoke_reason = revoked_reason
        self.revoked_by = revoked_by
        self.date_revoked = date_revoked
        self.created_at = created_at

    @classmethod
    @provide_session
    def get_token(cls, token, session=None):
        tkn = session.query(cls).filter(cls.jti == token).first()
        return tkn

    @classmethod
    @provide_session
    def delete_token(cls, token, session=None):
        tkn = session.query(cls).filter(cls.jti == token).first()
        if tkn:
            session.delete(tkn)
            session.commit()
