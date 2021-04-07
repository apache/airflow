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
from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, Column, DateTime, Integer, String

from airflow.models.base import Base
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session


class JwtToken(Base, LoggingMixin):
    """
    A model for recording decoded token information

    :param jti: The token jti(JWT ID)
    :type jti: str
    :param refresh: Whether the token is a refresh token or not
    :type refresh: bool
    :param is_revoked: Whether the token is revoked
    :type is_revoked: bool
    :param revoked_by: The username of the user who revoked the token
    :type revoked_by: str
    :param date_revoked: The date the token was revoked
    :type date_revoked: datetime
    :param expiry_delta: The timestamp of when the token will expire. Token exp
    :type expiry_delta: int
    :param created_delta: The timestamp of when the token was created. The token's iat
    :type created_delta: int
    """

    __tablename__ = 'jwt_token'
    id = Column(Integer, primary_key=True)
    jti = Column(String(50), nullable=False)
    refresh = Column(Boolean(name="refresh"), default=False)
    is_revoked = Column(Boolean(name='revoked'), default=False)
    revoke_reason = Column(String(100))
    revoked_by = Column(String(50))
    date_revoked = Column(DateTime)
    expiry_delta = Column(Integer, nullable=False)
    created_delta = Column(Integer, nullable=False, default=datetime.timestamp(datetime.now()))

    def __init__(
        self,
        jti: str,
        expiry_delta: int,
        refresh: Optional[bool] = False,
        is_revoked: Optional[bool] = False,
        revoked_reason: Optional[str] = None,
        revoked_by: Optional[str] = None,
        date_revoked: Optional[datetime] = None,
        created_delta: Optional[int] = None,
    ):
        super().__init__()
        self.jti = jti
        self.expiry_delta = expiry_delta
        self.refresh = refresh
        self.is_revoked = is_revoked
        self.revoke_reason = revoked_reason
        self.revoked_by = revoked_by
        self.date_revoked = date_revoked
        self.created_delta = created_delta

    @classmethod
    @provide_session
    def get_token(cls, token, session=None):
        """Get a token"""
        tkn = session.query(cls).filter(cls.jti == token).first()
        return tkn

    @classmethod
    @provide_session
    def delete_token(cls, token, session=None):
        """Delete a token"""
        tkn = session.query(cls).filter(cls.jti == token).first()
        if tkn:
            session.delete(tkn)
            session.commit()
