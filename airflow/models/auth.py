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

from pendulum import from_timestamp
from sqlalchemy import Column, DateTime, Index, String

from airflow.models.base import Base
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session


class TokenBlockList(Base, LoggingMixin):
    """
    A model for recording blocked token

    :param jti: The token jti(JWT ID)
    :type jti: str
    :param expiry_date: When the token would expire
    :type expiry_date: DateTime
    """

    __tablename__ = 'token_blocklist'
    jti = Column(String(50), nullable=False, primary_key=True)
    expiry_date = Column(DateTime(), nullable=False)
    __table_args__ = (Index('idx_expiry_date_token_blocklist', expiry_date),)

    @classmethod
    @provide_session
    def get_token(cls, token, session=None):
        """Get a token"""
        return session.query(cls).get(token)

    @classmethod
    @provide_session
    def delete_token(cls, token, session=None):
        """Delete a token"""
        session.query(cls).filter(cls.jti == token).delete()

    @classmethod
    @provide_session
    def add_token(cls, jti, expiry_delta, session=None):
        """Add a token to blocklist"""
        # pylint: disable=unexpected-keyword-arg
        token = cls(jti=jti, expiry_date=from_timestamp(expiry_delta))
        session.add(token)
        session.commit()

    @classmethod
    @provide_session
    def delete_expired_tokens(cls, session=None):
        """Delete all expired tokens"""
        session.query(cls).filter(cls.expiry_date <= datetime.now()).delete()
