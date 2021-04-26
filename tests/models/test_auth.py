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

import unittest
from datetime import datetime

from airflow.models.auth import TokenBlockList
from airflow.utils.session import provide_session

EXPIRY_DELTA = datetime.timestamp(datetime.now())


class TestToken(unittest.TestCase):
    def tearDown(self) -> None:
        self.delete_tokens()

    @provide_session
    def delete_tokens(self, session=None):
        tokens = session.query(TokenBlockList).all()
        for i in tokens:
            session.delete(i)

    @provide_session
    def test_add_token(self, session=None):
        TokenBlockList.add_token(jti="token", expiry_delta=EXPIRY_DELTA)
        token = session.query(TokenBlockList).all()
        assert len(token) == 1

    def test_get_token_method(self):
        TokenBlockList.add_token(jti="token", expiry_delta=EXPIRY_DELTA)
        token2 = TokenBlockList.get_token("token")
        assert token2.jti == "token"

    def test_delete_token_method(self):
        TokenBlockList.add_token(jti="token", expiry_delta=EXPIRY_DELTA)
        token = TokenBlockList.get_token("token")
        assert token is not None
        TokenBlockList.delete_token("token")
        token = TokenBlockList.get_token("token")
        assert token is None

    def test_delete_expired_tokens(self):
        TokenBlockList.add_token(jti="token", expiry_delta=EXPIRY_DELTA)
        TokenBlockList.add_token(jti="token2", expiry_delta=EXPIRY_DELTA)
        assert TokenBlockList.get_token('token')
        assert TokenBlockList.get_token('token2')
        TokenBlockList.delete_expired_tokens()
        assert TokenBlockList.get_token('token') is None
        assert TokenBlockList.get_token('token2') is None
