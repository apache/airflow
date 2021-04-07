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

from airflow.models.auth import JwtToken
from airflow.utils.session import provide_session


class TestToken(unittest.TestCase):
    def tearDown(self) -> None:
        self.delete_tokens()

    @provide_session
    def delete_tokens(self, session=None):
        tokens = session.query(JwtToken).all()
        for i in tokens:
            session.delete(i)

    @provide_session
    def create_token(self, session=None):
        token = JwtToken(jti="token", expiry_delta=62939233)
        session.add(token)
        session.commit()
        return token

    @provide_session
    def test_create_token(self, session=None):
        self.create_token()
        token = session.query(JwtToken).all()
        assert len(token) == 1

    def test_get_token_method(self):
        token = self.create_token()
        token2 = JwtToken.get_token(token.jti)
        assert token2.jti == token.jti
        assert token2.expiry_delta == token.expiry_delta

    def test_delete_token_method(self):
        tkn = self.create_token()
        token = JwtToken.get_token(tkn.jti)
        assert token is not None
        JwtToken.delete_token(token.jti)
        token = JwtToken.get_token(token.jti)
        assert token is None
