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

import pytest
from sqlalchemy import select

from airflow.models import Log
from airflow.utils.session import provide_session

pytestmark = pytest.mark.db_test


class TestSession:
    def dummy_session(self, session=None):
        return session

    def test_raised_provide_session(self):
        with pytest.raises(ValueError, match="Function .*dummy has no `session` argument"):

            @provide_session
            def dummy():
                pass

    def test_provide_session_without_args_and_kwargs(self):
        assert self.dummy_session() is None

        wrapper = provide_session(self.dummy_session)

        assert wrapper() is not None

    def test_provide_session_with_args(self):
        wrapper = provide_session(self.dummy_session)

        session = object()
        assert wrapper(session) is session

    def test_provide_session_with_kwargs(self):
        wrapper = provide_session(self.dummy_session)

        session = object()
        assert wrapper(session=session) is session

    @pytest.mark.asyncio
    async def test_async_session(self):
        from airflow.settings import AsyncSession

        session = AsyncSession()
        session.add(Log(event="hihi1234"))
        await session.commit()
        my_special_log_event = await session.scalar(select(Log).where(Log.event == "hihi1234").limit(1))
        assert my_special_log_event.event == "hihi1234"
