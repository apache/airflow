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

import contextlib
import os
from collections.abc import Generator
from functools import wraps
from inspect import signature
from typing import Callable, TypeVar, cast

from sqlalchemy.orm import Session as SASession

from airflow import settings
from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.settings import TracebackSession, TracebackSessionForTests
from airflow.typing_compat import ParamSpec


@contextlib.contextmanager
def create_session(scoped: bool = True) -> Generator[SASession, None, None]:
    """Contextmanager that will create and teardown a session."""
    if InternalApiConfig.get_use_internal_api():
        if os.environ.get("RUN_TESTS_WITH_DATABASE_ISOLATION", "false").lower() == "true":
            traceback_session_for_tests = TracebackSessionForTests()
            try:
                yield traceback_session_for_tests
                if traceback_session_for_tests.current_db_session:
                    traceback_session_for_tests.current_db_session.commit()
            except Exception:
                traceback_session_for_tests.current_db_session.rollback()
                raise
            finally:
                traceback_session_for_tests.current_db_session.close()
        else:
            yield TracebackSession()
        return
    if scoped:
        Session = getattr(settings, "Session", None)
    else:
        Session = getattr(settings, "NonScopedSession", None)
    if Session is None:
        raise RuntimeError("Session must be set before!")
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextlib.asynccontextmanager
async def create_session_async():
    """
    Context manager to create async session.

    :meta private:
    """
    from airflow.settings import AsyncSession

    async with AsyncSession() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


PS = ParamSpec("PS")
RT = TypeVar("RT")


def find_session_idx(func: Callable[PS, RT]) -> int:
    """Find session index in function call parameter."""
    func_params = signature(func).parameters
    try:
        # func_params is an ordered dict -- this is the "recommended" way of getting the position
        session_args_idx = tuple(func_params).index("session")
    except ValueError:
        raise ValueError(f"Function {func.__qualname__} has no `session` argument") from None

    return session_args_idx


def provide_session(func: Callable[PS, RT]) -> Callable[PS, RT]:
    """
    Provide a session if it isn't provided.

    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """
    session_args_idx = find_session_idx(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> RT:
        if "session" in kwargs or session_args_idx < len(args):
            return func(*args, **kwargs)
        else:
            with create_session() as session:
                return func(*args, session=session, **kwargs)

    return wrapper


# A fake session to use in functions decorated by provide_session. This allows
# the 'session' argument to be of type Session instead of Session | None,
# making it easier to type hint the function body without dealing with the None
# case that can never happen at runtime.
NEW_SESSION: SASession = cast(SASession, None)
