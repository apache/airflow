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

import datetime
import multiprocessing

import pytest

from airflow.secrets.cache import SecretCache

from tests_common.test_utils.config import conf_vars


def test_cache_disabled_by_default():
    SecretCache.init()
    SecretCache.save_variable("test", "not saved")
    with pytest.raises(SecretCache.NotPresentException):
        SecretCache.get_variable("test")
    assert SecretCache._cache is None


class TestSecretCache:
    @staticmethod
    @conf_vars({("secrets", "use_cache"): "true"})
    def setup_method() -> None:
        SecretCache.init()

    @staticmethod
    def teardown_method(self) -> None:
        SecretCache.reset()

    def test_cache_accessible_from_other_process(self):
        def writer():
            SecretCache.save_variable("key", "secret_val")

        def reader(pipe: multiprocessing.connection.Connection):
            v = SecretCache.get_variable("key")
            pipe.send(v)
            pipe.close()

        SecretCache.init()  # init needs to be explicit before creating threads
        c = multiprocessing.get_context("fork")
        # run first process that's going to set a key in the cache
        p1 = c.Process(target=writer)
        p1.start()
        p1.join()
        # setup pipe to receive what second process read (returns reading and writing ends of the pipe)
        r, w = c.Pipe(duplex=False)
        # start second process that's going to try to get the same key from the cache
        p2 = c.Process(target=reader, args=(w,))
        p2.start()
        w.close()  # close pipe on our end because it's used by the child process
        val = r.recv()
        p2.join()

        assert val is not None
        assert val == "secret_val"

    def test_returns_none_when_not_init(self):
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable("whatever")

    def test_cache_saves_none_as_sentinel(self):
        SecretCache.save_variable("key", None)

        res = SecretCache.get_variable("key")

        assert res is None

    def test_invalidate(self):
        SecretCache.save_variable("key", "some_value")

        assert SecretCache.get_variable("key") == "some_value"

        SecretCache.invalidate_variable("key")

        # cannot get the value for that key anymore because we invalidated it
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable("key")

    def test_invalidate_key_not_present(self):
        SecretCache.invalidate_variable(
            "not present"
        )  # simply shouldn't raise any exception.

    def test_expiration(self):
        SecretCache.save_variable("key", "some_value")

        assert SecretCache.get_variable("key") == "some_value"

        SecretCache._ttl = datetime.timedelta(0)  # I don't want to sleep()

        # value is now seen as expired
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable("key")

    @conf_vars({("secrets", "use_cache"): "0"})
    def test_disabled(self):
        # do init to have it read config
        SecretCache.reset()
        SecretCache.init()

        SecretCache.save_variable("key", "some_value")  # will be ignored

        # cache is disabled, gets will always "fail"
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable("key")

    def test_independence_variable_connection(self):
        SecretCache.save_variable("same_key", "some_value")
        SecretCache.save_connection_uri("same_key", "some_other_value")

        assert SecretCache.get_variable("same_key") == "some_value"
        assert SecretCache.get_connection_uri("same_key") == "some_other_value"

        SecretCache.save_variable("var", "some_value")
        SecretCache.save_connection_uri("conn", "some_other_value")

        # getting the wrong type of thing with a key that exists in the other will not work
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_connection_uri("var")
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable("conn")

    def test_connections_do_not_save_none(self):
        # noinspection PyTypeChecker
        SecretCache.save_connection_uri("key", None)

        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_connection_uri("key")
