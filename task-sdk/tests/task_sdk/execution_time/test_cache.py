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

from airflow.sdk import SecretCache

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
    def teardown_method() -> None:
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

    @pytest.mark.parametrize(
        "team",
        [None, "team"],
    )
    def test_invalidate(self, team):
        SecretCache.save_variable("key", "some_value", team_name=team)

        assert SecretCache.get_variable("key", team_name=team) == "some_value"

        SecretCache.invalidate_variable("key", team_name=team)

        # cannot get the value for that key anymore because we invalidated it
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable("key", team_name=team)

    def test_invalidate_key_not_present(self):
        SecretCache.invalidate_variable("not present")  # simply shouldn't raise any exception.

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

    def test_independent_teams(self):
        SecretCache.save_variable("key1", "var_value1", "team1")
        SecretCache.save_connection_uri("conn1", "conn_value1", "team1")
        SecretCache.save_variable("key2", "var_value2", "team2")
        SecretCache.save_connection_uri("conn2", "conn_value2", "team2")

        assert SecretCache.get_variable("key1", team_name="team1") == "var_value1"
        assert SecretCache.get_connection_uri("conn1", team_name="team1") == "conn_value1"
        assert SecretCache.get_variable("key2", team_name="team2") == "var_value2"
        assert SecretCache.get_connection_uri("conn2", team_name="team2") == "conn_value2"

        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable("key1")
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable("key2")
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_connection_uri("conn1")
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_connection_uri("conn2")

        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable("key1", team_name="team2")
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable("key2", team_name="team1")
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_connection_uri("conn1", team_name="team2")
        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_connection_uri("conn2", team_name="team1")

    def test_connections_do_not_save_none(self):
        # noinspection PyTypeChecker
        SecretCache.save_connection_uri("key", None)

        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_connection_uri("key")

    def test_teamless_key_cannot_reach_a_team_entry(self):
        """A caller with no team must not be able to compose another team's key.

        The key used to be ``prefix + "_{team}_" + key`` when a team was given and
        ``prefix + key`` when it was not, so a team-less caller could pass
        ``"_analytics_DB_PASSWORD"`` and land on the entry stored for team
        ``analytics`` under key ``DB_PASSWORD``.
        """
        SecretCache.save_variable("DB_PASSWORD", "victim_secret", team_name="analytics")

        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_variable("_analytics_DB_PASSWORD")

    def test_teamless_key_cannot_reach_a_team_connection(self):
        SecretCache.save_connection_uri("prod_db", "postgres://victim", team_name="analytics")

        with pytest.raises(SecretCache.NotPresentException):
            SecretCache.get_connection_uri("_analytics_prod_db")

    def test_teamless_write_cannot_overwrite_a_team_entry(self):
        """The same collision must not let a team-less caller clobber a team's value."""
        SecretCache.save_variable("DB_PASSWORD", "victim_secret", team_name="analytics")
        SecretCache.save_variable("_analytics_DB_PASSWORD", "attacker_value")

        assert SecretCache.get_variable("DB_PASSWORD", team_name="analytics") == "victim_secret"
        assert SecretCache.get_variable("_analytics_DB_PASSWORD") == "attacker_value"

    def test_team_names_sharing_a_prefix_stay_separate(self):
        """Team names are compared as whole values, not as substrings of a joined key."""
        SecretCache.save_variable("k", "a_value", team_name="team")
        SecretCache.save_variable("k", "b_value", team_name="team_x")

        assert SecretCache.get_variable("k", team_name="team") == "a_value"
        assert SecretCache.get_variable("k", team_name="team_x") == "b_value"

    def test_invalidate_is_scoped_to_the_team(self):
        SecretCache.save_variable("DB_PASSWORD", "victim_secret", team_name="analytics")
        SecretCache.save_variable("_analytics_DB_PASSWORD", "attacker_value")

        SecretCache.invalidate_variable("_analytics_DB_PASSWORD")

        assert SecretCache.get_variable("DB_PASSWORD", team_name="analytics") == "victim_secret"
