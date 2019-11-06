# -*- coding: utf-8 -*-
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


from functools import wraps
import logging
from six import string_types
import socket
import string
import textwrap
from typing import Any

from airflow import configuration as conf
from airflow.exceptions import InvalidStatsNameException

log = logging.getLogger(__name__)


class DummyStatsLogger:
    @classmethod
    def incr(cls, stat, count=1, rate=1):
        pass

    @classmethod
    def decr(cls, stat, count=1, rate=1):
        pass

    @classmethod
    def gauge(cls, stat, value, rate=1, delta=False):
        pass

    @classmethod
    def timing(cls, stat, dt):
        pass


# Only characters in the character set are considered valid
# for the stat_name if stat_name_default_handler is used.
ALLOWED_CHARACTERS = set(string.ascii_letters + string.digits + '_.-')


def stat_name_default_handler(stat_name, max_length=250):
    if not isinstance(stat_name, string_types):
        raise InvalidStatsNameException('The stat_name has to be a string')
    if len(stat_name) > max_length:
        raise InvalidStatsNameException(textwrap.dedent("""\
            The stat_name ({stat_name}) has to be less than {max_length} characters.
        """.format(stat_name=stat_name, max_length=max_length)))
    if not all((c in ALLOWED_CHARACTERS) for c in stat_name):
        raise InvalidStatsNameException(textwrap.dedent("""\
            The stat name ({stat_name}) has to be composed with characters in
            {allowed_characters}.
            """.format(stat_name=stat_name,
                       allowed_characters=ALLOWED_CHARACTERS)))
    return stat_name


def validate_stat(f):
    @wraps(f)
    def wrapper(_self, stat, *args, **kwargs):
        try:
            from airflow.plugins_manager import stat_name_handler
            if stat_name_handler:
                handle_stat_name_func = stat_name_handler
            else:
                handle_stat_name_func = stat_name_default_handler
            stat_name = handle_stat_name_func(stat)
        except InvalidStatsNameException:
            log.warning('Invalid stat name: {}.'.format(stat), exc_info=True)
            return
        return f(_self, stat_name, *args, **kwargs)

    return wrapper


class SafeStatsdLogger:

    def __init__(self, statsd_client):
        self.statsd = statsd_client

    @validate_stat
    def incr(self, stat, count=1, rate=1):
        return self.statsd.incr(stat, count, rate)

    @validate_stat
    def decr(self, stat, count=1, rate=1):
        return self.statsd.decr(stat, count, rate)

    @validate_stat
    def gauge(self, stat, value, rate=1, delta=False):
        return self.statsd.gauge(stat, value, rate, delta)

    @validate_stat
    def timing(self, stat, dt):
        return self.statsd.timing(stat, dt)


Stats = DummyStatsLogger  # type: Any

try:
    if conf.getboolean('scheduler', 'statsd_on'):
        from statsd import StatsClient

        statsd = StatsClient(
            host=conf.get('scheduler', 'statsd_host'),
            port=conf.getint('scheduler', 'statsd_port'),
            prefix=conf.get('scheduler', 'statsd_prefix'))
        Stats = SafeStatsdLogger(statsd)
except (socket.gaierror, ImportError) as e:
    log.warning("Could not configure StatsClient: %s, using DummyStatsLogger instead.", e)
