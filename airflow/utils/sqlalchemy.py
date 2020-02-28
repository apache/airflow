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

import datetime
import json
import logging
import os
import time
import traceback

import pendulum
from dateutil import relativedelta
from sqlalchemy import event, exc
from sqlalchemy.types import DateTime, Text, TypeDecorator

from airflow.configuration import conf

log = logging.getLogger(__name__)

utc = pendulum.timezone('UTC')


def setup_event_handlers(engine):
    """
    Setups event handlers.
    """
    # pylint: disable=unused-argument
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    if engine.dialect.name == "sqlite":
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    # this ensures sanity in mysql when storing datetimes (not required for postgres)
    if engine.dialect.name == "mysql":
        @event.listens_for(engine, "connect")
        def set_mysql_timezone(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("SET time_zone = '+00:00'")
            cursor.close()

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                "Connection record belongs to pid {}, "
                "attempting to check out in pid {}".format(connection_record.info['pid'], pid)
            )
    if conf.getboolean('debug', 'sqlalchemy_stats', fallback=False):
        @event.listens_for(engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            conn.info.setdefault('query_start_time', []).append(time.time())

        @event.listens_for(engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            total = time.time() - conn.info['query_start_time'].pop()
            file_name = [
                f"'{f.name}':{f.filename}:{f.lineno}" for f
                in traceback.extract_stack() if 'sqlalchemy' not in f.filename][-1]
            stack = [f for f in traceback.extract_stack() if 'sqlalchemy' not in f.filename]
            stack_info = ">".join([f"{f.filename.rpartition('/')[-1]}:{f.name}" for f in stack][-3:])
            conn.info.setdefault('query_start_time', []).append(time.monotonic())
            log.info("@SQLALCHEMY %s |$ %s |$ %s |$  %s ",
                     total, file_name, stack_info, statement.replace("\n", " ")
                     )

# pylint: enable=unused-argument


class UtcDateTime(TypeDecorator):
    """
    Almost equivalent to :class:`~sqlalchemy.types.DateTime` with
    ``timezone=True`` option, but it differs from that by:

    - Never silently take naive :class:`~datetime.datetime`, instead it
      always raise :exc:`ValueError` unless time zone aware value.
    - :class:`~datetime.datetime` value's :attr:`~datetime.datetime.tzinfo`
      is always converted to UTC.
    - Unlike SQLAlchemy's built-in :class:`~sqlalchemy.types.DateTime`,
      it never return naive :class:`~datetime.datetime`, but time zone
      aware value, even with SQLite or MySQL.
    - Always returns DateTime in UTC

    """

    impl = DateTime(timezone=True)

    def process_bind_param(self, value, dialect):
        if value is not None:
            if not isinstance(value, datetime.datetime):
                raise TypeError('expected datetime.datetime, not ' +
                                repr(value))
            elif value.tzinfo is None:
                raise ValueError('naive datetime is disallowed')

            return value.astimezone(utc)
        return None

    def process_result_value(self, value, dialect):
        """
        Processes DateTimes from the DB making sure it is always
        returning UTC. Not using timezone.convert_to_utc as that
        converts to configured TIMEZONE while the DB might be
        running with some other setting. We assume UTC datetimes
        in the database.
        """
        if value is not None:
            if value.tzinfo is None:
                value = value.replace(tzinfo=utc)
            else:
                value = value.astimezone(utc)

        return value


class Interval(TypeDecorator):
    """
    Base class representing a time interval.
    """
    impl = Text

    attr_keys = {
        datetime.timedelta: ('days', 'seconds', 'microseconds'),
        relativedelta.relativedelta: (
            'years', 'months', 'days', 'leapdays', 'hours', 'minutes', 'seconds', 'microseconds',
            'year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond',
        ),
    }

    def process_bind_param(self, value, dialect):
        if isinstance(value, tuple(self.attr_keys)):
            attrs = {
                key: getattr(value, key)
                for key in self.attr_keys[type(value)]
            }
            return json.dumps({'type': type(value).__name__, 'attrs': attrs})
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if not value:
            return value
        data = json.loads(value)
        if isinstance(data, dict):
            type_map = {key.__name__: key for key in self.attr_keys}
            return type_map[data['type']](**data['attrs'])
        return data
