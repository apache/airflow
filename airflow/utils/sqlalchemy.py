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
from typing import Any, Dict

import pendulum
from dateutil import relativedelta
from sqlalchemy.orm.session import Session
from sqlalchemy.types import DateTime, Text, TypeDecorator

from airflow.configuration import conf

log = logging.getLogger(__name__)

utc = pendulum.tz.timezone('UTC')

using_mysql = conf.get('core', 'sql_alchemy_conn').lower().startswith('mysql')


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
            # For mysql we should store timestamps as naive values
            # Timestamp in MYSQL is not timezone aware. In MySQL 5.6
            # timezone added at the end is ignored but in MySQL 5.7
            # inserting timezone value fails with 'invalid-date'
            # See https://issues.apache.org/jira/browse/AIRFLOW-7001
            if using_mysql:
                from airflow.utils.timezone import make_naive
                return make_naive(value, timezone=utc)
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


def skip_locked(session: Session) -> Dict[str, Any]:
    """
    Return kargs for passing to `with_for_update()` suitable for the current DB engine version.

    We do this as we document the fact that on DB engines that don't support this construct, we do not
    support/recommend running HA scheduler. If a user ignores this and tries anyway everything will still
    work, just slightly slower in some circumstances.

    Specifically don't emit SKIP LOCKED for MySQL < 8, or MariaDB, neither of which support this construct

    See https://jira.mariadb.org/browse/MDEV-13115
    """
    dialect = session.bind.dialect

    if dialect.name != "mysql" or dialect.supports_for_update_of:
        return {'skip_locked': True}
    else:
        return {}
