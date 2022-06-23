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
import logging
import re
import traceback
from collections import Counter
from contextlib import contextmanager
from typing import Optional

from sqlalchemy import event

# Long import to not create a copy of the reference, but to refer to one place.
import airflow.settings

log = logging.getLogger(__name__)


def assert_equal_ignore_multiple_spaces(case, first, second, msg=None):
    def _trim(s):
        return re.sub(r"\s+", " ", s.strip())

    return case.assertEqual(_trim(first), _trim(second), msg)


class CountQueries:
    """
    Counts the number of queries sent to Airflow Database in a given context.

    Does not support multiple processes. When a new process is started in context, its queries will
    not be included.
    """

    def __init__(self):
        self.result = Counter()

    def __enter__(self):
        event.listen(airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute)
        return self.result

    def __exit__(self, type_, value, tb):
        event.remove(airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute)
        log.debug("Queries count: %d", sum(self.result.values()))

    def after_cursor_execute(self, *args, **kwargs):
        stack = [
            f
            for f in traceback.extract_stack()
            if 'sqlalchemy' not in f.filename
            and __file__ != f.filename
            and ('session.py' not in f.filename and f.name != 'wrapper')
        ]
        stack_info = ">".join([f"{f.filename.rpartition('/')[-1]}:{f.name}:{f.lineno}" for f in stack][-5:])
        self.result[f"{stack_info}"] += 1


count_queries = CountQueries


@contextmanager
def assert_queries_count(expected_count: int, message_fmt: Optional[str] = None, margin: int = 0):
    """
    Asserts that the number of queries is as expected with the margin applied
    The margin is helpful in case of complex cases where we do not want to change it every time we
    changed queries, but we want to catch cases where we spin out of control
    :param expected_count: expected number of queries
    :param message_fmt: message printed optionally if the number is exceeded
    :param margin: margin to add to expected number of calls
    """
    with count_queries() as result:
        yield None

    count = sum(result.values())
    if count > expected_count + margin:
        message_fmt = (
            message_fmt
            or "The expected number of db queries is {expected_count} with extra margin: {margin}. "
            "The current number is {current_count}.\n\n"
            "Recorded query locations:"
        )
        message = message_fmt.format(current_count=count, expected_count=expected_count, margin=margin)

        for location, count in result.items():
            message += f'\n\t{location}:\t{count}'

        raise AssertionError(message)
