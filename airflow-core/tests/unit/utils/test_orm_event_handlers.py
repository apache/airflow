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

"""
These tests ensure compatibility with the deprecation of `sqlalchemy.orm.mapper()` in SQLAlchemy 2.0.0.b1.
See also: https://docs.sqlalchemy.org/en/21/orm/mapping_styles.html#orm-imperative-mapping
"""

from __future__ import annotations

import contextlib
import warnings

from sqlalchemy.exc import InvalidRequestError, SADeprecationWarning

from airflow.utils.orm_event_handlers import setup_event_handlers


def test_setup_event_handlers_no_sa_deprecation_warning():
    warnings.simplefilter("error", category=SADeprecationWarning)
    with contextlib.suppress(InvalidRequestError):
        # We are only interested in the warning that may be issued at the beginning of the method.
        setup_event_handlers(engine=None)
