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

import traceback
from logging import Logger

from sqlalchemy.orm import Session


def print_session_usage(log: Logger):
    """Makes any calls to Session.query/merge print stacktrace.

    This allows to check the usages of any Session.query/Session.merge usages
    for planning the next steps for AIP-44.
    """
    current_query = Session.query
    current_merge = Session.merge

    def new_query(self, *entities, **kwargs):
        trace_str = "".join(list(map(str, traceback.format_stack(limit=5))))
        log.warning("QUERY:\n%s", trace_str)
        return current_query(self, *entities, **kwargs)

    def new_merge(self, instance, load):
        trace_str = "".join(list(map(str, traceback.format_stack(limit=5))))
        log.warning("MERGE:%s", trace_str)
        return current_merge(self, instance, load)

    Session.query = new_query
    Session.merge = new_merge
