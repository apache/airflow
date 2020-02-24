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
"""Dags APIs.."""
import re
import typing

from airflow.models.dag import DagModel
from airflow.utils.session import provide_session


@provide_session
def get_dags(regex: str = '', session=None) -> typing.List[str]:
    """
    Return a list of ids of the dags located within the dagbag.
    Optionally filter the dag list with a `regex` string.
    """
    dags = [y for x in session.query(DagModel.dag_id) for y in x]

    if regex:
        pattern = re.compile(regex)
        return list(filter(pattern.match, dags))
    else:
        return dags
