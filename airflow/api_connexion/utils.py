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

from pendulum.exceptions import ParserError

from airflow.api_connexion.exceptions import BadRequest
from airflow.utils import timezone


def conn_parse_datetime(datetime: str):
    """
    Datetime format parser for args since connexion doesn't parse datetimes
    https://github.com/zalando/connexion/issues/476

    This should only be used within connection views because it raises 400
    """
    if datetime[-1] != 'Z':
        datetime = datetime.replace(" ", '+')
    try:
        datetime = timezone.parse(datetime)
    except (ParserError, TypeError) as err:
        raise BadRequest("Incorrect datetime argument",
                         detail=str(err)
                         )
    return datetime
