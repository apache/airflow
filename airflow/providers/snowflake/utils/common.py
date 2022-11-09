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


def enclose_param(param: str) -> str:
    """
    Replace all single quotes in parameter by two single quotes and enclose param in single quote.

    .. seealso::
        https://docs.snowflake.com/en/sql-reference/data-types-text.html#single-quoted-string-constants

    Examples:
     .. code-block:: python

        enclose_param("without quotes")  # Returns: 'without quotes'
        enclose_param("'with quotes'")  # Returns: '''with quotes'''
        enclose_param("Today's sales projections")  # Returns: 'Today''s sales projections'
        enclose_param("sample/john's.csv")  # Returns: 'sample/john''s.csv'
        enclose_param(".*'awesome'.*[.]csv")  # Returns: '.*''awesome''.*[.]csv'

    :param param: parameter which required single quotes enclosure.
    """
    return f"""'{param.replace("'", "''")}'"""
