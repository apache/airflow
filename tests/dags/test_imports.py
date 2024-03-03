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

# fmt: off

# this file contains sample code than only needs to pass the lexer
# it is "badly" formatted on purpose to test edge cases.

from __future__ import annotations

# multiline import
import  \
        datetime,   \
enum,time
"""
import airflow.in_comment
"""
# from import
from airflow.utils import file
# multiline airflow import
import airflow.decorators, airflow.models\
, airflow.sensors

if prod:
    import airflow.if_branch
else:
    import airflow.else_branch

def f():
    # local import
    import airflow.local_import

# fmt: on
