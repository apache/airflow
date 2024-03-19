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

import sys
from datetime import datetime

from airflow.models.dag import DAG

# Tests to make sure that a system exit won't cause the scheduler to fail.
# Start with 'z' to get listed last.


DEFAULT_DATE = datetime(2100, 1, 1)

dag1 = DAG(dag_id="test_system_exit", start_date=DEFAULT_DATE)

sys.exit(-1)
