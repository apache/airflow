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
Default configuration for this branch. Those two variables below
should be the only one that should be changed when we branch off
different airflow branch.

This file is different in every branch (`main`, `vX_Y_test' of airflow)
The _stable branches have the same values as _test branches.

Examples:

    main:

        AIRFLOW_BRANCH = "main"
        DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH = "constraints-main"

    v2-2-test:

        AIRFLOW_BRANCH = "v2-2-test"
        DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH = "constraints-2-2"

"""
from __future__ import annotations

AIRFLOW_BRANCH = "main"
DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH = "constraints-main"
DEBIAN_VERSION = "bullseye"
