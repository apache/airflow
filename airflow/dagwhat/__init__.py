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

# This module defines an experimental API to define property tests for Airflow
# DAGs.
# A property test works by defining a series of invariants and verifying
# program behavior against them.
#
# Principles of the `dagwhat`
#  1. Dagwhat is *not* concerned with specific operator functionality
#  2. Dagwhat is concerned with DAG architecture, and expected DAG execution
#     behavior.
#  3. TODO(pabloem)


from airflow.dagwhat.api import *
