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
from __future__ import annotations  # Added by precommit hooks

# from airflow.providers.amazon.aws.executors.batch.batch_executor import AwsBatchExecutor
from airflow.providers.amazon.aws.executors.batch import batch_executor

# precommit hooks (rust as of the time of commit) throws F401 - "Module imported but unused"
# One of the solutions it suggests is using a "redundant alias". This is used below instead of doing a #no-qa
# type ignore of the issue.
AwsBatchExecutor = batch_executor.AwsBatchExecutor
