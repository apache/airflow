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

from airflow.utils.state import State

EXECUTION_STATES = {
    State.RUNNING,
    State.QUEUED,
}

# In order to be able to get queued a task must have one of these states
SCHEDULEABLE_STATES = {
    State.NONE,
    State.UP_FOR_RETRY,
    State.UP_FOR_RESCHEDULE,
}

RUNNABLE_STATES = {
    # For cases like unit tests and run manually
    State.NONE,
    State.UP_FOR_RETRY,
    State.UP_FOR_RESCHEDULE,
    # For normal scheduler/backfill cases
    State.QUEUED,
}

QUEUEABLE_STATES = {
    State.SCHEDULED,
}

BACKFILL_QUEUEABLE_STATES = {
    # For cases like unit tests and run manually
    State.NONE,
    State.UP_FOR_RESCHEDULE,
    State.UP_FOR_RETRY,
    # For normal backfill cases
    State.SCHEDULED,
}
