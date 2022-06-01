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

from datetime import datetime


def datetime_to_epoch(dt: datetime) -> int:
    """Convert a datetime object to an epoch integer (seconds)."""
    return int(dt.timestamp())


def datetime_to_epoch_ms(dt: datetime) -> int:
    """Convert a datetime object to an epoch integer (milliseconds)."""
    return int(dt.timestamp() * 1_000)


def datetime_to_epoch_us(dt: datetime) -> int:
    """Convert a datetime object to an epoch integer (microseconds)."""
    return int(dt.timestamp() * 1_000_000)
