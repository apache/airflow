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

from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.jobs.base_job import BaseJob


class JobSchema(SQLAlchemySchema):
    """Sla Miss Schema."""

    class Meta:
        """Meta."""

        model = BaseJob

    id = auto_field(dump_only=True)
    dag_id = auto_field(dump_only=True)
    state = auto_field(dump_only=True)
    job_type = auto_field(dump_only=True)
    start_date = auto_field(dump_only=True)
    end_date = auto_field(dump_only=True)
    latest_heartbeat = auto_field(dump_only=True)
    executor_class = auto_field(dump_only=True)
    hostname = auto_field(dump_only=True)
    unixname = auto_field(dump_only=True)
