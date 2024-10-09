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
"""
Example Airflow DAG that show how to use the email decorator.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task


@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def example_email_decorator():
    # [START email_decorator]
    @task.email(
        to="joe@airflow.com",
        from_email="sender@airflow.com",
        subject="A special greeting {{ params.name }}",
    )
    def email(**context):
        return """
        <h1>Hello {{ params.name }},</h1>
        <br />
        <p>It's so nice to meet you!</p>
        """

    # [END email_decorator]


example_email_decorator()
