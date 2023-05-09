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

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.triggers.s3 import S3KeyTrigger

bucket_name = "enve388d72f-s3-bucket"
key = "enve388d72f-key"

trigger = (
    S3KeyTrigger(
        bucket_name=bucket_name,
        bucket_key=key,
        wildcard_match=False,
        check_fn=None,
        aws_conn_id="aws_default",
        verify=False,
    ),
)


sensor_one_key = S3KeySensor(
    task_id="sensor_one_key",
    bucket_name=bucket_name,
    bucket_key=key,
    deferrable=True,
)

op = S3KeySensor.execute(None)
