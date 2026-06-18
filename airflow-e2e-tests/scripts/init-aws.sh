#!/bin/bash
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

set -euo pipefail

# This script runs as a LocalStack READY init hook *inside* the localstack
# container, so it must reach the gateway over loopback (localhost), not via the
# docker compose service name "localstack" — the service-name endpoint is not
# reliably resolvable/connectable from within the container at the READY stage
# and intermittently caused "Could not connect to the endpoint URL" failures,
# leaving the buckets uncreated and remote-logging tests failing with NoSuchBucket.
endpoint_url="http://localhost:4566"

# The READY hook fires when LocalStack reports ready, but guard against a
# transient gateway delay so bucket creation never silently fails.
for _ in $(seq 1 30); do
  if aws --endpoint-url="${endpoint_url}" s3 ls >/dev/null 2>&1; then
    break
  fi
  echo "Waiting for LocalStack S3 gateway at ${endpoint_url} ..."
  sleep 1
done

aws --endpoint-url="${endpoint_url}" s3 mb s3://test-airflow-logs
aws --endpoint-url="${endpoint_url}" s3 mb s3://test-xcom-objectstorage-backend
aws --endpoint-url="${endpoint_url}" s3 ls
