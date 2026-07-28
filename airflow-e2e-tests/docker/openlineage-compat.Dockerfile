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

# Lightweight image for running the OpenLineage e2e tests against an OLDER, released Airflow version.
# Instead of building a full PROD image, it starts from the published apache/airflow:<version> image
# and reinstalls the providers under test (OpenLineage + the providers the system-test DAGs rely on)
# from wheels built from main — so the current provider code runs on an older Airflow core, the same
# idea as the provider compatibility tests. The default (prod) run does NOT use this Dockerfile; it
# uses the breeze PROD image with current sources.
ARG AIRFLOW_BASE_IMAGE
FROM ${AIRFLOW_BASE_IMAGE}

COPY --chown=airflow:0 provider_dist/ /tmp/provider_dist/
RUN pip install --no-cache-dir --upgrade /tmp/provider_dist/*.whl
