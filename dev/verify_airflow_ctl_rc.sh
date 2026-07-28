#!/usr/bin/env bash
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
#
# Non-interactive contributor smoke test for an Apache Airflow CTL release
# candidate.
#
# Meant to be run *inside* the Breeze container so a single command brings up
# Airflow and exercises ``airflowctl`` end-to-end against the live API.
# ``breeze shell`` configures SimpleAuthManager with admin/admin and a per-shell
# JWT secret, so booting ``airflow standalone`` and running the CLI in the
# *same* shell is all that is needed.
#
# Note on auth in a headless container: ``airflowctl auth login`` with a
# username/password stores the token in the OS keyring, which has no backend in
# the container. The headless path is to mint a token with ``airflowctl auth
# token`` and export it as ``AIRFLOW_CLI_TOKEN`` (``airflowctl auth login
# --skip-keyring`` still persists the api-url so subsequent commands know where
# to connect).
#
# Usage (from the repo root, with the chosen RC version):
#
#   CTL_VERSION=0.1.5rc1 breeze shell --load-example-dags --backend sqlite \
#       "bash dev/verify_airflow_ctl_rc.sh"
#
# To test the exact SVN artifact instead of the PyPI RC, point CTL_WHEEL at the
# wheel (visible under /opt/airflow inside the container):
#
#   CTL_WHEEL=/opt/airflow/dist/apache_airflow_ctl-0.1.5-py3-none-any.whl \
#       breeze shell --load-example-dags "bash dev/verify_airflow_ctl_rc.sh"
set -uo pipefail

cd /opt/airflow || exit 1
export AIRFLOW__CORE__LOAD_EXAMPLES=True
API_URL="http://localhost:8080"

CTL_VERSION="${CTL_VERSION:-}"
CTL_WHEEL="${CTL_WHEEL:-}"

echo "### Starting airflow standalone (background) ###"
airflow standalone > /tmp/standalone.log 2>&1 &
SA=$!

echo "### Waiting for API server on :8080 ###"
up=0
for i in $(seq 1 90); do
  if curl -fsS -o /dev/null --max-time 3 "${API_URL}/api/v2/monitor/health" 2>/dev/null; then
    echo "API healthy after ~$((i * 4))s"; up=1; break
  fi
  sleep 4
done
if [[ "${up}" != "1" ]]; then
  echo "ERROR: API server did not become healthy"; tail -40 /tmp/standalone.log; kill "${SA}" 2>/dev/null; exit 1
fi

echo "### Installing apache-airflow-ctl ###"
if [[ -n "${CTL_WHEEL}" ]]; then
  pip install --force-reinstall "${CTL_WHEEL}"
elif [[ -n "${CTL_VERSION}" ]]; then
  pip install --force-reinstall "apache-airflow-ctl==${CTL_VERSION}"
else
  echo "ERROR: set CTL_VERSION (e.g. 0.1.5rc1) or CTL_WHEEL"; kill "${SA}" 2>/dev/null; exit 2
fi
airflowctl version

echo "### Authenticating (headless: token + AIRFLOW_CLI_TOKEN) ###"
TOKEN=$(airflowctl auth token --api-url "${API_URL}" --username admin --password admin 2>/dev/null | tail -1)
airflowctl auth login --api-url "${API_URL}" --api-token "${TOKEN}" --skip-keyring
export AIRFLOW_CLI_TOKEN="${TOKEN}"

echo "### Exercising commands against the live API ###"
rc=0
for cmd in "dags list" "pools list" "connections list" "variables list"; do
  echo "--- airflowctl ${cmd} ---"
  # shellcheck disable=SC2086
  if ! airflowctl ${cmd} -o json; then rc=1; fi
done

kill "${SA}" 2>/dev/null || true
if [[ "${rc}" == "0" ]]; then
  echo "AIRFLOW_CTL_RC_VERIFY: OK"
else
  echo "AIRFLOW_CTL_RC_VERIFY: FAILED"
fi
exit "${rc}"
