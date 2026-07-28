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
# Non-interactive contributor smoke test for an Apache Airflow Python Client
# release candidate.
#
# The documented contributor flow (README_RELEASE_PYTHON_CLIENT.md) uses the
# interactive ``breeze start-airflow`` + ``test_python_client.py``. This script
# is the headless equivalent: it is meant to be run *inside* the Breeze
# container so a single command brings up Airflow and exercises the client
# end-to-end. ``breeze shell`` configures SimpleAuthManager with admin/admin and
# a per-shell JWT secret, so booting ``airflow standalone`` and running the test
# in the *same* shell is all that is needed.
#
# Usage (from the repo root, with the chosen RC version):
#
#   CLIENT_VERSION=3.2.2rc1 breeze shell --load-example-dags --backend sqlite \
#       "bash dev/verify_python_client_rc.sh"
#
# By default the client is installed from PyPI (the RC is published there with
# the rcN suffix). To test the exact SVN artifact instead, mount/copy the wheel
# into the repo (it is available under /opt/airflow inside the container) and
# point CLIENT_WHEEL at it:
#
#   CLIENT_WHEEL=/opt/airflow/dist/apache_airflow_client-3.2.2-py3-none-any.whl \
#       breeze shell --load-example-dags "bash dev/verify_python_client_rc.sh"
set -uo pipefail

cd /opt/airflow || exit 1
export AIRFLOW__CORE__LOAD_EXAMPLES=True

CLIENT_VERSION="${CLIENT_VERSION:-}"
CLIENT_WHEEL="${CLIENT_WHEEL:-}"

echo "### Starting airflow standalone (background) ###"
airflow standalone > /tmp/standalone.log 2>&1 &
SA=$!

echo "### Waiting for API server on :8080 ###"
up=0
for i in $(seq 1 90); do
  if curl -fsS -o /dev/null --max-time 3 http://localhost:8080/api/v2/monitor/health 2>/dev/null; then
    echo "API healthy after ~$((i * 4))s"; up=1; break
  fi
  sleep 4
done
if [[ "${up}" != "1" ]]; then
  echo "ERROR: API server did not become healthy"; tail -40 /tmp/standalone.log; kill "${SA}" 2>/dev/null; exit 1
fi

echo "### Installing Apache Airflow Python Client ###"
if [[ -n "${CLIENT_WHEEL}" ]]; then
  pip install --force-reinstall "${CLIENT_WHEEL}"
elif [[ -n "${CLIENT_VERSION}" ]]; then
  pip install --force-reinstall "apache-airflow-client==${CLIENT_VERSION}"
else
  echo "ERROR: set CLIENT_VERSION (e.g. 3.2.2rc1) or CLIENT_WHEEL"; kill "${SA}" 2>/dev/null; exit 2
fi
python -c "import importlib.metadata as m; print('installed client version:', m.version('apache-airflow-client'))"

echo "### Running test_python_client.py against the live API ###"
python /opt/airflow/clients/python/test_python_client.py
rc=$?

kill "${SA}" 2>/dev/null || true
if [[ "${rc}" == "0" ]]; then
  echo "PYTHON_CLIENT_RC_VERIFY: OK"
else
  echo "PYTHON_CLIENT_RC_VERIFY: FAILED (rc=${rc})"
fi
exit "${rc}"
