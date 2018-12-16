#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
AIRFLOW_BREEZE_CONFIG_DIR=${AIRFLOW_BREEZE_CONFIG_DIR:=${HOME}/airflow-breeze-config}
export GCP_SERVICE_ACCOUNT_KEY_DIR=${AIRFLOW_BREEZE_CONFIG_DIR/keys

pushd ${GCP_SERVICE_ACCOUNT_KEY_DIR}
for FILE in *.json *.pem;
do
  gcloud kms encrypt --plaintext-file ${FILE} --ciphertext-file ${FILE}.enc \
     --location=global --keyring=incubator-airflow --key=service_accounts_crypto_key \
     --project=${GCP_PROJECT_ID}
     && echo Encrypted ${FILE}
done
popd
