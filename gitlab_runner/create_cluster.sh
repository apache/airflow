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

set -xeuo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# shellcheck source=scripts/ci/_utils.sh
.  "${MY_DIR}/_config.sh"

gcloud beta container --project "${PROJECT_ID}" clusters create \
  "${CLUSTER_NAME}" --zone "${ZONE}" --no-enable-basic-auth \
  --cluster-version "${CLUSTER_VERSION}" \
  --machine-type "custom-6-23040" \
  --image-type "COS" --disk-type "pd-standard" --disk-size "100" \
  --local-ssd-count "1" --metadata disable-legacy-endpoints=true \
  --max-pods-per-node "${MAX_PODS_PER_NODE}" --default-max-pods-per-node "${MAX_PODS_PER_NODE}" \
  --preemptible --enable-autoscaling \
  --num-nodes "${MIN_NODES}" --min-nodes "${MIN_NODES}" --max-nodes "${MAX_NODES}" \
  --no-enable-cloud-logging --no-enable-cloud-monitoring --enable-ip-alias  \
  --addons HorizontalPodAutoscaling,HttpLoadBalancing,KubernetesDashboard \
  --enable-autoupgrade --enable-autorepair

gcloud container clusters get-credentials "${CLUSTER_NAME}"
