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
from __future__ import annotations

from pathlib import Path

AUXILIARY_ASSET_TYPES = {"vertex_ai_raycluster": "vertex_ai_raycluster.*"}

ASSET_TYPES = {
    "ai": "aiplatform.googleapis.com.*",
    "alloydb": "alloydb.googleapis.com.*",
    "artifact": "artifactregistry.googleapis.com.*",
    "batch": "batch.googleapis.com.*",
    "bq": "bigquery.googleapis.com.*",
    "bqtransfer": "bigquerydatatransfer.googleapis.com.*",
    "bigtable": "bigtableadmin.googleapis.com.*",
    "cloudbuild": "cloudbuild.googleapis.com.*",
    "cloudtasks": "cloudtasks.googleapis.com.*",
    "composer": "composer.googleapis.com.*",
    "compute": "compute.googleapis.com.*",
    "dataflow": "dataflow.googleapis.com.*",
    "dataform": "dataform.googleapis.com.*",
    "datafusion": "datafusion.googleapis.com.*",
    "dataplex": "dataplex.googleapis.com.*",
    "dataproc": "dataproc.googleapis.com.*",
    "dataproc_metastore": "metastore.googleapis.com.*",
    "dlp": "dlp.googleapis.com.*",
    "firestore": "firestore.googleapis.com.*",
    "gke": "container.googleapis.com.*",
    "logging": "logging.googleapis.com.*",
    "memcache": "memcache.googleapis.com.*",
    "metastore": "metastore.googleapis.com.*",
    "monitoring": "monitoring.googleapis.com.*",
    "pubsub": "pubsub.googleapis.com.*",
    "run": "run.googleapis.com.*",
    "sqladmin": "sqladmin.googleapis.com.*",
    "storage": "storage.googleapis.com.*",
    "storagetransfer": "storagetransfer.googleapis.com.*",
    "kafka": "managedkafka.googleapis.com.*",
    "workflows": "workflows.googleapis.com.*",
    "firebase": "firebaserules.googleapis.com.*",
    "container": "container.googleapis.com.*",
    "spanner": "spanner.googleapis.com.*",
} | AUXILIARY_ASSET_TYPES

ASSET_TYPE_OPTIONS = list(ASSET_TYPES.keys())

_CWD = Path.cwd()
RESOURCES_FOLDER = _CWD / "resources"
OUTPUT_FOLDER = _CWD / "output"
CONFIG_FOLDER = _CWD / "config"

DO_NOT_DELETE_LABELS = {
    "owner",
    "do-not-delete",
    "dont-delete",
    "donotdelete",
    "dontdelete",
}

DO_NOT_DELETE_ASSET_TYPES = {
    "cloudresourcemanager.googleapis.com/Organization",
    "cloudresourcemanager.googleapis.com/Project",
    "cloudbilling.googleapis.com/ProjectBillingInfo",
    "iam.googleapis.com/ServiceAccount",
    "iam.googleapis.com/ServiceAccountKey",
    "compute.googleapis.com/Network",  # avoid to delete networks
    "compute.googleapis.com/Route",  # avoid to delete auto peering routes
    "osconfig.googleapis.com/OSPolicyAssignment",
    "serviceusage.googleapis.com/Service",
    "apikeys.googleapis.com/Key",
    "secretmanager.googleapis.com/Secret",
    "secretmanager.googleapis.com/SecretVersion",
    "servicedirectory.googleapis.com/Namespace",
    "servicenetworking.googleapis.com/Connection",
    "serviceusage.googleapis.com/Service",
    "apikeys.googleapis.com/Key",
    "dataproc.googleapis.com/AutoscalingPolicy",
}
