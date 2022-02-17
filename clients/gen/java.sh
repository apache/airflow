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

CLIENTS_GEN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
readonly CLIENTS_GEN_DIR
echo "$CLIENTS_GEN_DIR"

CLEANUP_DIRS=(api docs src)
readonly CLEANUP_DIRS
echo $CLEANUP_DIRS

# shellcheck source=./clients/gen/common.sh
source "${CLIENTS_GEN_DIR}/common.sh"

VERSION=2.1.0
readonly VERSION

java_config=(
     "groupId=com.apache.airflow"
     "artifactId=airflow-client"
     "artifactVersion=${VERSION}"
     "artifactUrl=https://github.com/apache/airflow-client-java"
     "artifactDescription=Apache Airflow - OpenApi Client for Java"

     "basePackage=com.apache.airflow.client"
     "configPackage=com.apache.airflow.client.config"
     "apiPackage=com.apache.airflow.client.api"
     "modelPackage=com.apache.airflow.client.model"
     "dateLibrary=java8"
     "java8=true"
     "hideGenerationTimestamp=true"

     "developerEmail=dev@airflow.apache.org"
     "developerName=Apache Airflow Developers"
     "developerOrganization=Apache Software Foundation"
     "developerOrganizationUrl=https://airflow.apache.org"
     "scmConnection=scm:git:git@github.com:apache/apache-client-java.git"
     "scmDeveloperConnection=scm:git:git@github.com:apache/apache-client-java.git"
     "scmUrl=https://github.com/apache/airflow-client-java"

     "licenseName=Apache License 2.0"
     "licenseUrl=https://github.com/apache/airflow-client-java/blob/master/LICENSE"
)

validate_input "$@"

# additional-properties key value tuples need to be separated by comma, not space
IFS=,

gen_client java \
    --package-name airflow \
    --git-repo-id airflow-client-java/airflow \
    --additional-properties "${java_config[*]}"

#copying tests
TEST_SOURCE_DIR=$(dirname $OUTPUT_DIR)"/dev/tests"
echo "TEST_SOURCE_DIR: $TEST_SOURCE_DIR"

TEST_DEST_DIR=$OUTPUT_DIR/src/test/java/com/apache/airflow/client/dev
mkdir $TEST_DEST_DIR
cp "$TEST_SOURCE_DIR"/* "$TEST_DEST_DIR"

# run_pre_commit
echo "Generation successful"
