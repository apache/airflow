#!/usr/bin/env bash

#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MINIKDC_VERSION=2.7.1
MINIKDC_HOME=/tmp/minikdc
MINIKDC_CACHE=${CACHE}/minikdc
# Setup MiniKDC environment
mkdir -p ${MINIKDC_HOME}
mkdir -p ${MINIKDC_CACHE}

URL=http://search.maven.org/remotecontent?filepath=org/apache/ivy/ivy/2.3.0/ivy-2.3.0.jar
echo "Downloading ivy"
curl -z ${MINIKDC_CACHE}/ivy.jar -o ${MINIKDC_CACHE}/ivy.jar -L ${URL}

if [ $? != 0 ]; then
    echo "Failed to download ivy"
    exit 1
fi

echo "Getting minikdc dependencies"
java -jar ${MINIKDC_CACHE}/ivy.jar -dependency org.apache.hadoop hadoop-minikdc ${MINIKDC_VERSION} \
           -cache ${MINIKDC_CACHE} \
           -retrieve "${MINIKDC_CACHE}/lib/[artifact]-[revision](-[classifier]).[ext]"

if [ $? != 0 ]; then
    echo "Failed to download dependencies for minikdc"
    exit 1
fi

cp -r ${MINIKDC_CACHE}/* ${MINIKDC_HOME}/

mkdir -p ${MINIKDC_HOME}/work
