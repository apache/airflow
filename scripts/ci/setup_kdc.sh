#!/usr/bin/env bash
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
