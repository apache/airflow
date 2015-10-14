#!/usr/bin/env bash
MINIKDC_VERSION=2.7.1
MINIKDC_HOME=/tmp/minikdc

# Setup MiniKDC environment
mkdir -p ${MINIKDC_HOME}

URL=http://search.maven.org/remotecontent?filepath=org/apache/ivy/ivy/2.3.0/ivy-2.3.0.jar
echo "Downloading ivy"
curl -o ${MINIKDC_HOME}/ivy.jar -L ${URL}

if [ $? != 0 ]; then
    echo "Failed to download ivy"
    exit 1
fi

echo "Getting minikdc dependencies"
java -jar ${MINIKDC_HOME}/ivy.jar -dependency org.apache.hadoop hadoop-minikdc ${MINIKDC_VERSION} \
           -retrieve "${MINIKDC_HOME}/lib/[artifact]-[revision](-[classifier]).[ext]"

if [ $? != 0 ]; then
    echo "Failed to download dependencies for minikdc"
    exit 1
fi

mkdir -p ${MINIKDC_HOME}/work
