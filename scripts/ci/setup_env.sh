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

set -o verbose

MINIKDC_VERSION=2.7.1

HADOOP_DISTRO=${HADOOP_DISTRO:-"hdp"}

ONLY_DOWNLOAD=${ONLY_DOWNLOAD:-false}
ONLY_EXTRACT=${ONLY_EXTRACT:-false}

MINICLUSTER_URL=https://github.com/bolkedebruin/minicluster/releases/download/1.1/minicluster-1.1-SNAPSHOT-bin.zip

HIVE_HOME=/tmp/hive

while test $# -gt 0; do
    case "$1" in
        -h|--help)
            echo "Setup environment for airflow tests"
            echo " "
            echo "options:"
            echo -e "\t-h, --help            show brief help"
            echo -e "\t-o, --only-download   just download hadoop tar(s)"
            echo -e "\t-e, --only-extract    just extract hadoop tar(s)"
            echo -e "\t-d, --distro          select distro (hdp|cdh)"
            exit 0
            ;;
        -o|--only-download)
            shift
            ONLY_DOWNLOAD=true
            ;;
        -e|--only-extract)
            shift
            ONLY_EXTRACT=true
            ;;
        -d|--distro)
            shift
            if test $# -gt 0; then
                HADOOP_DISTRO=$1
            else
                echo "No Hadoop distro specified - abort" >&2
                exit 1
            fi
            shift
            ;;
        *)
            echo "Unknown options: $1" >&2
            exit 1
            ;;
    esac
done

HADOOP_HOME=/tmp/hadoop-${HADOOP_DISTRO}
MINICLUSTER_HOME=/tmp/minicluster

if $ONLY_DOWNLOAD && $ONLY_EXTRACT; then
    echo "Both only-download and only-extract specified - abort" >&2
    exit 1
fi

mkdir -p ${HADOOP_HOME}
mkdir -p ${TRAVIS_CACHE}/${HADOOP_DISTRO}
mkdir -p ${TRAVIS_CACHE}/minicluster
mkdir -p ${TRAVIS_CACHE}/hive
mkdir -p ${HIVE_HOME}
chmod -R 777 ${HIVE_HOME}
mkdir -p /user/hive/warehouse

if [ $HADOOP_DISTRO = "cdh" ]; then
    URL="http://archive.cloudera.com/cdh5/cdh/5/hadoop-latest.tar.gz"
    HIVE_URL="http://archive.cloudera.com/cdh5/cdh/5/hive-latest.tar.gz"
elif [ $HADOOP_DISTRO = "hdp" ]; then
    URL="http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.3.2.0/tars/hadoop-2.7.1.2.3.2.0-2950.tar.gz"
    HIVE_URL="http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.3.2.0/tars/apache-hive-1.2.1.2.3.2.0-2950-bin.tar.gz"
else
    echo "No/bad HADOOP_DISTRO='${HADOOP_DISTRO}' specified" >&2
    exit 1
fi

if ! $ONLY_EXTRACT; then
    echo "Downloading Hadoop from $URL to ${TRAVIS_CACHE}/${HADOOP_DISTRO}/hadoop.tar.gz"
    curl -z ${TRAVIS_CACHE}/${HADOOP_DISTRO}/hadoop.tar.gz -o ${TRAVIS_CACHE}/${HADOOP_DISTRO}/hadoop.tar.gz -L $URL

    if [ $? != 0 ]; then
        echo "Failed to download Hadoop from $URL - abort" >&2
        exit 1
    fi
fi

if $ONLY_DOWNLOAD; then
    exit 0
fi

echo "Extracting ${HADOOP_HOME}/hadoop.tar.gz into $HADOOP_HOME"
tar zxf ${TRAVIS_CACHE}/${HADOOP_DISTRO}/hadoop.tar.gz --strip-components 1 -C $HADOOP_HOME

if [ $? != 0 ]; then
    echo "Failed to extract Hadoop from ${HADOOP_HOME}/hadoop.tar.gz to ${HADOOP_HOME} - abort" >&2
    echo "Trying again..." >&2
    # dont use cache
    curl -o ${TRAVIS_CACHE}/${HADOOP_DISTRO}/hadoop.tar.gz -L $URL
    tar zxf ${TRAVIS_CACHE}/${HADOOP_DISTRO}/hadoop.tar.gz --strip-components 1 -C $HADOOP_HOME
    if [ $? != 0 ]; then
        echo "Failed twice in downloading and unpacking hadoop!" >&2
        exit 1
    fi
fi

echo "Downloading and unpacking hive"
curl -z ${TRAVIS_CACHE}/hive/hive.tar.gz -o ${TRAVIS_CACHE}/hive/hive.tar.gz -L ${HIVE_URL}
tar zxf ${TRAVIS_CACHE}/hive/hive.tar.gz --strip-components 1 -C ${HIVE_HOME}

if [ $? != 0 ]; then
    echo "Failed to extract hive from ${TRAVIS_CACHE}/hive/hive.tar.gz" >&2
    echo "Trying again..." >&2
    # dont use cache
    curl -o ${TRAVIS_CACHE}/hive/hive.tar.gz -L ${HIVE_URL}
    tar zxf ${TRAVIS_CACHE}/hive/hive.tar.gz --strip-components 1 -C ${HIVE_HOME}
    if [ $? != 0 ]; then
        echo "Failed twice in downloading and unpacking hive!" >&2
        exit 1
    fi
fi

echo "Downloading and unpacking minicluster"
curl -z ${TRAVIS_CACHE}/minicluster/minicluster.zip -o ${TRAVIS_CACHE}/minicluster/minicluster.zip -L ${MINICLUSTER_URL}
ls -l ${TRAVIS_CACHE}/minicluster/minicluster.zip
unzip ${TRAVIS_CACHE}/minicluster/minicluster.zip -d /tmp
if [ $? != 0 ] ; then
    # Try downloading w/o cache if there's a failure
    curl -o ${TRAVIS_CACHE}/minicluster/minicluster.zip -L ${MINICLUSTER_URL}
    ls -l ${TRAVIS_CACHE}/minicluster/minicluster.zip
    unzip ${TRAVIS_CACHE}/minicluster/minicluster.zip -d /tmp
    if [ $? != 0 ] ; then
        echo "Failed twice in downloading and unpacking minicluster!" >&2
        exit 1
    fi
    exit 1
fi

echo "Path = ${PATH}"

java -cp "/tmp/minicluster-1.1-SNAPSHOT/*" com.ing.minicluster.MiniCluster > /dev/null &
