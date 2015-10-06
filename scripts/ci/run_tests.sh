#!/usr/bin/env bash

if [ -z "$HADOOP_HOME" ]; then
    echo "HADOOP_HOME not set - abort" >&2
    exit 1
fi

echo "Using ${HADOOP_DISTRO} distribution of Hadoop from ${HADOOP_HOME}"

../../run_unit_tests.sh
