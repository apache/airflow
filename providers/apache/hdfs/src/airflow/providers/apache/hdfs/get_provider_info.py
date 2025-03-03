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

# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE OVERWRITTEN!
#
# IF YOU WANT TO MODIFY THIS FILE, YOU SHOULD MODIFY THE TEMPLATE
# `get_provider_info_TEMPLATE.py.jinja2` IN the `dev/breeze/src/airflow_breeze/templates` DIRECTORY


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-apache-hdfs",
        "name": "Apache HDFS",
        "description": "`Hadoop Distributed File System (HDFS) <https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html>`__\nand `WebHDFS <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>`__.\n",
        "state": "ready",
        "source-date-epoch": 1740734082,
        "versions": [
            "4.7.0",
            "4.6.0",
            "4.5.1",
            "4.5.0",
            "4.4.2",
            "4.4.1",
            "4.4.0",
            "4.3.3",
            "4.3.2",
            "4.3.1",
            "4.3.0",
            "4.2.0",
            "4.1.1",
            "4.1.0",
            "4.0.0",
            "3.2.1",
            "3.2.0",
            "3.1.0",
            "3.0.1",
            "3.0.0",
            "2.2.3",
            "2.2.2",
            "2.2.1",
            "2.2.0",
            "2.1.1",
            "2.1.0",
            "2.0.0",
            "1.0.1",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Hadoop Distributed File System (HDFS)",
                "external-doc-url": "https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html",
                "logo": "/docs/integration-logos/hadoop.png",
                "tags": ["apache"],
            },
            {
                "integration-name": "WebHDFS",
                "external-doc-url": "https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html",
                "how-to-guide": ["/docs/apache-airflow-providers-apache-hdfs/operators/webhdfs.rst"],
                "tags": ["apache"],
            },
        ],
        "sensors": [
            {
                "integration-name": "WebHDFS",
                "python-modules": ["airflow.providers.apache.hdfs.sensors.web_hdfs"],
            }
        ],
        "hooks": [
            {"integration-name": "WebHDFS", "python-modules": ["airflow.providers.apache.hdfs.hooks.webhdfs"]}
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook",
                "connection-type": "webhdfs",
            }
        ],
        "dependencies": [
            "apache-airflow>=2.9.0",
            'hdfs[avro,dataframe,kerberos]>=2.5.4;python_version<"3.12"',
            'hdfs[avro,dataframe,kerberos]>=2.7.3;python_version>="3.12"',
            "pandas>=2.1.2,<2.2",
        ],
    }
