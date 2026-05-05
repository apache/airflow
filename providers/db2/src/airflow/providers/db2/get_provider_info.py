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


def get_provider_info():
    """Return provider information for Db2."""
    return {
        "package-name": "apache-airflow-providers-db2",
        "name": "IBM Db2",
        "description": "`IBM Db2 <https://www.ibm.com/products/db2>`__\n",
        "integrations": [
            {
                "integration-name": "IBM Db2",
                "external-doc-url": "https://www.ibm.com/products/db2",
                "logo": "/docs/integration-logos/DB2.png",
                "tags": ["software"],
            }
        ],
        "hooks": [
            {
                "integration-name": "IBM Db2",
                "python-modules": ["airflow.providers.db2.hooks.db2"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.db2.hooks.db2.Db2Hook",
                "connection-type": "db2",
            }
        ],
    }
