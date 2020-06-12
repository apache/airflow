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

# TODO(mik-laj): We have to implement it.
#     Do you want to help? Please look at:
#     * https://github.com/apache/airflow/issues/8128
#     * https://github.com/apache/airflow/issues/8138


def get_dag():
    """
    Get basic information about a DAG.
    """
    raise NotImplementedError("Not implemented yet.")


def get_dag_details():
    """
    Get details of DAG.
    """
    raise NotImplementedError("Not implemented yet.")


def get_dags():
    """
    Get all DAGs.
    """
    raise NotImplementedError("Not implemented yet.")


def patch_dag():
    """
    Update the specific DAG
    """
    raise NotImplementedError("Not implemented yet.")
