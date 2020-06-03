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
#     Do you want to help? Please look at: https://github.com/apache/airflow/issues/8129


def delete_dag_run():
    """
    Delete a DAG Run
    """
    raise NotImplementedError("Not implemented yet.")


def get_dag_run():
    """
    Get a DAG Run.
    """
    raise NotImplementedError("Not implemented yet.")


def get_dag_runs():
    """
    Get all DAG Runs.
    """
    raise NotImplementedError("Not implemented yet.")


def get_dag_runs_batch():
    """
    Get list of DAG Runs
    """
    raise NotImplementedError("Not implemented yet.")


def patch_dag_run():
    """
    Update a DAG Run
    """
    raise NotImplementedError("Not implemented yet.")


def post_dag_run():
    """
    Trigger a DAG.
    """
    raise NotImplementedError("Not implemented yet.")
