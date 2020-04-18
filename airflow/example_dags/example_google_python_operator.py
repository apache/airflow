#
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

"""
Example Airflow DAG that uses Python operator together with Google services
"""
import os

from airflow import models
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils import dates

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project-id")

with models.DAG(
    "example_google_python_operator",
    default_args=dict(start_date=dates.days_ago(1)),
    schedule_interval=None,
    tags=['example'],
) as dag:
    # [START howto_operator_python_custom]
    def callable_custom_script():
        """Example function that use Google library."""
        from google.cloud import storage

        # If you don't specify credentials when constructing the client, the
        # client library will use the ADC strategy to determine the credentials
        storage_client = storage.Client()

        # Make an authenticated API request
        buckets = list(storage_client.list_buckets())
        print("\n".join(bucket.name for bucket in buckets))

    custom_script = PythonOperator(
        task_id='custom_script',
        python_callable=callable_custom_script,
        gcp_conn_id=GCP_PROJECT_ID
    )
    # [END howto_operator_python_custom]

    # [START howto_operator_python_gcloud]
    def callable_gcloud():
        """Execute function that use ``gcloud`` from python script."""
        from subprocess import run

        run(['gcloud', 'auth', 'print-access-token'], check=True)

    gcloud = PythonOperator(
        task_id='gcloud',
        python_callable=callable_gcloud,
        gcp_conn_id=GCP_PROJECT_ID
    )
    # [END howto_operator_python_gcloud]

    def callable_virtualenv():
        """
        Example function that will be performed in a virtual environment.

        Importing at the module level ensures that it will not attempt to import the
        library before it is installed.
        """
        from google.cloud import storage

        # If you don't specify credentials when constructing the client, the
        # client library will use the ADC strategy to determine the credentials
        storage_client = storage.Client()

        # Make an authenticated API request
        buckets = list(storage_client.list_buckets())
        print("\n".join(bucket.name for bucket in buckets))

    virtualenv = PythonVirtualenvOperator(
        task_id="virtualenv",
        python_callable=callable_virtualenv,
        requirements=[
            "google-auth==1.13.1",
            'google-cloud-storage==1.27.0',
        ],
        system_site_packages=False,
        gcp_conn_id=GCP_PROJECT_ID,
    )
