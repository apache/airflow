# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is an example Dockerfile. It is not intended for PRODUCTION use
# [START Dockerfile]
# You can switch the Airflow and Python version numbers here (check which Python versions are suppored in the :doc:`Prerequisites <prerequisites>`)
FROM apache/airflow:2.4.2-python3.8
# ExternalPythonOperator :
# Compulsory to switch parameter to create your own virtual environment
# in case of using Airlfow's base Image because it has `PIP_USER` enabled by default as explained in
# https://airflow.apache.org/docs/docker-stack/build.html#important-notes-for-the-base-images
ENV PIP_USER=false
# python virtual environment setup in container :
RUN python3 -m venv /opt/airflow/venv1
# Installing dependencies :
COPY airflow/requirements.txt .
# copy over folder that contains our own local pip package :
COPY airflow/my_own_pip_package_folder /opt/airflow/my_own_pip_package_folder
# copy over our file :
COPY my_file.py /opt/airflow/my_file.py
# installing packages in our own docker virtual environment :
RUN /opt/airflow/venv1/bin/pip install -r requirements.txt
RUN /opt/airflow/venv1/bin/pip install /opt/airflow/my_own_pip_package_folder
RUN /opt/airflow/venv1/bin/pip install 'apache-airflow==2.4.2' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.4.1/constraints-3.8.txt"
# [END Dockerfile]
