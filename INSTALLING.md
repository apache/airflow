<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

## Local Development Setup

This section outlines a recommended approach for setting up a local development environment for Apache Airflow on macOS and Linux, primarily using PyEnv for Python version management.

> ⚠️ Avoid using either system-installed Python or Python from Homebrew, as these versions are often labeled `--externally-managed` resulting in restricted dependency installation.

You can use other ways to install Python and airflow. Airflow development setup requires `uv` and if you want to setup environment for development, `uv` is the only supported local development environment setup, because we are using `uv workspace` extensively. See [local virtualenv setup in contributing docs](https://github.com/apache/airflow/blob/main/contributing-docs/07_local_virtualenv.rst) for details.

If you are just installing airflow to run it locally, You can use other ways to set up your Python and virtualenv: `uv` is one of the options (refer to `uv` documentation), but you can also use more traditional tools - for example `pyenv`. Note that it is recommended to install airflow with constraints - at least initially - because this way you can reproducibly install airflow. See [Installation from PyPI](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html) for more details.

### ✅ Setup using pyenv:

1. **Install pyenv (macOS and Linux)**:

```bash
brew install pyenv
```

(Note: Homebrew is the recommended method on macOS. For Linux, you can typically install pyenv using the `pyenv-installer` script as detailed in the official documentation: [https://github.com/pyenv/pyenv#installation](https://github.com/pyenv/pyenv#installation).)

2. **Install Python**:

```bash
pyenv install 3.11.9
pyenv global 3.11.9
```

3. **Check Python version**:

```bash
python --version
```

4. **Create and Activate a Virtual Environment**: Since Apache Airflow requires multiple dependencies, it's a good practice to isolate these dependencies in a virtual environment.

- Create a virtual environment:

```bash
python -m venv airflow_venv
```

- Activate the virtual environment:

```bash
source airflow_venv/bin/activate
```

5. **Install Apache Airflow**: Apache Airflow is available on PyPI. To install it, you can use the following command in your terminal:

```bash
pip install apache-airflow==3.0.0 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt"
```

Note that installing with constraints - at least initially - is recommended for reproducible installation. It might sometimes happen that 3rd-party distributions are released and their latest versions break airflow. Using constraints makes the installation reproducible with versions of dependencies that were "frozen" at the time of releasing airflow. Note you have to specify both - airflow version and Python version you are using.

You can also specify additional extras - when you want to install airflow with additional providers:

```bash
pip install apache-airflow[amazon,google]==3.0.0 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt"
```

6. **Set the AIRFLOW_HOME Environment Variable**: Apache Airflow requires a directory to store configuration files, logs, and other data. Set the AIRFLOW_HOME variable to specify this directory.

- Set the Airflow home directory:

```bash
export AIRFLOW_HOME=~/airflow
```

7. **Run Airflow in standalone mode**: Apache Airflow runs several components, like the scheduler, web server, and API server, to manage workflows and show the UI.

- To run Airflow in standalone mode (which will automatically start the required components):

```bash
airflow standalone
```

8. **Access the Airflow Web UI**: Once the components are up and running, you can access the Airflow UI through your browser:

- Open your browser and go to:

```text
http://localhost:8080
```
