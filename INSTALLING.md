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

# Step-by-Step Guide: Setting Up Apache Airflow for Local Development

This guide will help you set up Apache Airflow on your computer for development purposes. It is written for users of **macOS** and **Linux**. If you are new to Python, virtual environments, or Airflow, don’t worry—each step is explained in detail.

---

## 1. **Install Python Version Manager (pyenv)**

**Why?**  
Different projects may require different Python versions. `pyenv` lets you easily switch between them.

**For macOS:**

- Open the Terminal app.
- Install Homebrew (if you don’t have it):  
  ```bash
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  ```
- Install pyenv:  
  ```bash
  brew install pyenv
  ```

**For Linux:**

- Follow the instructions at the [pyenv GitHub page](https://github.com/pyenv/pyenv#installation) to install pyenv using the `pyenv-installer` script.

---

## 2. **Install Python 3.11.9 Using pyenv**

**Why?**  
Airflow works best with specific Python versions. We’ll use 3.11.9.

- In your terminal, run:
  ```bash
  pyenv install 3.11.9
  pyenv global 3.11.9
  ```

- Check that the correct Python version is active:
  ```bash
  python --version
  ```
  You should see:  
  `Python 3.11.9`

---

## 3. **Create a Virtual Environment**

**Why?**  
A virtual environment keeps your project’s dependencies separate from other projects and your system.

- Create a new virtual environment named `airflow_venv`:
  ```bash
  python -m venv airflow_venv
  ```

- Activate the virtual environment:
  - On macOS/Linux:
    ```bash
    source airflow_venv/bin/activate
    ```
  - You should see `(airflow_venv)` at the start of your terminal prompt.

---

## 4. **Install Apache Airflow**

**Why?**  
Airflow is the main tool we want to use. We’ll install it with “constraints” to ensure all dependencies are compatible.

- Install Airflow (replace `3.0.0` with your desired version if needed):
  ```bash
  pip install apache-airflow==3.0.0 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt"
  ```

- **Optional:** If you need extra features (like Amazon or Google integrations), add them in square brackets:
  ```bash
  pip install apache-airflow[amazon,google]==3.0.0 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt"
  ```

---

## 5. **Set the AIRFLOW_HOME Environment Variable**

**Why?**  
Airflow needs a folder to store its files (configurations, logs, etc.).

- Set the home directory for Airflow:
  ```bash
  export AIRFLOW_HOME=~/airflow
  ```
  This command tells Airflow to use a folder named `airflow` in your home directory.

---

## 6. **Start Airflow in Standalone Mode**

**Why?**  
This will start all the necessary Airflow components (web server, scheduler, etc.) for you.

- Run:
  ```bash
  airflow standalone
  ```

- The first time you run this, Airflow will set up its database and create a default user. The terminal will show you a username and password for the web interface.

---

## 7. **Access the Airflow Web Interface**

- Open your web browser and go to:
  ```
  http://localhost:8080
  ```
- Log in using the username and password shown in your terminal.

---

## **Troubleshooting Tips**

- If you see errors about missing commands, make sure you have activated your virtual environment.
- If you get permission errors, try running the terminal as an administrator or check your folder permissions.
- If you need to stop Airflow, press `Ctrl+C` in the terminal window where it’s running.

---

## **Summary**

1. Install `pyenv` to manage Python versions.
2. Use `pyenv` to install Python 3.11.9.
3. Create and activate a virtual environment.
4. Install Airflow with pip and constraints.
5. Set the `AIRFLOW_HOME` variable.
6. Start Airflow in standalone mode.
7. Open the Airflow web UI in your browser.

---

If you follow these steps, you’ll have a working local Airflow development environment. If you get stuck, copy any error messages and ask for help!
