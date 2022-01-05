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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Airflow Breeze CI environment](#airflow-breeze-ci-environment)
  - [Breeze on Linux](#breeze-on-linux)
  - [Breeze on Windows](#breeze-on-windows)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<div align="center">
    <img src="../../../images/AirflowBreeze_logo.png"
        alt="Airflow Breeze - Development and Test Environment for Apache Airflow">
</div>

# Airflow Breeze CI environment

Airflow Breeze is an easy-to-use development and test environment using Docker Compose. The environment is available for local use and is also used in Airflow's CI tests.

We called it Airflow Breeze as It's a Breeze to contribute to Airflow.

The advantages and disadvantages of using the Breeze environment vs. other ways of testing Airflow are described in CONTRIBUTING.rst.

All the output from the last ./breeze command is automatically logged to the logs/breeze.out file.

Watch the video below about Airflow Breeze. It explains the motivation for Breeze and screencasts all its uses.

[![Watch the video](https://i.imgur.com/vKb2F1B.png)](https://www.youtube.com/watch?v=4MCTXq-oF68)


## Breeze on Linux

Installation is as easy as checking out Airflow repository and running Breeze command. You enter the Breeze test environment by running the ./breeze script. You can run it with the help command to see the list of available options. See Breeze Command-Line Interface Reference for details.

```bash
./breeze
```

The First time you run Breeze, it pulls and builds a local version of Docker images. It pulls the latest Airflow CI images from the GitHub Container Registry and uses them to build your local Docker images. Note that the first run (per python) might take up to 10 minutes on a fast connection to start. Subsequent runs should be much faster.

Once you enter the environment, you are dropped into bash shell of the Airflow container and you can run tests immediately.

To use the full potential of Breeze you should set up autocomplete and you can add the checked-out Airflow repository to your PATH to run Breeze without the ./ and from any directory.

The Breeze command comes with a built-in bash/zsh autocomplete setup command. After installing, when you start typing the command, you can use <TAB> to show all the available switches and get auto-completion on typical values of parameters that you can use.

You should set up the autocomplete option automatically by running:

```bash
./breeze setup-autocomplete
```

You get the auto-completion working when you re-enter the shell.

## Breeze on Windows

In Windows environment, you will need to use pipx to install Breeze.

Install pipx

```bash
pip install --user pipx
```

Install Breeze, this command will generate Breeze2.exe

```bash
pipx install -e dev/breeze
```

Breeze, is not globally accessible until your PATH is updated. Add <USER FOLDER>\.local\bin as a variable environments. This can be done automatically by the following command.

```bash
pipx ensurepath
```

Finally, use Breeze, --help will show you Breeze options.

```bash
Breeze2 --help
```

To update the installation use: --force

```bash
pipx install --force -e dev/breeze
```
