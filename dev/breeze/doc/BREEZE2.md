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

Here we are, introducing the post about BREEZE (Updated BREEZE version). It's going to be great!
But first: Created a TOC for easy reference about BREEZE commands

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Updated BREEZE](#updated-breeze)
- [BREEZE setting up autocomplete](#breeze-setting-up-autocomplete)
- [Breeze on Linux](#breeze-on-linux)
- [Breeze2 on Windows](#breeze2-on-windows)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<div align="center">
    <img src="../../../images/AirflowBreeze_logo.png"
        alt="Airflow Breeze - Development and Test Environment for Apache Airflow">
</div>

# Updated BREEZE

Here we'll discuss about BREEZE...

# BREEZE setting up autocomplete

The full potential of Breeze can be put to use by enabling autocompletion. Breeze setup-autocomplete helps you to enable autocompletion for the list of commands that BREEZE supports with tab click. The command that helps to setup autocompletion is:

`Breeze2 setup-autocomplete`

After setting up the autocomplete command, you can use <TAB> to see list of available options and get autocompletion. To enable this feature, under the hood we use click-completion that helps setup the autocompletion for bash, zsh, fish and powershell. It auto detects the shell, generate the respective code for the shell type and links the shell type and generated activation command for autocompletion.

Using this command, it will display confirmation message asking if the command has to add the autocompletion activation script to shell. If yes, then the command will take care of associating the autocompletion activation script to shell. If no, these steps have to done manually.

**For Bash users**
Activation script is generated in the path `AIRFLOW_SOURCE/.build/autocomplete/Breeze2-complete.bash`. In the ~/.bash_completion file, append the below command and save it

```

source AIRFLOW_SOURCE/.build/autocomplete/Breeze2-complete.bash

```

After editing ~/.bash_completion file, `source ~/.bash_completion`

**For Zsh users**
Activation script is generated in the path `AIRFLOW_SOURCE/.build/autocomplete/Breeze2-complete.zsh`. In the ~/.zshrc file, append the below command and save it

```

source AIRFLOW_SOURCE/.build/autocomplete/Breeze2-complete.zsh

```

After editing ~/.zshrc, `source ~/.zshrc`

**For Fish users**
Activation script is generated in the path `AIRFLOW_SOURCE/.build/autocomplete/Breeze2-complete.fish`. Copy the generated script to `~/.config/fish/completions/Breeze.fish`

```

cp AIRFLOW_SOURCE/.build/autocomplete/Breeze2-complete.fish ~/.config/fish/completions/Breeze.fish

```

**For Powershell users**

# Breeze on Linux

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

# Breeze2 on Windows

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
