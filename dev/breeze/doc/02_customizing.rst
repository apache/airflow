

 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


Customizing breeze environment
==============================

Breeze can be customized in a number of ways. You can read about those ways in this document.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**


Customizing Breeze startup
--------------------------

When you enter the Breeze environment, automatically an environment file is sourced from
``files/airflow-breeze-config/environment_variables.env``.

An example of the file is:

.. code-block:: bash

  # Logging settings
  AIRFLOW__LOGGING__REMOTE_LOGGING=true
  AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER="logs"

  # Remote logging Conn ID
  AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID="logs_default"


You can also add ``files/airflow-breeze-config/init.sh`` and the script will be sourced always
when you enter Breeze. For example you can add ``pip install`` commands if you want to install
custom dependencies - but there are no limits to add your own customizations.

You can override the name of the init script by setting ``INIT_SCRIPT_FILE`` environment variable before
running the breeze environment.

You can also customize your environment by setting ``BREEZE_INIT_COMMAND`` environment variable. This variable
will be evaluated at entering the environment.

The ``files`` folder from your local sources is automatically mounted to the container under
``/files`` path and you can put there any files you want to make available for the Breeze container.

You can also copy any .whl or ``sdist`` packages to dist and when you pass ``--use-distributions-from-dist`` flag
as ``wheel`` or ``sdist`` line parameter, breeze will automatically install the packages found there
when you enter Breeze.

You can also add your local tmux configuration in ``files/airflow-breeze-config/.tmux.conf`` and
these configurations will be available for your tmux environment.

There is a symlink between ``files/airflow-breeze-config/.tmux.conf`` and ``~/.tmux.conf`` in the container,
so you can change it at any place, and run

.. code-block:: bash

  tmux source ~/.tmux.conf

inside container, to enable modified tmux configurations.

Tmux tldr
~~~~~~~~~

In case you, like some Airflow core devs, are a tmux dummy, here are some tmux config entries
that you may find helpful.

.. code-block::

  # if you like vi mode instead of emacs
  set-window-option -g mode-keys vi

  # will not clear the selection immediately
  bind-key -T copy-mode-vi MouseDragEnd1Pane send-keys -X copy-selection-no-clear

  # make it so ctrl+shift+arrow moves the focused pane
  bind -T root C-S-Left select-pane -L
  bind -T root C-S-Right select-pane -R
  bind -T root C-S-Up select-pane -U
  bind -T root C-S-Down select-pane -D

  # quickly disable mouse + zoom pane (for easy copying)
  bind c run-shell "tmux setw mouse off" \; resize-pane -Z \; display-message "🚫 Mouse disabled & pane zoomed"

  # quickly re-enable mouse + unzoom pane (restore normal behavior)
  bind v run-shell "tmux setw mouse on" \; resize-pane -Z \; display-message "🖱️ Mouse enabled & pane unzoomed"

Some helpful commands:

  - ``ctrl-b + z``: zoom into selected pane
  - ``ctrl-b + [``: enter copy mode

To copy an entire pane:
  - select the pane
  - enter copy mode: ``ctrl-b + [``
  - go to start: ``g``
  - begin selection: ``space``
  - extend selection to end: ``G``
  - copy and clear selection: ``enter``

.. tip::

  You can add the custom bindings (like ``bind c`` and ``bind v``) directly to ``files/airflow-breeze-config/.tmux.conf``.
  This way they will be automatically loaded when you start a Breeze tmux session.

Additional tools in Breeze container
------------------------------------

To shrink the Docker image, not all tools are pre-installed in the Docker image. But we have made sure that there
is an easy process to install additional tools.

Additional tools are installed in ``/files/bin``. This path is added to ``$PATH``, so your shell will
automatically autocomplete files that are in that directory. You can also keep the binaries for your tools
in this directory if you need to.

**Installation scripts**

For the development convenience, we have also provided installation scripts for commonly used tools. They are
installed to ``/files/opt/``, so they are preserved after restarting the Breeze environment. Each script
is also available in ``$PATH``, so just type ``install_<TAB>`` to get a list of tools.

Currently available scripts:

* ``install_aws.sh`` - installs `the AWS CLI <https://aws.amazon.com/cli/>`__ including
* ``install_az.sh`` - installs `the Azure CLI <https://github.com/Azure/azure-cli>`__ including
* ``install_gcloud.sh`` - installs `the Google Cloud SDK <https://cloud.google.com/sdk>`__ including
  ``gcloud``, ``gsutil``.
* ``install_imgcat.sh`` - installs `imgcat - Inline Images Protocol <https://iterm2.com/documentation-images.html>`__
  for iTerm2 (Mac OS only)
* ``install_java.sh`` - installs `the OpenJDK 8u41 <https://openjdk.java.net/>`__
* ``install_kubectl.sh`` - installs `the Kubernetes command-line tool, kubectl <https://kubernetes.io/docs/reference/kubectl/kubectl/>`__
* ``install_snowsql.sh`` - installs `SnowSQL <https://docs.snowflake.com/en/user-guide/snowsql.html>`__
* ``install_terraform.sh`` - installs `Terraform <https://www.terraform.io/docs/index.html>`__

Launching Breeze integrations
-----------------------------

When Breeze starts, it can start additional integrations. Those are additional docker containers
that are started in the same docker-compose command. Those are required by some of the tests
as described in `</contributing-docs/testing/integration_tests.rst>`_.

By default Breeze starts only Airflow container without any integration enabled. If you selected
``postgres`` or ``mysql`` backend, the container for the selected backend is also started (but only the one
that is selected). You can start the additional integrations by passing ``--integration`` flag
with appropriate integration name when starting Breeze. You can specify several ``--integration`` flags
to start more than one integration at a time.
Finally, you can specify ``--integration all-testable`` to start all testable integrations and
``--integration all`` to enable all integrations.

Once integration is started, it will continue to run until the environment is stopped with
``breeze down`` command.

Note that running integrations uses significant resources - CPU and memory.

Setting default answers for user interaction
--------------------------------------------

Sometimes during the build, you are asked whether to perform an action, skip it, or quit. This happens
when rebuilding or removing an image and in few other cases - actions that take a lot of time
or could be potentially destructive. You can force answer to the questions by providing an
``--answer`` flag in the commands that support it.

For automation scripts, you can export the ``ANSWER`` variable (and set it to
``y``, ``n``, ``q``, ``yes``, ``no``, ``quit`` - in all case combinations).

.. code-block::

  export ANSWER="yes"

------

Next step: Follow the `Developer tasks <03_developer_tasks.rst>`_ guide to learn how to use Breeze for regular development tasks.
