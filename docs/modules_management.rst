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



Modules Management
==================

Airflow allows you to use your own Python modules in the DAG and in the
Airflow configuration. The following article will describe how you can
create your own module so that Airflow can load it correctly, as well as
diagnose problems when modules are not loaded properly.


Packages Loading in Python
--------------------------

The list of directories from which Python tries to load the module is given
by the variable ``sys.path``. Python really tries to
`intelligently determine the contents of <https://stackoverflow.com/a/38403654>`_
of this variable, including depending on the operating system and how Python
is installed and which Python version is used.

You can check the contents of this variable for the current Python environment
by running an interactive terminal as in the example below:

.. code-block:: pycon

    >>> import sys
    >>> from pprint import pprint
    >>> pprint(sys.path)
    ['',
     '/home/arch/.pyenv/versions/3.7.4/lib/python37.zip',
     '/home/arch/.pyenv/versions/3.7.4/lib/python3.7',
     '/home/arch/.pyenv/versions/3.7.4/lib/python3.7/lib-dynload',
     '/home/arch/venvs/airflow/lib/python3.7/site-packages']

``sys.path`` is initialized during program startup. The first precedence is
given to the current directory, i.e, ``path[0]`` is the directory containing
the current script that was used to invoke or an empty string in case it was
an interactive shell. Second precedence is given to the ``PYTHONPATH`` if provided,
followed by installation-dependent default paths which is managed by
`site <https://docs.python.org/3/library/site.html#module-site>`_ module.

``sys.path`` can also be modified during a Python session by simply using append
(for example, ``sys.path.append("/path/to/custom/package")``). Python will start
searching for packages in the newer paths once they're added. Airflow makes use
of this feature as described in the section :ref:`Additional modules in Airflow <additional-modules-in-airflow>`.

In the variable ``sys.path`` there is a directory ``site-packages`` which
contains the installed **external packages**, which means you can install
packages with ``pip`` or ``anaconda`` and you can use them in Airflow.
In the next section, you will learn how to create your own simple
installable package and how to specify additional directories to be added
to ``sys.path`` using the environment variable :envvar:`PYTHONPATH`.


Creating a package in Python
----------------------------

1. Before starting, install the following packages:

``setuptools``: setuptools is a package development process library designed
for creating and distributing Python packages.

``wheel``: The wheel package provides a bdist_wheel command for setuptools. It
creates .whl file which is directly installable through the ``pip install``
command. We can then upload the same file to pypi.org.

.. code-block:: bash

    pip install --upgrade pip setuptools wheel

2. Create the package directory - in our case, we will call it ``airflow_operators``.

.. code-block:: bash

    mkdir airflow_operators

3. Create the file ``__init__.py`` inside the package and add following code:

.. code-block:: python

    print("Hello from airflow_operators")

When we import this package, it should print the above message.

4. Create ``setup.py``:

.. code-block:: python

    import setuptools

    setuptools.setup(
        name='airflow_operators',
    )

5. Build the wheel:

.. code-block:: bash

    python setup.py bdist_wheel

This will create a few directories in the project and the overall structure will
look like following:

.. code-block:: bash

    .
    ├── airflow_operators
    │   ├── __init__.py
    ├── airflow_operators.egg-info
    │   ├── PKG-INFO
    │   ├── SOURCES.txt
    │   ├── dependency_links.txt
    │   └── top_level.txt
    ├── build
    │   └── bdist.macosx-10.15-x86_64
    ├── dist
    │   └── airflow_operators-0.0.0-py3-none-any.whl
    └── setup.py


6. Install the .whl file using pip:

.. code-block:: bash

    pip install dist/airflow_operators-0.0.0-py3-none-any.whl

7. The package is now ready to use!

.. code-block:: pycon

  >>> import airflow_operators
  Hello from airflow_operators
  >>>

The package can be removed using pip command:

.. code-block:: bash

    pip uninstall airflow_operators

For more details on how to create to create and publish python packages,
see `Packaging Python Projects <https://packaging.python.org/tutorials/packaging-projects/>`_.


Adding directories to the path
------------------------------

You can specify additional directories to be added to ``sys.path`` using the
environment variable :envvar:`PYTHONPATH`. Start the python shell by providing
the path to root of your project using the following command:

.. code-block:: bash

    PYTHONPATH=/home/arch/projects/airflow_operators python

The ``sys.path`` variable will look like below:

.. code-block:: pycon

    >>> import sys
    >>> from pprint import pprint
    >>> pprint(sys.path)
    ['',
     '/home/arch/projects/airflow_operators'
     '/home/arch/.pyenv/versions/3.7.4/lib/python37.zip',
     '/home/arch/.pyenv/versions/3.7.4/lib/python3.7',
     '/home/arch/.pyenv/versions/3.7.4/lib/python3.7/lib-dynload',
     '/home/arch/venvs/airflow/lib/python3.7/site-packages']

As we can see that our provided directory is now added to the path, let's
try to import the package now:

.. code-block:: pycon

    >>> import airflow_operators
    Hello from airflow_operators
    >>>

We can also use :envvar:`PYTHONPATH` variable with the airflow commands.
For example, if we run the following airflow command:

.. code-block:: bash

    PYTHONPATH=/home/arch/projects/airflow_operators airflow info

We'll see the ``Python PATH`` updated with our mentioned :envvar:`PYTHONPATH`
value as shown below:

.. code-block:: none

    Python PATH: [/home/arch/venv/bin:/home/arch/projects/airflow_operators:/usr/lib/python38.zip:/usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/home/arch/venv/lib/python3.8/site-packages:/home/arch/airflow/dags:/home/arch/airflow/config:/home/arch/airflow/plugins]


.. _additional-modules-in-airflow:

Additional modules in Airflow
-----------------------------

Airflow adds three additional directories to the ``sys.path``:

- DAGS folder: It is configured with option ``dags_folder`` in section ``[core]``.
- Config folder: It is configured by setting ``AIRFLOW_HOME`` variable (``{AIRFLOW_HOME}/config``) by default.
- Plugins Folder: It is configured with option ``plugins_folder`` in section ``[core]``.

You can also see the exact paths using the ``airflow info`` command,
and use them similar to directories specified with the environment variable
:envvar:`PYTHONPATH`. An example of the contents of the sys.path variable
specified by this command may be as follows:

Python PATH: [/home/rootcss/venvs/airflow/bin:/usr/lib/python38.zip:/usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/home/rootcss/venvs/airflow/lib/python3.8/site-packages:/home/rootcss/airflow/dags:/home/rootcss/airflow/config:/home/rootcss/airflow/plugins]

Below is the sample output of the ``airflow info`` command:

.. code-block:: none

    Apache Airflow [1.10.11]

    Platform: [Linux, x86_64] uname_result(system='Linux', node='Shekhar', release='5.4.0-42-generic', version='#46-Ubuntu SMP Fri Jul 10 00:24:02 UTC 2020', machine='x86_64', processor='x86_64')
    Locale: ('en_US', 'UTF-8')
    Python Version: [3.8.2 (default, Jul 16 2020, 14:00:26)  [GCC 9.3.0]]
    Python Location: [/home/rootcss/venvs/airflow/bin/python3]

    git: [git version 2.25.1]
    ssh: [OpenSSH_8.2p1 Ubuntu-4ubuntu0.1, OpenSSL 1.1.1f  31 Mar 2020]
    kubectl: [NOT AVAILABLE]
    gcloud: [NOT AVAILABLE]
    cloud_sql_proxy: [NOT AVAILABLE]
    mysql: [NOT AVAILABLE]
    sqlite3: [3.31.1 2020-01-27 19:55:54 3bfa9cc97da10598521b342961df8f5f68c7388fa117345eeb516eaa837balt1]
    psql: [psql (PostgreSQL) 12.2 (Ubuntu 12.2-4)]

    Airflow Home: [/home/rootcss/airflow]
    System PATH: [/home/rootcss/venvs/airflow/bin:/home/rootcss/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/home/rootcss/.rvm/bin:/usr/local/go/bin:/home/rootcss/.rvm/bin]
    Python PATH: [/home/rootcss/venvs/airflow/bin:/usr/lib/python38.zip:/usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/home/rootcss/venvs/airflow/lib/python3.8/site-packages:/home/rootcss/airflow/dags:/home/rootcss/airflow/config:/home/rootcss/airflow/plugins]
    airflow on PATH: [True]

    Executor: [SequentialExecutor]
    SQL Alchemy Conn: [sqlite:////home/rootcss/airflow/airflow.db]
    DAGS Folder: [/home/rootcss/airflow/dags]
    Plugins Folder: [/home/rootcss/airflow/plugins]
    Base Log Folder: [/home/rootcss/airflow/logs]
