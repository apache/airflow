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

Often you want to use your own python code in your Airflow deployment,
for example common code, libraries, you might want to generate DAGs using
shared python code and have several DAG python files.

You can do it in one of those ways:

* add your modules to one of the folders that Airflow automatically adds to ``PYTHONPATH``
* add extra folders where you keep your code to ``PYTHONPATH``
* package your code into a Python package and install it together with Airflow.

The next chapter has a general description of how Python loads packages and modules, and dives
deeper into the specifics of each of the three possibilities above.

How package/modules loading in Python works
-------------------------------------------

The list of directories from which Python tries to load the module is given
by the variable ``sys.path``. Python really tries to
`intelligently determine the contents <https://stackoverflow.com/a/38403654>`_
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
of this feature as described in the section
:ref:`Adding directories to the PYTHONPATH <adding_directories_to_pythonpath>`.

In the variable ``sys.path`` there is a directory ``site-packages`` which
contains the installed **external packages**, which means you can install
packages with ``pip`` or ``anaconda`` and you can use them in Airflow.
In the next section, you will learn how to create your own simple
installable package and how to specify additional directories to be added
to ``sys.path`` using the environment variable :envvar:`PYTHONPATH`.

Also make sure to :ref:`Add init file to your folders <add_init_py_to_your_folders>`.

Typical structure of packages
-----------------------------

This is an example structure that you might have in your ``dags`` folder:

.. code-block:: none

   <DIRECTORY ON PYTHONPATH>
   | .airflowignore  -- only needed in ``dags`` folder, see below
   | -- my_company
                 | __init__.py
                 | common_package
                 |              |  __init__.py
                 |              | common_module.py
                 |              | subpackage
                 |                         | __init__.py
                 |                         | subpackaged_util_module.py
                 |
                 | my_custom_dags
                                 | __init__.py
                                 | my_dag1.py
                                 | my_dag2.py
                                 | base_dag.py

In the case above, these are the ways you could import the python files:

.. code-block:: python

   from my_company.common_package.common_module import SomeClass
   from my_company.common_package.subpackage.subpackaged_util_module import AnotherClass
   from my_company.my_custom_dags.base_dag import BaseDag

You can see the ``.airflowignore`` file at the root of your folder. This is a file that you can put in your
``dags`` folder to tell Airflow which files from the folder should be ignored when the Airflow
scheduler looks for DAGs. It should contain either regular expressions (the default) or glob expressions
for the paths that should be ignored. You do not need to have that file in any other folder in
``PYTHONPATH`` (and also you can only keep shared code in the other folders, not the actual DAGs).

In the example above the dags are only in ``my_custom_dags`` folder, the ``common_package`` should not be
scanned by scheduler when searching for DAGS, so we should ignore ``common_package`` folder. You also
want to ignore the ``base_dag.py`` if you keep a base DAG there that ``my_dag1.py`` and ``my_dag2.py`` derives
from. Your ``.airflowignore`` should look then like this:

.. code-block:: none

   my_company/common_package/.*
   my_company/my_custom_dags/base_dag\.py

If ``DAG_IGNORE_FILE_SYNTAX`` is set to ``glob``, the equivalent ``.airflowignore`` file would be:

.. code-block:: none

   my_company/common_package/
   my_company/my_custom_dags/base_dag.py

Built-in ``PYTHONPATH`` entries in Airflow
------------------------------------------

Airflow, when running dynamically adds three directories to the ``sys.path``:

- The ``dags`` folder: It is configured with option ``dags_folder`` in section ``[core]``.
- The ``config`` folder: It is configured by setting ``AIRFLOW_HOME`` variable (``{AIRFLOW_HOME}/config``) by default.
- The ``plugins`` Folder: It is configured with option ``plugins_folder`` in section ``[core]``.

.. note::
   The DAGS folder in Airflow 2 should not be shared with the webserver. While you can do it, unlike in Airflow 1.10,
   Airflow has no expectations that the DAGS folder is present in the webserver. In fact it's a bit of
   security risk to share the ``dags`` folder with the webserver, because it means that people who write DAGS
   can write code that the webserver will be able to execute (ideally the webserver should
   never run code which can be modified by users who write DAGs). Therefore if you need to share some code
   with the webserver, it is highly recommended that you share it via ``config`` or ``plugins`` folder or
   via installed Airflow packages (see below). Those folders are usually managed and accessible by different
   users (Admins/DevOps) than DAG folders (those are usually data-scientists), so they are considered
   as safe because they are part of configuration of the Airflow installation and controlled by the
   people managing the installation.

Best practices for module loading
---------------------------------

There are a few gotchas you should be careful about when you import your code.

Use unique top package name
...........................

It is recommended that you always put your dags/common files in a subpackage which is unique to your
deployment (``my_company`` in the example below). It is far too easy to use generic names for the
folders that will clash with other packages already present in the system. For example if you
create ``airflow/operators`` subfolder it will not be accessible because Airflow already has a package
named ``airflow.operators`` and it will look there when importing ``from airflow.operators``.

Don't use relative imports
..........................

Never use relative imports (starting with ``.``) that were added in Python 3.

This is tempting to do something like that it in ``my_dag1.py``:

.. code-block:: python

   from .base_dag import BaseDag  # NEVER DO THAT!!!!

You should import such shared dag using full path (starting from the directory which is added to
``PYTHONPATH``):

.. code-block:: python

   from my_company.my_custom_dags.base_dag import BaseDag  # This is cool

The relative imports are counter-intuitive, and depending on how you start your python code, they can behave
differently. In Airflow the same DAG file might be parsed in different contexts (by schedulers, by workers
or during tests) and in those cases, relative imports might behave differently. Always use full
python package paths when you import anything in Airflow DAGs, this will save you a lot of troubles.
You can read more about relative import caveats in
`this Stack Overflow thread <https://stackoverflow.com/q/16981921/516701>`_.

.. _add_init_py_to_your_folders:

Add ``__init__.py`` in package folders
......................................

When you create folders you should add ``__init__.py`` file as empty files in your folders. While in Python 3
there is a concept of implicit namespaces where you do not have to add those files to folder, Airflow
expects that the files are added to all packages you added.

Inspecting your ``PYTHONPATH`` loading configuration
----------------------------------------------------

You can also see the exact paths using the ``airflow info`` command,
and use them similar to directories specified with the environment variable
:envvar:`PYTHONPATH`. An example of the contents of the sys.path variable
specified by this command may be as follows:

.. code-block:: none

    Python PATH: [/home/rootcss/venvs/airflow/bin:/usr/lib/python38.zip:/usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/home/rootcss/venvs/airflow/lib/python3.8/site-packages:/home/rootcss/airflow/dags:/home/rootcss/airflow/config:/home/rootcss/airflow/plugins]

Below is the sample output of the ``airflow info`` command:

.. seealso:: :ref:`plugins:loading`

.. code-block:: none

    Apache Airflow: 2.0.0b3

    System info
    OS              | Linux
    architecture    | x86_64
    uname           | uname_result(system='Linux', node='85cd7ab7018e', release='4.19.76-linuxkit', version='#1 SMP Tue May 26 11:42:35 UTC 2020', machine='x86_64', processor='')
    locale          | ('en_US', 'UTF-8')
    python_version  | 3.8.6 (default, Nov 25 2020, 02:47:44)  [GCC 8.3.0]
    python_location | /usr/local/bin/python

    Tools info
    git             | git version 2.20.1
    ssh             | OpenSSH_7.9p1 Debian-10+deb10u2, OpenSSL 1.1.1d  10 Sep 2019
    kubectl         | NOT AVAILABLE
    gcloud          | NOT AVAILABLE
    cloud_sql_proxy | NOT AVAILABLE
    mysql           | mysql  Ver 8.0.22 for Linux on x86_64 (MySQL Community Server - GPL)
    sqlite3         | 3.27.2 2019-02-25 16:06:06 bd49a8271d650fa89e446b42e513b595a717b9212c91dd384aab871fc1d0alt1
    psql            | psql (PostgreSQL) 11.9 (Debian 11.9-0+deb10u1)

    Paths info
    airflow_home    | /root/airflow
    system_path     | /usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
    python_path     | /usr/local/bin:/opt/airflow:/files/plugins:/usr/local/lib/python38.zip:/usr/local/lib/python3.8:/usr/
                    | local/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/site-packages:/files/dags:/root/airflow/conf
                    | ig:/root/airflow/plugins
    airflow_on_path | True

    Config info
    executor             | LocalExecutor
    task_logging_handler | airflow.utils.log.file_task_handler.FileTaskHandler
    sql_alchemy_conn     | postgresql+psycopg2://postgres:airflow@postgres/airflow
    dags_folder          | /files/dags
    plugins_folder       | /root/airflow/plugins
    base_log_folder      | /root/airflow/logs

    Providers info
    apache-airflow-providers-amazon           | 1.0.0b2
    apache-airflow-providers-apache-cassandra | 1.0.0b2
    apache-airflow-providers-apache-druid     | 1.0.0b2
    apache-airflow-providers-apache-hdfs      | 1.0.0b2
    apache-airflow-providers-apache-hive      | 1.0.0b2

.. _adding_directories_to_pythonpath:

Adding directories to the ``PYTHONPATH``
----------------------------------------

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

Creating a package in Python
----------------------------

This is most organized way of adding your custom code. Thanks to using packages,
you might organize your versioning approach, control which versions of the shared code are installed
and deploy the code to all your instances and containers in controlled way - all by system admins/DevOps
rather than by the DAG writers. It is usually suitable when you have a separate team that manages this
shared code, but if you know your python ways you can also distribute your code this way in smaller
deployments. You can also install your :doc:`/plugins` and :doc:`apache-airflow-providers:index` as python
packages, so learning how to build your package is handy.

Here is how to create your package:

1. Before starting, install the following packages:

``setuptools``: setuptools is a package development process library designed
for creating and distributing Python packages.

``wheel``: The wheel package provides a bdist_wheel command for setuptools. It
creates .whl file which is directly installable through the ``pip install``
command. We can then upload the same file to `PyPI <pypi.org>`_.

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
        name="airflow_operators",
        packages=setuptools.find_packages(),
    )

5. Build the wheel:

.. code-block:: bash

    python setup.py bdist_wheel

This will create a few directories in the project and the overall structure will
look like following:

.. code-block:: bash

    .
    ├── airflow_operators
    │   ├── __init__.py
    ├── airflow_operators.egg-info
    │   ├── PKG-INFO
    │   ├── SOURCES.txt
    │   ├── dependency_links.txt
    │   └── top_level.txt
    ├── build
    │   └── bdist.macosx-10.15-x86_64
    ├── dist
    │   └── airflow_operators-0.0.0-py3-none-any.whl
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
