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



Loading Packages in Python and Airflow
======================================

How does Python load packages?
------------------------------
Python's `sys <https://docs.python.org/3/library/sys.html>`_ package provides key variables and functions that are used
or maintained by the interpreter. One such variable is ``sys.path``. It's a list of directories which is searched by
Python during imports. for example,

.. code-block:: pycon

    >>> import sys
    >>> from pprint import pprint
    >>> pprint(sys.path)
    ['',
     '/home/arch/.pyenv/versions/3.7.4/lib/python37.zip',
     '/home/arch/.pyenv/versions/3.7.4/lib/python3.7',
     '/home/arch/.pyenv/versions/3.7.4/lib/python3.7/lib-dynload',
     '/home/arch/venvs/airflow/lib/python3.7/site-packages']

``sys.path`` is initialized during program startup. The first precedence is given to the current directory,
i.e, ``path[0]`` is the directory containing the current script that was used to invoke or an empty string in case
it was an interactive shell. Second precedence is given to the ``PYTHONPATH``, followed by installation-dependent
default paths which is managed by `site <https://docs.python.org/3/library/site.html#module-site>`_ module.

``sys.path`` can also be modified during a Python session, by simply using append (for example, ``sys.path.append("/path/to/custom/package")``). Python will start searching in the newer paths once they're added. Airflow makes use
of this feature as described in the next section.


How does Airflow modify this behavior?
--------------------------------------
Airflow adds three additional directories to ``sys.path``:

- ``conf.get('core', 'dags_folder')``
- ``conf.get('core', 'airflow_home')``
- ``conf.get('core', 'plugins_folder')``


When and how you can affect the module loading mechanism?
---------------------------------------------------------


How to create python package with operators/plugins?
----------------------------------------------------


How to use PYTHONPATH?
----------------------


How do you check the contents of the sys.path variable?
-------------------------------------------------------


How to set configuration option with import e.g. hostname_callback, api_client?
-------------------------------------------------------------------------------
