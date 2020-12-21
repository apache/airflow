:mod:`airflow.utils.python_virtualenv`
======================================

.. py:module:: airflow.utils.python_virtualenv

.. autoapi-nested-parse::

   Utilities for creating a virtual environment



Module Contents
---------------

.. function:: _generate_virtualenv_cmd(tmp_dir: str, python_bin: str, system_site_packages: bool) -> List[str]

.. function:: _generate_pip_install_cmd(tmp_dir: str, requirements: List[str]) -> Optional[List[str]]

.. function:: prepare_virtualenv(venv_directory: str, python_bin: str, system_site_packages: bool, requirements: List[str]) -> str
   Creates a virtual environment and installs the additional python packages

   :param venv_directory: The path for directory where the environment will be created
   :type venv_directory: str
   :param python_bin: Path for python binary
   :type python_bin: str
   :param system_site_packages: Whether to include system_site_packages in your virtualenv.
       See virtualenv documentation for more information.
   :type system_site_packages: bool
   :param requirements: List of additional python packages
   :type requirements: List[str]
   :return: Path to a binary file with Python in a virtual environment.
   :rtype: str


.. function:: write_python_script(jinja_context: dict, filename: str)
   Renders the python script to a file to execute in the virtual environment.

   :param jinja_context: The jinja context variables to unpack and replace with its placeholders in the
       template file.
   :type jinja_context: dict
   :param filename: The name of the file to dump the rendered script to.
   :type filename: str


