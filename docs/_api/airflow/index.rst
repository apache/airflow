:mod:`airflow`
==============

.. py:module:: airflow

.. autoapi-nested-parse::

   Authentication is implemented using flask_login and different environments can
   implement their own login mechanisms by providing an `airflow_login` module
   in their PYTHONPATH. airflow_login should be based off the
   `airflow.www.login`

   isort:skip_file



Subpackages
-----------
.. toctree::
   :titlesonly:
   :maxdepth: 3

   api/index.rst
   api_connexion/index.rst
   cli/index.rst
   config_templates/index.rst
   contrib/index.rst
   executors/index.rst
   hooks/index.rst
   jobs/index.rst
   kubernetes/index.rst
   lineage/index.rst
   macros/index.rst
   models/index.rst
   mypy/index.rst
   operators/index.rst
   providers/index.rst
   secrets/index.rst
   security/index.rst
   sensors/index.rst
   serialization/index.rst
   smart_sensor_dags/index.rst
   task/index.rst
   ti_deps/index.rst
   utils/index.rst
   www/index.rst


Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   __main__/index.rst
   configuration/index.rst
   decorators/index.rst
   exceptions/index.rst
   logging_config/index.rst
   plugins_manager/index.rst
   providers_manager/index.rst
   sentry/index.rst
   settings/index.rst
   stats/index.rst
   typing_compat/index.rst
   version/index.rst


Package Contents
----------------

.. data:: __version__
   

   

.. data:: login
   :annotation: :Optional[Callable]

   

