.. image:: img/pin_large.png
    :width: 70

Airflow Documentation
================================

Airflow is a platform to programmatically author, schedule and monitor
workflows.

Use airflow to author workflows as directed acyclic graphs (DAGs) of tasks.
The airflow scheduler executes your tasks on an array of workers while
following the specified dependencies. Rich command line utilities make
performing complex surgeries on DAGs a snap. The rich user interface
makes it easy to visualize pipelines running in production,
monitor progress, and troubleshoot issues when needed.

When workflows are defined as code, they become more maintainable,
versionable, testable, and collaborative.

------------

.. image:: img/airflow.gif

------------

Principles
----------

- **Dynamic**:  Airflow pipelines are configuration as code (Python), allowing for dynamic pipeline generation. This allows for writing code that instantiates pipelines dynamically.
- **Extensible**:  Easily define your own operators, executors and extend the library so that it fits the level of abstraction that suits your environment.
- **Elegant**:  Airflow pipelines are lean and explicit. Parameterizing your scripts is built into the core of Airflow using the powerful **Jinja** templating engine.
- **Scalable**:  Airflow has a modular architecture and uses a message queue to orchestrate an arbitrary number of workers. Airflow is ready to scale to infinity.


Beyond the Horizon
------------------

Airflow **is not** a data streaming solution. Tasks do not move data from
one to the other (though tasks can exchange metadata!). Airflow is not
in the `Spark Streaming <http://spark.apache.org/streaming/>`_
or `Storm <https://storm.apache.org/>`_ space, it is more comparable to
`Oozie <http://oozie.apache.org/>`_ or
`Azkaban <http://data.linkedin.com/opensource/azkaban>`_.

Workflows are expected to be mostly static or slowly changing. You can think
of the structure of the tasks in your workflow as slightly more dynamic
than a database structure would be. Airflow workflows are expected to look 
similar from a run to the next, this allows for clarity around 
unit of work and continuity.

Content
-------
.. toctree::
    :maxdepth: 4

    start
    tutorial
    ui
    installation
    concepts
    profiling
    cli
    scheduler
    plugins
    security
    faq 
    code
