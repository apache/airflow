
Tutorial
================

This tutorial walks you through some of the fundamental Airflow concepts, 
objects and their usage while writing your first pipeline.

Example Pipeline definition
---------------------------

Here is an example of a basic pipeline definition. Do not worry if this looks 
complicated, a line by line explanation follows below.

.. code:: python

    """
    Code that goes along with the Airflow located at:
    http://airflow.readthedocs.org/en/latest/tutorial.html
    """
    from airflow import DAG
    from airflow.operators import BashOperator
    from datetime import datetime


    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2015, 1, 1),
        'email': ['airflow@airflow.com'],
        'email_on_failure': False,
        'email_on_retry': False,
    }

    dag = DAG('tutorial', default_args=default_args)

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag)

    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
        dag=dag)

    templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """

    t3 = BashOperator(
        task_id='templated',
        bash_command=templated_command,
        params={'my_param': 'Paramater I passed in'},
        dag=dag)

    t2.set_upstream(t1)
    t3.set_upstream(t1)


Importing Modules
-----------------

An Airflow pipeline is just a common Python script that happens to define
an Airflow DAG object. Let's start by importing the libraries we will need.

.. code:: python

    # The DAG object, we'll need this to instantiate a DAG
    from airflow import DAG

    # Operators, we need this to operate!
    from airflow.operators import BashOperator, MySqlOperator

Default Arguments
-----------------
We're about to create a DAG and some tasks, and we have the choice to 
explicitly pass a set of arguments to each task's constructor 
(which would become redundant), or (better!) we can define a dictionary 
of default parameters that we can use when creating tasks.

.. code:: python

    from datetime import datetime

    args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2015, 1, 1),
        'email': ['airflow@airflow.com',],
        'email_on_failure': True,
        'email_on_retry': True,
    }

For more information about the BaseOperator's parameters and what they do,
refer to the :py:class:``airflow.models.BaseOperator`` documentation.

Also, note that you could easily define different sets of arguments that
would serve different purposes. An example of that would be to have 
different settings between a production and development environment.


Instantiate a DAG
-----------------

We'll need a DAG object to nest our tasks into. Here we pass a string 
that defines the dag_id, which serves as a unique identifier for your DAG.
We also pass the default argument dictionary that we just define.

.. code:: python

    dag = DAG('tutorial', default_args=default_args)

Tasks
-----
Tasks are generated when instantiating objects from operators. The first
argument ``task_id`` acts as a unique identifier for the task.

.. code:: python

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag)

    t2 = BashOperator(
        task_id='sleep',
        email_on_failure=False,
        bash_command='sleep 5',
        dag=dag)

Notice how we pass a mix of operator specific arguments (``bash_command``) and
an argument common to all operators (``email_on_failure``) inherited 
from BaseOperator to the operators constructor. This is simpler than 
passing every argument for every constructor call. Also, notice that in 
the second call we override ``email_on_failure`` parameter with ``False``.

The precedence rules for operator is:

* Use the argument explicitly passed to the constructor
* Look in the default_args dictonary, use the value from there if it exists
* Use the operator's default, if any
* If none of these are defined, Airflow raises an exception


Templating with Jinja
---------------------
Airflow leverages the power of 
`Jinja Templating <http://jinja.pocoo.org/docs/dev/>`_  and provides
the pipeline author
with a set of builtin parameters and macros. Airflow also provides
hooks for the pipeline author to define their own parameters, macros and
templates.

This tutorial barely scratches the surfaces of what you can do 
with templating in Airflow, but the goal of this section is to let you know 
this feature exists, get you familiar with double
curly brackets, and point to the most common template variable: ``{{ ds }}``.

.. code:: python

    templated_command = """
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
            echo "{{ params.my_param }}"
        {% endfor %}
    """

    t3 = BashOperator(
        task_id='templated',
        bash_command=templated_command,
        params={'my_param': 'Paramater I passed in'},
        dag=dag)

Notice that the ``templated_command`` contains code logic in ``{% %}`` blocks,
references parameters like ``{{ ds }}``, calls a function as in
``{{ macros.ds_add(ds, 7)}}``, and references a user defined parameter
in ``{{ params.my_param }}``.

The ``params`` hook in BaseOperator allows you to pass a dictionary of 
parameters and/or objects to your templates. Please take the time
to understand how the parameter ``my_param`` makes it through to the template.

Note that templated fields can point to files if you prefer. 
It may be desirable for many reasons, like keeping your scripts logic
outside of your pipeline code, getting proper code highlighting in files, 
and just generally allowing you to organize your pipeline's logic as you
please. 

In the above example, we could have 
had a file ``templated_command.sh``, and referenced it in the ``bash_command``
parameter, as in
``bash_command='templated_command.sh'`` where the file location is relative
to the pipeline's (``tutorial.py``) location. Note that it is also possible 
to define your ``template_searchpath`` pointing to any folder 
locations in the DAG constructor call.

Setting up Dependencies
-----------------------
We have two simple tasks that do not depend on each other, here's a few ways
you can define dependencies between them:

.. code:: python

    t2.set_upstream(t1)

    # This means that t2 will depend on t1 
    # running successfully to run
    # It is equivalent to
    # t1.set_downstream(t2)

    t3.set_upstream(t1)

    # all of this is equivalent to 
    # dag.set_dependencies('print_date', 'sleep')
    # dag.set_dependencies('print_date', 'templated')

Note that when executing your script, Airflow will raise exceptions when
it finds cycles in your DAG or when a dependency is referenced more
than once.

Recap
-----
Alright, so we have a pretty basic DAG. At this point your code should look 
something like this:

.. code:: python

    """
    Code that goes along with the Airflow located at:
    http://airflow.readthedocs.org/en/latest/tutorial.html
    """
    from airflow import DAG
    from airflow.operators import BashOperator
    from datetime import datetime


    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2015, 1, 1),
        'email': ['airflow@airflow.com'],
        'email_on_failure': False,
        'email_on_retry': False,
    }

    dag = DAG('tutorial', default_args=default_args)

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag)

    t2 = BashOperator(
        task_id='sleep',
        email_on_failure=False,
        bash_command='sleep 5',
        dag=dag)

    templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """

    t3 = BashOperator(
        task_id='templated',
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
        dag=dag)

    t2.set_upstream(t1)
    t3.set_upstream(t1)

Testing
--------

Running the Script
''''''''''''''''''

Time to run some tests. First let's make sure that the pipeline
parses. Let's assume we're saving the code from the previous step in
``tutorial.py`` in the DAGs folder referenced in your ``airflow.cfg``.
The default location for your DAGs is ``~/airflow/dags``.

.. code-block:: bash

    python ~/airflow/dags/tutorial.py

If the script does not raise an exception it means that you haven't done
anything horribly wrong, and that your Airflow environment is somewhat
sound.

Command Line Metadata Validation
'''''''''''''''''''''''''''''''''
Let's run a few commands to validate this script further.

.. code-block:: bash

    # print the list of active DAGs
    airflow list_dags

    # prints the list of tasks the "tutorial" dag_id
    airflow list_tasks tutorial

    # prints the hierarchy of tasks in the tutorial DAG
    airflow list_tasks tutorial --tree


Testing
'''''''
Let's test by running the actual task instances on a specific date.

.. code-block:: bash

    # command layout: command subcommand dag_id task_id date

    # testing print_date
    airflow test tutorial print_date 2015-01-01

    # testing sleep
    airflow test tutorial sleep 2015-01-01

Now remember what we did with templating earlier? See how this template
gets rendered and executed by running this command:

.. code-block:: bash

    # testing templated
    airflow test tutorial templated 2015-01-01

This should result in displaying a verbose log of events and ultimately 
running your bash command and printing the result.

Note that the ``airflow test`` command runs task instances locally, output
their log to stdout (on screen), don't bother with dependencies, and
don't communicate their state (running, success, failed, ...) to the 
database. It simply allows to test a single a task instance.

Backfill
''''''''
Everything looks like it's running fine so let's run a backfill.
``backfill`` will respect your dependencies, log into files and talk to the
database to record status. If you do have a webserver up, you'll be able to
track the progress. ``airflow webserver`` will start a web server if you
are interested in tracking the progress visually as you backfill progresses.

Note that if you use ``depend_on_past=True``, individual task instances 
depends the success of the preceding task instance, except for the
start_date specified itself, for which this dependency is disregarded.

.. code-block:: bash

    # optional, start a web server in debug mode in the background
    # airflow webserver --debug &

    # start your backfill on a date range
    airflow backfill tutorial -s 2015-01-01 -e 2015-01-07


What's Next?
-------------
That's it, you've written, tested and backfilled your very first Airflow
pipeline. Merging your code into a code repository that has a master scheduler
running on top of should get it to get triggered and run everyday.

Here's a few things you might want to do next:

* Take an in-depth tour of the UI, click all the things!
* Keep reading the docs! Especially the sections on:

    * Command line interface
    * Operators
    * Macros

* Write you first pipeline!
