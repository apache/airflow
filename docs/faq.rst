FAQ
========

**Why isn't my task getting scheduled?**

There are very many reasons why your task might not be getting scheduled. 
Here are some of the common causes:

- Does your script "compile", can the Airflow engine parse it and find your
  DAG object. To test this, you can run ``airflow list_dags`` and 
  confirm that your DAG shows up in the list. You can also run 
  ``airflow list_tasks foo_dag_id --tree`` and confirm that your task 
  shows up in the list as expected. If you use the CeleryExecutor, you
  may way to confirm that this works both where the scheduler runs as well
  as where the worker runs.

- Is your ``start_date`` set properly? The Airflow scheduler triggers the
  task soon after the ``start_date + scheduler_interval`` is passed. 

- Is your ``start_date`` beyond where you can see it in the UI? If you 
  set your it to some time say 3 months ago, you won't be able to see
  it in the main view in the UI, but you should be able to see it in the
  ``Menu -> Browse ->Task Instances``.

- Are the dependencies for the task met. The task instances directly
  upstream from the task need to be in a ``success`` state. Also,
  if you have set ``depends_on_past=True``, the previous task instance
  needs to have succeeded (except if it is the first run for that task).
  Also, if ``wait_for_downstream=True``, make sure you understand
  what it means.
  You can view how these properties are set from the ``Task Details``
  page for your task. 

You may also want to read the Scheduler section of the docs and make 
sure you fully understand how it proceeds.


**How do I trigger tasks based on another task's failure?**

Check out the ``Trigger Rule`` section in the Concepts section of the 
documentation

