Lineage
=======

.. note:: Lineage support is very experimental and subject to change.

Airflow can help track origins of data, what happens to it and where it moves over time. This can aid having
audit trails and data governance, but also debugging of data flows.

Airflow tracks data by means of inlets and outlets of the tasks. Let's work from an example and see how it
works.

.. code:: python

    from airflow.operators.bash_operator import BashOperator
    from airflow.operators.dummy_operator import DummyOperator
    from airflow.lineage.datasets import File
    from airflow.models import DAG
    from datetime import timedelta
    
    FILE_CATEGORIES = ["CAT1", "CAT2", "CAT3"]
    
    args = {
        'owner': 'airflow',
        'start_date': airflow.utils.dates.days_ago(2)
    }
    
    dag = DAG(
        dag_id='example_lineage', default_args=args,
        schedule_interval='0 0 * * *',
        dagrun_timeout=timedelta(minutes=60))
    
    f_final = File("/tmp/final")
    run_this_last = DummyOperator(task_id='run_this_last', dag=dag, 
        inlets={"auto": True},
        outlets={"datasets": [f_final,]})
    
    f_in = File("/tmp/whole_directory/")
    outlets = []
    for file in FILE_CATEGORIES:
        f_out = File("/tmp/{}/{{{{ execution_date }}}}".format(file))
        outlets.append(f_out)
    run_this = BashOperator(    
        task_id='run_me_first', bash_command='echo 1', dag=dag,
        inlets={"datasets": [f_in,]},
        outlets={"datasets": outlets}
        )
    run_this.set_downstream(run_this_last)


Tasks take the parameters `inlets` and `outlets`. Inlets can be manually defined by a list of dataset `{"datasets":
[dataset1, dataset2]}` or can be configured to look for outlets from upstream tasks `{"task_ids": ["task_id1", "task_id2"]}`
or can be configured to pick up outlets from direct upstream tasks `{"auto": True}` or a combination of them. Outlets 
are defined as list of dataset `{"datasets": [dataset1, dataset2]}`. Any fields for the dataset are templated with 
the context when the task is being executed. 

.. note:: Operators can add inlets and outlets automatically if the operator supports it.

In the example DAG task `run_me_first` is a BashOperator that takes 3 inlets: `CAT1`, `CAT2`, `CAT3`, that are 
generated from a list. Note that `execution_date` is a templated field and will be rendered when the task is running.

.. note:: Behind the scenes Airflow prepares the lineage metadata as part of the `pre_execute` method of a task. When the task
          has finished execution `post_execute` is called and lineage metadata is pushed into XCOM. Thus if you are creating 
          your own operators that override this method make sure to decorate your method with `prepare_lineage` and `apply_lineage`
          respectively.


Apache Atlas
------------

Airflow can send its lineage metadata to Apache Atlas. You need to enable the `atlas` backend and configure it 
properly, e.g. in your `airflow.cfg`:

.. code:: python

    [lineage]
    backend = airflow.lineage.backend.atlas

    [atlas]
    username = my_username
    password = my_password
    host = host
    port = 21000
    

Please make sure to have the `atlasclient` package installed.
