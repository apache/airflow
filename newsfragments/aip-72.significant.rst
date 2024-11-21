Create a TaskExecution interface and enforce DB isolation from Tasks

As part of this change the following breaking changes have occurred:

- Tasks and DAG Parsing code is not able to access the Airflow metadata DB

  Access via Variables and Connection is still allowed (though these will use an API, not direct DB access) -- it should be assumed that any use of the database models from within ``airflow.models`` inside of DAG files or tasks will break.

- Remove the concept of pluggable TaskRunners.

  The ``task_runner`` config in ``[core]`` has been removed.

  There were two build in options for this, Standard (the default) which used Fork or a new process as appropriate, and CGroupRunner to launch tasks in a new CGroup (not usable inside docker or Kubernetes).

  With the move of the execution time code into the TaskSDK we are using this opportunity to reduce complexity for seldom used features.

- Shipping DAGs via pickle is no longer supported

  This was a feature that was not widely used and was a security risk. It has been removed.

- Pickling is no longer supported for XCom serialization.

  XCom data will no longer support pickling. This change is intended to improve security and simplify data
  handling by supporting JSON-only serialization. DAGs that depend on XCom pickling must update to use JSON-serializable data.

  As part of that change, ``[core] enable_xcom_pickling`` configuration option has been removed.

  If you still need to use pickling, you can use a custom XCom backend that stores references in the metadata DB and
  the pickled data can be stored in a separate storage like S3.

  The ``value`` field in the XCom table has been changed to a ``JSON`` type via DB migration. The XCom records that
  contains pickled data are archived in the ``_xcom_archive`` table. You can safely drop this table if you don't need
  the data anymore.
