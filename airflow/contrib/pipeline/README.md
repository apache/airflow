# Airflow Pipelines

Pipelines allow Airflow tasks to share data between (optionally named) outputs
of upstream tasks and named inputs of downstream tasks.

Pipelines automatically serialize/deserialize data and communicate between tasks. Users can specify:
- The serialization method (how data objects are transformed to strings)
    - For example, a dictionary might be serialized to JSON or a file might be uploaded to remote storage and serialized to a URI
- The deserialization method (how data objects are reconstructed from serialized strings)
    - For example, restoring a dictinoary from JSON or downloading a file from a remote URI

The basic Pipeline class stores all data in Airflow XComs. XComs are only appropriate for very small and simple data. Other Pipelines include `FileSystemPipeline`, which stores data in the local filesystem (this can not be used with distributed Airflow environments) and `GCSPipeline`, which stores data in Google Cloud Storage.


## Example

```python

with dag:

    # ----
    # create a task that generates data
    upstream_task = PythonOperator(
        task_id='upstream_task',
        python_callable=lambda: {'output_1': 1, 'output_2': [2, 3]})

    # ----
    # create a task to work with that data
    downstream_task = PythonOperator(
        task_id='downstream_task',
        python_callable=lambda x: x + 1)

    # ----
    # create a pipeline to automatically move data from the upstream task to the downstream task
    Pipeline(
        # identify the downstream task
        downstream_task=downstream_task,

        # the pipelined data will be available in the context under this key.
        # since the task is a PythonOperator, it will also be passed as a kwarg.
        downstream_key='x',

        # specify the upstream task
        upstream_task=upstream_task,

        # the upstream task's result will be indexed by this optional key
        upstream_key='output_1'
    )

    # ----
    # or use the add_pipeline convenience function (this is equivalent to the above command)
    add_pipeline(
        downstream_task=downstream_task,
        pipelines={
            'x': {'task': upstream_task, 'key': output_1}
        }
    )

```

## Implementation

Pipelines are built on top of Airflow's XCom system. Once a Pipeline is created,
the following happens whenever tasks are run:

After the upstream task runs:
1. The `upstream_task` generates a `result`. If an `upstream_key` was provided to the `Pipeline`, the result is indexed by that key. This allows users to select one of potentially many named outputs for a specific pipeline.
1. The pipeline's `serialize()` function is called on the result. This user-defined function takes the data and transforms it in to a string representation via some reversible transformation.
1. The upstream task pushes an XCom containing the serialized result.

Before the downstream task runs:
1. The upstream task's XCom is pulled and the serialized data is retrieved
1. The pipeline's `deserialize()` function is called to restore the original data from the serialized representation.
1. The data is added to the `downstream_task`'s `context` as `context['pipeline'][downstream_key]`. This allows pipelines to be retrieved as named inputs. If the downstream task is a `PythonOperator`, the data is also passed as a keyword argument.

After the downstream task runs:
1. The pipeline's `clean_up()` method is called
