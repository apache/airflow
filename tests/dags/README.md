# Unit Tests DAGs Folder

This folder contains DAGs for Airflow unit testing.

To access a DAG in this folder, use the following code inside a unit test. Note this only works when `test_mode` is on; otherwise the normal Airflow `DAGS_FOLDER` will take precedence.

```python
dagbag = DagBag()
dag = dagbag.get_dag(dag_id)
```
