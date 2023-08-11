 .. code-block:: python

    @task
    def print_ti_info(task_instance=None, dag_run=None):
        print(f"Run ID: {task_instance.run_id}") # Run ID: scheduled__2023-08-09T00:00:00+00:00
        print(f"Duration: {task_instance.duration}") # Duration: 0.972019
        print(f"DAG Run queued at: {dag_run.queued_at}") # 2023-08-10 00:00:01+02:20