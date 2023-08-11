 .. code-block:: python

    @task
    def print_ti_info(**kwargs):
        ti = kwargs['task_instance']
        print(f"Run ID: {ti.run_id}") # Run ID: scheduled__2023-08-09T00:00:00+00:00
        print(f"Duration: {ti.duration}") # Duration: 0.972019

        dr = kwargs['dag_run']
        print(f"DAG Run queued at: {dr.queued_at}") # 2023-08-10 00:00:01+02:20