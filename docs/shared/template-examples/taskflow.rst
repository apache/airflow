 .. code-block:: python

    @task
    def print_ti_info(task_instance=None):
        print(f"Run ID: {task_instance.run_id}") # Run ID: scheduled__2023-08-09T00:00:00+00:00
        print(f"Duration: {task_instance.duration}") # Duration: 0.972019