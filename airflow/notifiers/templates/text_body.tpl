Airflow alert: {{ task_instance }}

Try {{ task_instance.try_number }} out of {{ task.retries + 1 }} is marked {{ task_instance.state }}
Exception:
{{ message }}

Host: {{ task_instance.hostname }}
Log: {{ task_instance.log_url }}
Log file: {{ task_instance.log_filepath }}
Mark success: {{ task_instance.mark_success_url }}