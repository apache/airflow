<html>
<body>
	<h1>
		Airflow alert: {{ task_instance }}
	</h1>
	<div>
		<p>
			Try {{ task_instance.try_number }} out of {{ task.retries + 1 }} is marked {{ task_instance.state }}
		</p>
		<p>
			Exception:<pre>{{ message }}</pre>
		</p>
		<p>
			Host: {{ task_instance.hostname }}<br>
			Log: <a href='{{ task_instance.log_url }}'>Link</a><br>
			Log file: {{ task_instance.log_filepath }}<br>
			Mark success: <a href='{{ task_instance.mark_success_url }}'>Link</a>
		</p>
	</div>
</body>
</html>