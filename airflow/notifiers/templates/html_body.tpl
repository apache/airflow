{#
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  
    http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#}
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