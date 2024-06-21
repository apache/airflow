# {{ report_title }}
Updated: {{ creation_date }}


## Configuration

Results table
- project id: {{ project_id }}
- dataset: {{ dataset_id }}
- table: {{ table_id }}

Detailed information about configurations of tests attempts belonging to the table:

{{ conf_table }}

## Analysis

Test attempts that contain failed dag runs or are considered anomalous (their test duration is
either lower than 50% or longer than 150% of average test duration) are removed from analysis.

Only aggregated time series metrics are used in analysis.

### Metrics description
- Time series metrics
    - Node metrics
        - all__k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_evictable -
        memory bytes used by all nodes that can be evicted at any time.
        - all__k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_non_evictable -
        memory bytes used by all nodes that cannot be evicted.
        - all__k8s_node__GAUGE__kubernetes_io_node_cpu_core_usage_time_per_second - rate at which
        CPU usage changes on all cores across all nodes.
        - all__k8s_node__GAUGE__kubernetes_io_node_network_received_bytes_count_per_second - rate in
        bytes per second at which bytes are received over the network across all nodes.
        - all__k8s_node__GAUGE__kubernetes_io_node_network_sent_bytes_count_per_second - rate in
        bytes per second at which number of bytes are transmitted over the network across all nodes.
    - Airflow worker metrics
        - airflow-worker__k8s_pod__GAUGE__kubernetes_io_pod_network_received_bytes_count_per_second -
        rate in bytes per second at which bytes are received over the network across all airflow-worker pods.
        - airflow-worker__k8s_pod__GAUGE__kubernetes_io_pod_network_sent_bytes_count_per_second -
        rate in bytes per second at which bytes are sent over the network across all airflow-worker pods.
        - airflow-worker__k8s_container__GAUGE__kubernetes_io_container_cpu_core_usage_time_per_second -
        rate at which CPU usage changes on all cores across all containers running on airflow-worker pods.
        - airflow-worker__k8s_container__GAUGE__kubernetes_io_container_memory_used_bytes__memory_type_evictable -
        memory bytes used by all containers running on airflow-worker pods that can be evicted at any time.
        - airflow-worker__k8s_container__GAUGE__kubernetes_io_container_memory_used_bytes__memory_type_non_evictable -
        memory bytes used by all containers running on airflow-worker pods that cannot be evicted.
    - Airflow scheduler metrics
        - airflow-scheduler__k8s_pod__GAUGE__kubernetes_io_pod_network_received_bytes_count_per_second -
        rate in bytes per second at which bytes are received over the network across all airflow-scheduler pods.
        - airflow-scheduler__k8s_pod__GAUGE__kubernetes_io_pod_network_sent_bytes_count_per_second -
        rate in bytes per second at which bytes are sent over the network across all airflow-scheduler pods.
        - airflow-scheduler__k8s_container__GAUGE__kubernetes_io_container_cpu_core_usage_time_per_second -
        rate at which CPU usage changes on all cores across all containers running on airflow-scheduler pods.
        - airflow-scheduler__k8s_container__GAUGE__kubernetes_io_container_memory_used_bytes__memory_type_evictable -
        memory bytes used by all containers running on airflow-scheduler pods that can be evicted at any time.
        - airflow-scheduler__k8s_container__GAUGE__kubernetes_io_container_memory_used_bytes__memory_type_non_evictable -
        memory bytes used by all containers running on airflow-scheduler pods that cannot be evicted.
- Airflow time metrics
    - dag_run_average_duration - average duration of test dag runs
    - dag_run_max_duration - max duration of test dag run
    - dag_run_min_duration - min duration of test dag run
    - task_instance_average_duration - average duration of task instances belonging to test dag runs
    - task_instance_max_duration - max duration of any task instance belonging to test dag runs
    - task_instance_min_duration - min duration of any task instance belonging to test dag runs
    - test_attempts - total number of test attempts belonging to given group
    - test_duration - total time in which all expected test dag runs were finished

### Average values for aggregated metrics

Table below contains average values of the metrics described above. Averages were first calculated
for every test attempt separately (in case of time series metrics), then across all test attempts
belonging to given group.

{{ statistics_df }}

### Time series charts

Charts below show how the presented time series metrics changed in time. Each line represents a
single test attempt (uuid) belonging to the results table.

{% for time_series_chart in time_series_charts_paths %}
![alt text]({{ time_series_chart }} "Time series chart")
{% endfor %}

### Boxplot charts

Boxplot charts show the overall range of values of the time series metrics
across all test attempts belonging to the results table.

{% for box_plot in box_plots_paths %}
![alt text]({{ box_plot }} "Boxplot chart")

{% endfor %}
