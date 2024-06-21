# Set of metrics that refer to time series data
TIME_SERIES_METRICS = {
    "k8s_node__CUMULATIVE__kubernetes_io_node_cpu_core_usage_time",
    "k8s_node__CUMULATIVE__kubernetes_io_node_network_received_bytes_count",
    "k8s_node__CUMULATIVE__kubernetes_io_node_network_sent_bytes_count",
    "k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_evictable",
    "k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_non_evictable",
    "k8s_pod__CUMULATIVE__kubernetes_io_pod_network_received_bytes_count",
    "k8s_pod__CUMULATIVE__kubernetes_io_pod_network_sent_bytes_count",
    "k8s_container__CUMULATIVE__kubernetes_io_container_cpu_core_usage_time",
    "k8s_container__GAUGE__kubernetes_io_container_memory_used_bytes__memory_type_evictable",
    "k8s_container__GAUGE__kubernetes_io_container_memory_used_bytes__memory_type_non_evictable",
}

# Set of time series metrics designated for aggregated resources
AGGREGATED_TIME_SERIES_METRICS = {
    "k8s_node__GAUGE__kubernetes_io_node_cpu_core_usage_time_per_second",
    "k8s_node__GAUGE__kubernetes_io_node_network_received_bytes_count_per_second",
    "k8s_node__GAUGE__kubernetes_io_node_network_sent_bytes_count_per_second",
    "k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_evictable",
    "k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_non_evictable",
    "k8s_pod__GAUGE__kubernetes_io_pod_network_received_bytes_count_per_second",
    "k8s_pod__GAUGE__kubernetes_io_pod_network_sent_bytes_count_per_second",
    "k8s_container__GAUGE__kubernetes_io_container_cpu_core_usage_time_per_second",
    "k8s_container__GAUGE__kubernetes_io_container_memory_used_bytes__memory_type_evictable",
    "k8s_container__GAUGE__kubernetes_io_container_memory_used_bytes__memory_type_non_evictable",
}

# Set of metrics that refer to the whole test attempt
TIME_SERIES_METRIC_TYPES_ORDER = (
    "cpu_core_usage_time",
    "received_bytes_count",
    "sent_bytes_count",
    "memory_used_bytes__memory_type_evictable",
    "memory_used_bytes__memory_type_non_evictable",
)

# Their average across different test attempts will be calculated
GENERAL_METRICS = {
    "test_duration",
    "dag_run_average_duration",
    "dag_run_min_duration",
    "dag_run_max_duration",
    "task_instance_average_duration",
    "task_instance_min_duration",
    "task_instance_max_duration",
}

TIME_SERIES_CHART_METRIC_COLUMNS = [
    "all__k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_evictable",
    "all__k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_non_evictable",
    "all__k8s_node__GAUGE__kubernetes_io_node_cpu_core_usage_time_per_second",
    "all__k8s_node__GAUGE__kubernetes_io_node_network_received_bytes_count_per_second",
    "all__k8s_node__GAUGE__kubernetes_io_node_network_sent_bytes_count_per_second",
]

BOXPLOT_METRIC_COLUMNS = [
    "k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_evictable",
    "k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_non_evictable",
    "k8s_node__GAUGE__kubernetes_io_node_cpu_core_usage_time_per_second",
    "k8s_node__GAUGE__kubernetes_io_node_network_received_bytes_count_per_second",
    "k8s_node__GAUGE__kubernetes_io_node_network_sent_bytes_count_per_second",
]
