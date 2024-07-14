"""
Module containing definition of Cloud Monitoring metrics collected during performance tests.
"""

import enum

from typing import Dict, List

RESOURCE_LABEL_HIERARCHY = [
    "project_id",
    "location",
    "cluster_name",
    "node_name",
    "namespace_name",
    "pod_name",
    "container_name",
]


class ResourceType(enum.Enum):
    """
    Class representing resource types metrics of which are collected during performance tests.
    The order of enum member values will be used to decide the order of metric column in results
    dataframe.
    The reverse order will be used when joining resource dataframes
    - because of that resource type A should be placed before resource type B
    if the resource labels of type A are a subset of labels of type B.
    """

    CLUSTER = "k8s_cluster"
    NODE = "k8s_node"
    POD = "k8s_pod"
    CONTAINER = "k8s_container"


RESOURCE_TYPE_LABELS = {
    ResourceType.CLUSTER: ("project_id", "location", "cluster_name"),
    ResourceType.NODE: ("project_id", "location", "cluster_name", "node_name"),
    ResourceType.POD: (
        "project_id",
        "location",
        "cluster_name",
        "namespace_name",
        "pod_name",
    ),
    ResourceType.CONTAINER: (
        "project_id",
        "location",
        "cluster_name",
        "namespace_name",
        "pod_name",
        "container_name",
    ),
}


def get_monitored_resources_map(
    cluster_id: str, airflow_namespace_prefix: str
) -> Dict[ResourceType, Dict]:
    """
    Returns a dictionary with information which metrics should be collected for every ResourceType
    and which filtering labels should be applied to both the resource and the metric
    (for every ResourceType a time series listing call should be executed
    for every combination of resource group and metric).

    :param cluster_id: id of GKE cluster metrics of which should be collected.
    :type cluster_id: str
    :param airflow_namespace_prefix: prefix of the namespace where airflow pods are located.
    :type airflow_namespace_prefix: str

    :return: a dictionary that contains resource and metric information
        required to execute time series listing requests.
    :rtype: Dict[ResourceType, Dict]
    """

    monitored_resources = {
        ResourceType.NODE: {
            "resource_groups": [
                {"resource_label_filters": {"cluster_name": cluster_id}},
                {
                    "aggregation": True,
                    "new_resource_labels": {
                        "cluster_name": cluster_id,
                        "node_name": "nodes_combined",
                    },
                    "resource_label_filters": {"cluster_name": cluster_id},
                },
            ],
            "metrics": [
                {"metric_type": "kubernetes.io/node/cpu/core_usage_time"},
                {"metric_type": "kubernetes.io/node/network/received_bytes_count"},
                {"metric_type": "kubernetes.io/node/network/sent_bytes_count"},
                {"metric_type": "kubernetes.io/node_daemon/cpu/core_usage_time"},
                {"metric_type": "kubernetes.io/node/cpu/allocatable_cores"},
                {"metric_type": "kubernetes.io/node/cpu/allocatable_utilization"},
                {"metric_type": "kubernetes.io/node/cpu/total_cores"},
                {
                    "metric_type": "kubernetes.io/node/ephemeral_storage/allocatable_bytes"
                },
                {"metric_type": "kubernetes.io/node/ephemeral_storage/inodes_free"},
                {"metric_type": "kubernetes.io/node/ephemeral_storage/inodes_total"},
                {"metric_type": "kubernetes.io/node/ephemeral_storage/total_bytes"},
                {"metric_type": "kubernetes.io/node/ephemeral_storage/used_bytes"},
                {"metric_type": "kubernetes.io/node/memory/allocatable_bytes"},
                # we provide metric like this one twice with different metric label filters
                # to make aggregations for time series with different labels separately
                {
                    "metric_type": "kubernetes.io/node/memory/allocatable_utilization",
                    "metric_label_filters": {"memory_type": "evictable"},
                },
                {
                    "metric_type": "kubernetes.io/node/memory/allocatable_utilization",
                    "metric_label_filters": {"memory_type": "non-evictable"},
                },
                {"metric_type": "kubernetes.io/node/memory/total_bytes"},
                {
                    "metric_type": "kubernetes.io/node/memory/used_bytes",
                    "metric_label_filters": {"memory_type": "evictable"},
                },
                {
                    "metric_type": "kubernetes.io/node/memory/used_bytes",
                    "metric_label_filters": {"memory_type": "non-evictable"},
                },
                {"metric_type": "kubernetes.io/node/pid_limit"},
                {"metric_type": "kubernetes.io/node/pid_used"},
                {
                    "metric_type": "kubernetes.io/node_daemon/memory/used_bytes",
                    "metric_label_filters": {"memory_type": "evictable"},
                },
                {
                    "metric_type": "kubernetes.io/node_daemon/memory/used_bytes",
                    "metric_label_filters": {"memory_type": "non-evictable"},
                },
            ],
        },
        ResourceType.POD: {
            "resource_groups": [
                {
                    "resource_label_filters": {
                        "cluster_name": cluster_id,
                        "namespace_name_prefix": airflow_namespace_prefix,
                    }
                },
                {
                    "resource_label_filters": {
                        "cluster_name": cluster_id,
                        "namespace_name": "default",
                    }
                },
                {
                    "resource_label_filters": {
                        "cluster_name": cluster_id,
                        "namespace_name": "kube-system",
                        "pod_name_prefix": "fluentd-gcp",
                    }
                },
                {
                    "aggregation": True,
                    "new_resource_labels": {
                        "cluster_name": cluster_id,
                        "pod_name": "airflow_workers_combined",
                    },
                    "resource_label_filters": {
                        "cluster_name": cluster_id,
                        "namespace_name_prefix": airflow_namespace_prefix,
                        "pod_name_prefix": "airflow-worker",
                    },
                },
                {
                    "aggregation": True,
                    "new_resource_labels": {
                        "cluster_name": cluster_id,
                        "pod_name": "airflow_schedulers_combined",
                    },
                    "resource_label_filters": {
                        "cluster_name": cluster_id,
                        "namespace_name_prefix": airflow_namespace_prefix,
                        "pod_name_prefix": "airflow-scheduler",
                    },
                },
            ],
            "metrics": [
                {"metric_type": "kubernetes.io/pod/network/received_bytes_count"},
                {"metric_type": "kubernetes.io/pod/network/sent_bytes_count"},
            ],
        },
        ResourceType.CONTAINER: {
            "resource_groups": [
                {
                    "resource_label_filters": {
                        "cluster_name": cluster_id,
                        "namespace_name_prefix": airflow_namespace_prefix,
                    }
                },
                {
                    "resource_label_filters": {
                        "cluster_name": cluster_id,
                        "namespace_name": "default",
                    }
                },
                {
                    "resource_label_filters": {
                        "cluster_name": cluster_id,
                        "namespace_name": "kube-system",
                        "pod_name_prefix": "fluentd-gcp",
                    }
                },
                {
                    "aggregation": True,
                    "new_resource_labels": {
                        "cluster_name": cluster_id,
                        "pod_name": "airflow_workers_combined",
                    },
                    "resource_label_filters": {
                        "cluster_name": cluster_id,
                        "namespace_name_prefix": airflow_namespace_prefix,
                        "pod_name_prefix": "airflow-worker",
                    },
                },
                {
                    "aggregation": True,
                    "new_resource_labels": {
                        "cluster_name": cluster_id,
                        "pod_name": "airflow_schedulers_combined",
                    },
                    "resource_label_filters": {
                        "cluster_name": cluster_id,
                        "namespace_name_prefix": airflow_namespace_prefix,
                        "pod_name_prefix": "airflow-scheduler",
                    },
                },
            ],
            "metrics": [
                {"metric_type": "kubernetes.io/container/cpu/core_usage_time"},
                {"metric_type": "kubernetes.io/container/restart_count"},
                {"metric_type": "kubernetes.io/container/cpu/limit_cores"},
                {"metric_type": "kubernetes.io/container/cpu/limit_utilization"},
                {"metric_type": "kubernetes.io/container/cpu/request_cores"},
                {"metric_type": "kubernetes.io/container/cpu/request_utilization"},
                {
                    "metric_type": "kubernetes.io/container/ephemeral_storage/limit_bytes"
                },
                {
                    "metric_type": "kubernetes.io/container/ephemeral_storage/request_bytes"
                },
                {"metric_type": "kubernetes.io/container/ephemeral_storage/used_bytes"},
                {"metric_type": "kubernetes.io/container/memory/limit_bytes"},
                {
                    "metric_type": "kubernetes.io/container/memory/limit_utilization",
                    "metric_label_filters": {"memory_type": "evictable"},
                },
                {
                    "metric_type": "kubernetes.io/container/memory/limit_utilization",
                    "metric_label_filters": {"memory_type": "non-evictable"},
                },
                {
                    "metric_type": "kubernetes.io/container/memory/page_fault_count",
                    "metric_label_filters": {"fault_type": "major"},
                },
                {
                    "metric_type": "kubernetes.io/container/memory/page_fault_count",
                    "metric_label_filters": {"fault_type": "minor"},
                },
                {"metric_type": "kubernetes.io/container/memory/request_bytes"},
                {
                    "metric_type": "kubernetes.io/container/memory/request_utilization",
                    "metric_label_filters": {"memory_type": "evictable"},
                },
                {
                    "metric_type": "kubernetes.io/container/memory/request_utilization",
                    "metric_label_filters": {"memory_type": "non-evictable"},
                },
                {
                    "metric_type": "kubernetes.io/container/memory/used_bytes",
                    "metric_label_filters": {"memory_type": "evictable"},
                },
                {
                    "metric_type": "kubernetes.io/container/memory/used_bytes",
                    "metric_label_filters": {"memory_type": "non-evictable"},
                },
                {"metric_type": "kubernetes.io/container/uptime"},
            ],
        },
    }

    return monitored_resources


def get_merging_order() -> List[ResourceType]:
    """
    Returns the order in which resource types should be considered
    in terms of merging into each other

    :return: a list of ResourceType
    :rtype: List[ResourceType]
    """

    # we use a reversed order of resource types to start merging from the types
    # further in the hierarchy and avoid duplicating metric columns in the process;
    # for example, with POD type being mergeable into CONTAINER type and CLUSTER type
    # being mergeable into POD type (so, by extension, also CONTAINER type),
    # we want to first merge POD into CONTAINER, then CLUSTER into the resulting dataframe,
    # instead of merging CLUSTER separately into both POD and CONTAINER and then joining them
    resource_types_reversed = list(ResourceType)
    resource_types_reversed.reverse()

    return resource_types_reversed
