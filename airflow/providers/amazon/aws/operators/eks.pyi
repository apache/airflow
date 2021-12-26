from typing import Optional, Dict, List

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.eks import (
    DEFAULT_NODEGROUP_NAME,
    DEFAULT_COMPUTE_TYPE,
    DEFAULT_FARGATE_PROFILE_NAME,
    DEFAULT_CONN_ID,
    DEFAULT_NAMESPACE_NAME,
)

class EksCreateClusterOperator(BaseOperator):
    def __init__(
        self,
        cluster_role_arn: str,
        resources_vpc_config: Dict,
        cluster_name: Optional[str] = None,
        compute: Optional[str] = DEFAULT_COMPUTE_TYPE,
        nodegroup_name: Optional[str] = DEFAULT_NODEGROUP_NAME,
        nodegroup_role_arn: Optional[str] = None,
        fargate_profile_name: Optional[str] = DEFAULT_FARGATE_PROFILE_NAME,
        fargate_pod_execution_role_arn: Optional[str] = None,
        fargate_selectors: Optional[List] = None,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None: ...

class EksCreateNodegroupOperator:
    def __init__(
        self,
        nodegroup_subnets: List[str],
        nodegroup_role_arn: str,
        cluster_name: Optional[str] = None,
        nodegroup_name: Optional[str] = DEFAULT_NODEGROUP_NAME,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None: ...

class EksDeleteClusterOperator:
    def __init__(
        self,
        force_delete_compute: bool = False,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        cluster_name: Optional[str] = None,
        **kwargs,
    ) -> None: ...

class EksDeleteNodegroupOperator:
    def __init__(
        self,
        nodegroup_name: str,
        cluster_name: Optional[str] = None,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None: ...

class EksPodOperator:
    def __init__(
        self,
        # Setting in_cluster to False tells the pod that the config
        # file is stored locally in the worker and not in the cluster.
        in_cluster: bool = False,
        cluster_name: Optional[str] = None,
        namespace: str = DEFAULT_NAMESPACE_NAME,
        pod_context: Optional[str] = None,
        pod_name: Optional[str] = None,
        pod_username: Optional[str] = None,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None: ...

class EksCreateFargateProfileOperator:
    def __init__(
        self,
        pod_execution_role_arn: str,
        selectors: List,
        cluster_name: Optional[str] = None,
        fargate_profile_name: Optional[str] = DEFAULT_FARGATE_PROFILE_NAME,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None: ...

class EksDeleteFargateProfileOperator:
    def __init__(
        self,
        fargate_profile_name: str,
        cluster_name: Optional[str] = None,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ) -> None: ...
