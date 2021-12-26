from typing import Optional

from airflow.providers.amazon.aws.hooks.eks import ClusterStates, FargateProfileStates, NodegroupStates
from airflow.providers.amazon.aws.sensors.eks import DEFAULT_CONN_ID

class EksClusterStateSensor:
    def __init__(
        self,
        *,
        cluster_name: Optional[str] = None,
        target_state: ClusterStates = ClusterStates.ACTIVE,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ): ...

class EksFargateProfileStateSensor:
    def __init__(
        self,
        *,
        fargate_profile_name: str,
        cluster_name: Optional[str] = None,
        target_state: FargateProfileStates = FargateProfileStates.ACTIVE,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ): ...

class EksNodegroupStateSensor:
    def __init__(
        self,
        *,
        nodegroup_name: str,
        cluster_name: Optional[str] = None,
        target_state: NodegroupStates = NodegroupStates.ACTIVE,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ): ...
