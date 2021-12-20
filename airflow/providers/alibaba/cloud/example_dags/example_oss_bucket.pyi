from typing import Optional

class OSSCreateBucketOperator:
    def __init__(
        self,
        region: str = ...,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...

class OSSDeleteBucketOperator:
    def __init__(
        self,
        region: str = ...,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...
