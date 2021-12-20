from typing import Optional

class OSSUploadObjectOperator:
    def __init__(
        self,
        key: str = ...,
        file: str = ...,
        region: str = ...,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...

class OSSDownloadObjectOperator:
    def __init__(
        self,
        key: str = ...,
        file: str = ...,
        region: str = ...,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...

class OSSDeleteBatchObjectOperator:
    def __init__(
        self,
        keys: list = ...,
        region: str = ...,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...

class OSSDeleteObjectOperator:
    def __init__(
        self,
        key: str = ...,
        region: str = ...,
        bucket_name: Optional[str] = None,
        oss_conn_id: str = 'oss_default',
        **kwargs,
    ) -> None: ...
