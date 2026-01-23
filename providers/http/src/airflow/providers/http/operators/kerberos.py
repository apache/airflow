from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.http.hooks.kerberos import HttpKerberosHook
from airflow.providers.http.operators.http import HttpOperator


class HttpKerberosOperator(HttpOperator):
    def __init__(
        self,
        *,
        principal: str,
        service: str | None = None,
        endpoint: str | None = None,
        method: str = "POST",
        data: dict[str, Any] | str | None = None,
        headers: dict[str, str] | None = None,
        pagination_function: Callable[..., Any] | None = None,
        response_check: Callable[..., bool] | None = None,
        response_filter: Callable[..., Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        request_kwargs: dict[str, Any] | None = None,
        http_conn_id: str = "http_default",
        log_response: bool = False,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
        deferrable: bool = ...,
        retry_args: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.principal = principal
        self.service = service
        super().__init__(
            endpoint=endpoint,
            method=method,
            data=data,
            headers=headers,
            pagination_function=pagination_function,
            response_check=response_check,
            response_filter=response_filter,
            extra_options=extra_options,
            request_kwargs=request_kwargs,
            http_conn_id=http_conn_id,
            log_response=log_response,
            tcp_keep_alive=tcp_keep_alive,
            tcp_keep_alive_idle=tcp_keep_alive_idle,
            tcp_keep_alive_count=tcp_keep_alive_count,
            tcp_keep_alive_interval=tcp_keep_alive_interval,
            deferrable=deferrable,
            retry_args=retry_args,
            **kwargs,
        )

    @property
    def hook(self):
        conn_id = getattr(self, self.conn_id_field)
        self.log.debug("Get connection for %s", conn_id)
        conn = BaseHook.get_connection(conn_id)

        hook = HttpKerberosHook(
                principal=self.principal,
                service=self.service,
                method=self.method,
                keytab=conn.extra_dejson.get("keytab"),
                password=conn.password,
                renewal_lifetime= conn.extra_dejson.get("renewal_lifetime"),
                forwardable= conn.extra_dejson.get("forwardable"),
                include_ip = conn.extra_dejson.get("include_ip"),
                cache= conn.extra_dejson.get("cache"),
                auth_kwargs= conn.extra_dejson.get("auth_kwargs"),
                tcp_keep_alive=self.tcp_keep_alive,
                tcp_keep_alive_idle=self.tcp_keep_alive_idle,
                tcp_keep_alive_count=self.tcp_keep_alive_count,
                tcp_keep_alive_interval=self.tcp_keep_alive_interval,
        )
        return hook
