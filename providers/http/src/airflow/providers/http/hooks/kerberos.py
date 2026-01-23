from typing import Any
import json

from requests_kerberos import HTTPKerberosAuth

from airflow.security import kerberos
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.configuration import conf

from airflow.providers.http.hooks.http import HttpHook


class HttpKerberosHook(HttpHook, LoggingMixin):
    """HTTP Hook with Kerberos Authentication.
    Uses defaults from `[kerberos]` config section

    :param principal: Kerberos principal
    :param service: Keberos service
    :param keytab: Kerberos keytab
    :param password: Kerberos user's password
    :param renewal_lifetime: Kerberos ticket renewal
    :param forwardable: Is kerberos ticket forwardable
    :param include_ip: Include ip address in kerberos ticket
    :param cache: Kerberos ccache file
    :param auth_kwargs: additional kwargs to pass to HTTPKerberosAuth
    """

    conn_type = "http_kerberos"
    default_conn_name = "http_default"
    hook_name = "HTTP (Kerberos)"

    def __init__(
        self,
        principal: str | None = None,
        service: str | None = None,
        keytab: str | None = None,
        password: str | None = None,
        renewal_lifetime: int | None = None,
        forwardable: bool | None = None,
        include_ip: bool | None = None,
        cache: str | None = None,
        auth_kwargs: dict | None = None,
        **http_kwargs,
    ) -> None:
        self.principal = principal or conf.get_mandatory_value("kerberos", "principal")
        self.service = service
        self.keytab = keytab or conf.get_mandatory_value("kerberos", "keytab")
        self.password = password
        self.renewal_lifetime = renewal_lifetime or conf.getint("kerberos", "reinit_frequency")
        self.forwardable = forwardable or conf.getboolean("kerberos", "forwardable")
        self.include_ip = include_ip or conf.getboolean("kerberos", "include_ip")
        self.cache = cache or conf.get_mandatory_value("kerberos", "ccache")
        if auth_kwargs is None:
            auth_kwargs = {}
        auth_kwargs["principal"] = self.principal
        if service is not None:
            auth_kwargs["service"] = self.service
        super().__init__(auth_type=HTTPKerberosAuth(**auth_kwargs), **http_kwargs)

    def run(
        self,
        endpoint: str | None = None,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        **request_kwargs: Any,
    ) -> Any:
        self.log.info("Checking kerberos ticket")
        if not kerberos.check_klist_output(self.principal,self.service):
            if self.service:
                log_line = f"Renewing ticket for {self.principal} on service {self.service}"
            else:
                log_line = f"Renewing ticket for {self.principal}"
            self.log.info(log_line)
            self.reinit_kerberos()
        return super().run(endpoint, data, headers, extra_options, **request_kwargs)

    def reinit_kerberos(self):
        """Refresh kerberos ticket for this hook

        :return: True if kinit was successful.
        """
        ret = kerberos.renew_ticket(
            self.principal,
            self.keytab,
            self.password,
            exit_on_fail=False,
            renewal_lifetime=self.renewal_lifetime,
            forwardable=self.forwardable,
            include_ip=self.include_ip,
            cache=self.cache,
        )

        if ret != 0:
            raise ChildProcessError(f"kinit failed with exit code {ret}")

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": [],
            "relabeling": {
                "password": "Password (optional)",
            },
            "placeholders": {
                "extra": json.dumps(
                    {
                        "keytab": "optional/path/to/keytab",
                        "renewal_lifetime": "3600",
                        "cache": "optional/path/to/krb5/cache",
                        "auth_kwargs": "{}"
                    }
                )
            },
        }

    @classmethod
    def get_connection_from_widgets(cls):
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField, IntegerField

        return {
            "forwardable": BooleanField(lazy_gettext("Is ticket forwardable")),
            "include_ip": BooleanField(lazy_gettext("Include ip address in the ticket")),
            "keytab": StringField(lazy_gettext("Path to keytab file"), widget=BS3TextFieldWidget()),
            "principal": StringField(lazy_gettext("Kerberos principal"), widget=BS3TextFieldWidget()),
            "renew_lifetime": IntegerField(
                lazy_gettext("Kerberos ticket lifetime"), widget=BS3TextFieldWidget()
            ),
            "cache": StringField(lazy_gettext("Kerberos cache location"), widget=BS3TextFieldWidget()),
            "service": StringField(lazy_gettext("Kerberos Service"), widget=BS3TextFieldWidget()),
        }
