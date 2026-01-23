import pytest

from requests_kerberos import HTTPKerberosAuth

from airflow.providers.http.hooks.kerberos import HttpKerberosHook


class TestHttpKerberosHook:
    def test_kerberos_auth(self):
        principal = "test_principal"
        keytab = "/test/keytab/file"
        hook = HttpKerberosHook(principal, keytab=keytab)
        assert isinstance(hook.auth_type, HTTPKerberosAuth)
