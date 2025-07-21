import tempfile
from airflow.sdk.api.client import Client

def test_client_loads_custom_ssl_certificate(monkeypatch):
    with tempfile.NamedTemporaryFile("w") as cert_file:
        cert_file.write("-----BEGIN CERTIFICATE-----\n...dummy...\n-----END CERTIFICATE-----")
        cert_file.flush()
        monkeypatch.setenv("AIRFLOW__API__SSL_CERT", cert_file.name)
        client = Client(token="abc", base_url="https://localhost")
        assert client is not None
