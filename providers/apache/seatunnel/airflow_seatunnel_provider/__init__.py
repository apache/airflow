__version__ = "0.1.0"

def get_provider_info():
    return {
        "package-name": "airflow-provider-seatunnel",
        "name": "Apache SeaTunnel Provider",
        "description": "Apache Airflow provider for Apache SeaTunnel.",
        "connection-types": [
            {
                "connection-type": "seatunnel",
                "hook-class-name": "airflow_seatunnel_provider.hooks.seatunnel_hook.SeaTunnelHook"
            }
        ],
        "versions": [__version__]
    }
