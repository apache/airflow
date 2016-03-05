from airflow.hooks import BaseHook
import gcloud

class GCPBaseHook(BaseHook):
    """
    A hook for working wth Google Cloud Platform via the gcloud library.

    A GCP connection ID can be provided. If it is provided, its "extra" values
    will OVERRIDE any argments passed to GCSHook. The following precendance is
    observed:
        GCP connection "extra"
        GCPBaseHook initialization arguments
        host environment

    Extras should be JSON and take the form:
    {
        "project": "<google cloud project id>",
        "key_path": "<path to service account keyfile, either JSON or P12>"
        "service_account": "<google service account email, required for P12>"
        "scope": "<google service scopes>"
    }

    service_account is only required if the key_path points to a P12 file.

    scope is only used if key_path is provided. Scopes can include:
        https://www.googleapis.com/auth/devstorage.full_control
        https://www.googleapis.com/auth/devstorage.read_only
        https://www.googleapis.com/auth/devstorage.read_write

    If fields are not provided, either as arguments or extras, they can be set
    in the host environment.

    To set a default project, use:
        gcloud config set project <project-id>

    To log in:
        gcloud auth

    """

    client_class = None

    def __init__(
            self,
            gcp_conn_id=None,
            project=None,
            key_path=None,
            service_account=None,
            scope=None,
            *args,
            **kwargs):

        if not self.client_class:
            raise NotImplementedError(
                'The GCPBaseHook must be extended by providing a client_class.')

        # compatibility with GoogleCloudStorageHook
        if 'google_cloud_storage_conn_id' in kwargs and not gcp_conn_id:
            gcp_conn_id = kwargs.pop('google_cloud_storage_conn_id')

        self.gcp_conn_id = gcp_conn_id
        self.project = project
        self.key_path = key_path
        self.service_account = service_account
        self.scope = scope

        self.client = self.get_conn()

    def get_conn(self):
        # parse arguments and connection extras
        if self.gcp_conn_id:
            extras = self.get_connection(self.gcp_conn_id).extra_dejson
        else:
            extras = {}
        project = extras.get('project', self.project)
        key_path = extras.get('key_path', self.key_path)
        service_account = extras.get('service_account', self.service_account)
        scope = extras.get('scope', self.scope)

        # guess project, if possible
        if not project:
            project = gcloud._helpers._determine_default_project()
            # workaround for
            # https://github.com/GoogleCloudPlatform/gcloud-python/issues/1470
            if isinstance(project, bytes):
                project = project.decode()

        # load credentials/scope
        if key_path:
            if key_path.endswith('.json') or key_path.endswith('.JSON'):
                credentials = gcloud.credentials.get_for_service_account_json(
                    json_credentials_path=key_path,
                    scope=scope)
            elif key_path.endswith('.p12') or key_path.endswith('.P12'):
                credentials = gcloud.credentials.get_for_service_account_p12(
                    client_email=service_account,
                    private_key_path=key_path,
                    scope=scope)
            else:
                raise ValueError('Unrecognized keyfile: {}'.format(key_path))
            client = self.client_class(
                credentials=credentials,
                project=project)
        else:
            client = self.client_class(project=project)

        return client
