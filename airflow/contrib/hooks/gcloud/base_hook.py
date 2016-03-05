from airflow.hooks import BaseHook
import gcloud

class GCPBaseHook(BaseHook):
    """
    A hook for working wth Google Cloud Platform via the gcloud library.

    A GCP connection ID can be provided. If it is provided, its values
    will OVERRIDE any argments passed to the hook. The following precendance is
    observed:
        GCP connection fields
        GCPBaseHook initialization arguments
        host environment

    Google Cloud Platform connections can be created from the Airflow UI. If
    created manually, the relevant (but optional) fields should be added to
    the connection's "extra" field as JSON:
    {
        "project": "<google cloud project id>",
        "key_path": "<path to service account keyfile, either JSON or P12>"
        "service_account": "<google service account email, required for P12>"
        "scope": "<google service scopes, comma seperated>"
    }

    service_account is only required if the key_path points to a P12 file.

    scope is only used if key_path is provided. Scopes can include, for example:
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

        def load_field(f, fallback=None):
            # long_f: the format for UI-created fields
            long_f = 'extra__google_cloud_platform__{}'.format(f)
            if long_f in extras:
                return extras[long_f]
            elif f in extras:
                return extras[f]
            else:
                return getattr(self, fallback or f)

        project = load_field('project')
        key_path = load_field('key_path')
        service_account = load_field('service_account')
        scope = load_field('scope')
        if scope:
            scope = scope.split(',')
            
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
