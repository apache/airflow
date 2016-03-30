import httplib2
import logging

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from oauth2client.client import SignedJwtAssertionCredentials, GoogleCredentials

class GoogleCloudBaseHook(BaseHook):
    """
    A base hook for Google cloud-related hooks. Google cloud has a shared REST
    API client that is built in the same way no matter which service you use.
    This class helps construct and authorize the credentials needed to then
    call apiclient.discovery.build() to actually discover and build a client
    for a Google cloud service.

    The class also contains some miscellaneous helper functions.
    """

    def __init__(self, scope, conn_id, delegate_to=None):
        """
        :param scope: The scope of the hook.
        :type scope: string or an iterable of strings.
        :param conn_id: The connection ID to use when fetching connection info.
        :type conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have domain-wide delegation enabled.
        :type delegate_to: string

        """
        self.scope = scope
        self.conn_id = conn_id
        self.delegate_to = delegate_to

    def _authorize(self):
        """
        Returns an authorized HTTP object to be used to build a Google cloud
        service hook connection.
        """
        connection_info = self.get_connection(self.conn_id)
        connection_extras = connection_info.extra_dejson
        service_account = connection_extras.get('service_account', False)
        key_path = connection_extras.get('key_path', False)

        kwargs = {}
        if self.delegate_to:
            kwargs['sub'] = self.delegate_to

        if not key_path or not service_account:
            logging.info('Getting connection using `gcloud auth` user, since no service_account/key_path are defined for hook.')
            credentials = GoogleCredentials.get_application_default()
        elif self.scope:
            with open(key_path, 'rb') as key_file:
                key = key_file.read()
                credentials = SignedJwtAssertionCredentials(
                    service_account,
                    key,
                    scope=self.scope,
                    **kwargs)
        else:
            raise AirflowException('Scope undefined, or either key_path/service_account config was missing.')

        http = httplib2.Http()
        return credentials.authorize(http)

    def _extras_dejson(self):
        """
        A little helper method that returns the JSON-deserialized extras in a
        single call.
        """
        return self.get_connection(self.conn_id).extra_dejson
