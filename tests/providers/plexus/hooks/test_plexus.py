import arrow
import mock
import pytest
from airflow.exceptions import AirflowException
from airflow.providers.plexus.hooks.plexus import PlexusHook
from mock import Mock
from requests.exceptions import Timeout

class TestPlexusHook:

    @mock.patch('airflow.providers.plexus.hooks.plexus.PlexusHook._generate_token')
    def test_get_token(self, mock_generate_token):
        """test get token"""
        mock_generate_token.return_value = 'token'
        hook = PlexusHook()
        assert hook.token == 'token'

        hook.__token_exp = arrow.now().shift(minutes=-5)
        mock_generate_token.return_value = 'new_token'
        assert hook.token == 'new_token'


    @mock.patch('airflow.providers.plexus.hooks.plexus.jwt')
    @mock.patch('airflow.providers.plexus.hooks.plexus.requests')
    @mock.patch('airflow.providers.plexus.hooks.plexus.Variable')
    def test_generate_token(self, mock_creds, mock_request, mock_jwt):
        """test token generation"""
        hook = PlexusHook()
        mock_creds.get.side_effect = ['email', None]
        mock_request.post.return_value = Mock(**{'ok': True, 'json.return_value': {'access': 'token'}})
        mock_jwt.decode.return_value = {'user_id': 1, 'exp': 'exp'}
        with pytest.raises(AirflowException):
            hook._generate_token()

        mock_creds.get.side_effect = [None, 'pwd']
        with pytest.raises(AirflowException):
            hook._generate_token()

        
        mock_creds.get.side_effect = ['email', 'pwd']
        mock_request.post.return_value = Mock(ok=False)
        with pytest.raises(AirflowException):
            hook._generate_token()

        mock_creds.get.side_effect = ['email', 'pwd']
        mock_request.post.side_effect = Timeout
        with pytest.raises(Timeout):
            hook._generate_token()