import jwt
import arrow
import requests
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowException

class PlexusHook(BaseHook):
    def __init__(
        self
    ) -> None:
        super().__init__()
        self.__token = None
        self.__token_exp = None
        self.host = "https://apiplexus.corescientific.com/"
        self.user_id = None

    def _generate_token(self):
        login = Variable.get("email")
        pwd = Variable.get("password")
        if login is None or pwd is None:
            raise AirflowException("No valid email/password supplied.")
        token_endpoint = self.host + "sso/jwt-token/"
        response = requests.post(token_endpoint, data={"email": login, "password": pwd})
        if not response.ok:
            raise AirflowException("Could not retrieve JWT Token. Status Code: [{}]. Reason: {} - {}".format(response.status_code, response.reason, response.text)) 
        token = response.json()["access"]
        payload = jwt.decode(token, verify=False)
        self.user_id = payload["user_id"]
        self.token_exp = payload["exp"]
        
        return token

    @property
    def token(self):
        if self.__token is not None:
            if arrow.get(self.__token_exp) <= arrow.now():
                self.__token = self._generate_token()
            return self.__token
        else:
            self.__token = self._generate_token()
            return self.__token

        raise AirflowException("Could not retrieve valid Plexus token.")


