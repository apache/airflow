# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import base64
import hashlib
import hmac
import time
import urllib.parse
import uuid
from urllib.parse import urlencode

import requests

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class MagentoHook(BaseHook):
    """Creates new connection to Magento and allows you to pull data from Magento and push data to Magento."""

    conn_name_attr = "magento_conn_id"
    default_conn_name = "magento_default"
    conn_type = "magento"
    hook_name = "Magento"

    def __init__(self, magento_conn_id="magento_default", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.magento_conn_id = magento_conn_id
        self.connection = self.get_connection(self.magento_conn_id)
        self._configure_oauth()

    def _configure_oauth(self):
        """Configure OAuth authentication for Magento API."""
        conn = self.get_connection(self.magento_conn_id)
        self.consumer_key = conn.extra_dejson.get("consumer_key")
        self.consumer_secret = conn.extra_dejson.get("consumer_secret")
        self.access_token = conn.extra_dejson.get("access_token")
        self.access_token_secret = conn.extra_dejson.get("access_token_secret")

        if not all([self.consumer_key, self.consumer_secret, self.access_token, self.access_token_secret]):
            raise AirflowException("Magento OAuth credentials are not set properly in Airflow connection")

    def _generate_oauth_parameters(self, url, method, data=None):
        """Generate OAuth parameters for the request."""
        oauth_nonce = str(uuid.uuid4())
        oauth_timestamp = str(int(time.time()))
        oauth_signature_method = "HMAC-SHA256"
        oauth_version = "1.0"

        # Parse the URL to separate the query parameters
        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qsl(parsed_url.query)

        # Prepare OAuth parameters
        oauth_params = {
            "oauth_consumer_key": self.consumer_key,
            "oauth_nonce": oauth_nonce,
            "oauth_signature_method": oauth_signature_method,
            "oauth_timestamp": oauth_timestamp,
            "oauth_token": self.access_token,
            "oauth_version": oauth_version,
        }

        # Include data parameters if present
        if data:
            query_params.extend(data.items())

        # Combine OAuth and query/data parameters, and sort them
        all_params = oauth_params.copy()
        all_params.update(query_params)
        sorted_params = sorted(all_params.items(), key=lambda x: x[0])

        # Encode and create the parameter string
        param_str = urllib.parse.urlencode(sorted_params, safe="")

        # Construct the base string
        base_string = f"{method.upper()}&{urllib.parse.quote(parsed_url.scheme + '://' + parsed_url.netloc + parsed_url.path, safe='')}&{urllib.parse.quote(param_str, safe='')}"

        # Create the signing key
        signing_key = f"{urllib.parse.quote(self.consumer_secret, safe='')}&{urllib.parse.quote(self.access_token_secret, safe='')}"

        # Generate the OAuth signature
        oauth_signature = base64.b64encode(
            hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha256).digest()
        ).decode()

        # Add the signature to OAuth parameters
        oauth_params["oauth_signature"] = oauth_signature

        # Construct the Authorization header
        auth_header = "OAuth " + ", ".join(
            [f'{urllib.parse.quote(k)}="{urllib.parse.quote(v)}"' for k, v in oauth_params.items()]
        )

        return auth_header

    def get_request(self, endpoint, search_criteria=None, method="GET", data=None):
        """Perform an API request to Magento."""
        if search_criteria:
            # Convert search criteria dictionary to URL parameters
            query_string = urlencode(search_criteria, doseq=True)
            endpoint = f"{endpoint}?{query_string}"
        url = f"https://{self.connection.host}/rest/default/V1/{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "Authorization": self._generate_oauth_parameters(url, method, data),
        }

        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers)  # Use GET method, , verify=False
            else:
                response = requests.request(method, url, headers=headers, json=data)  # Use method provided

            response.raise_for_status()  # Will raise an error for bad responses

        except requests.exceptions.HTTPError as http_err:
            error_details = None
            try:
                # Attempt to parse and log the error response
                error_details = response.json() if response.content else {}
                self.log.error("Error details: %s", error_details)
            except ValueError:
                # Failed to parse JSON response
                pass

            raise AirflowException(f"Request failed: {http_err}. Error details: {error_details}")

        except requests.exceptions.RequestException as e:
            raise AirflowException(f"Request failed: {str(e)}")

        return response.json()
