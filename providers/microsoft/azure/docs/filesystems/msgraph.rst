 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Microsoft Graph Filesystem
===========================

The Microsoft Graph filesystem provides access to OneDrive, SharePoint, and Teams document libraries through Airflow's ObjectStoragePath interface.

Supported URL formats:

* ``msgraph://connection_id/drive_id/path/to/file``
* ``sharepoint://connection_id/drive_id/path/to/file``
* ``onedrive://connection_id/drive_id/path/to/file``
* ``msgd://connection_id/drive_id/path/to/file``

Connection Configuration
------------------------

Create a Microsoft Graph connection in Airflow with the following parameters:

* **Connection Type**: msgraph
* **Host**: Tenant ID
* **Login**: Client ID
* **Password**: Client Secret

The connection form provides additional configuration fields:

* **Tenant ID**: Azure AD tenant identifier
* **Drive ID**: Specific drive to access (optional - leave empty for general access)
* **Scopes**: OAuth2 scopes (default: https://graph.microsoft.com/.default)

Additional OAuth2 parameters supported via connection extras:

* **scope**: OAuth2 access scope
* **token_endpoint**: Custom token endpoint URL
* **redirect_uri**: OAuth2 redirect URI for authorization code flow
* **token_endpoint_auth_method**: Client authentication method (default: client_secret_basic)
* **code_challenge_method**: PKCE code challenge method (e.g., 'S256')
* **username**: Username for password grant flow
* **password**: Password for password grant flow

Connection extra field configuration example:

.. code-block:: json

    {
        "drive_id": "b!abc123...",
        "scope": "https://graph.microsoft.com/.default",
        "token_endpoint": "https://login.microsoftonline.com/your-tenant/oauth2/v2.0/token",
        "redirect_uri": "http://localhost:8080/callback",
        "token_endpoint_auth_method": "client_secret_post"
    }

Usage Examples
--------------

Reading Files
^^^^^^^^^^^^^

.. code-block:: python

    from airflow.sdk.io.path import ObjectStoragePath

    # Access a file in OneDrive
    path = ObjectStoragePath("onedrive://my_conn/drive123/Documents/data.csv")

    # Read file content
    with path.open("r") as f:
        content = f.read()

Directory Operations
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # List directory contents in SharePoint
    sharepoint_path = ObjectStoragePath("sharepoint://sp_conn/site_drive/Shared Documents/")

    for item in sharepoint_path.iterdir():
        print(f"Found: {item.name}")
        if item.is_file():
            print(f"  Size: {item.stat().st_size} bytes")

File Operations
^^^^^^^^^^^^^^^

.. code-block:: python

    # Copy file between drives
    source = ObjectStoragePath("msgraph://conn1/drive1/source.txt")
    target = ObjectStoragePath("msgraph://conn2/drive2/backup/source.txt")
    source.copy(target)

    # Move file
    old_path = ObjectStoragePath("onedrive://conn/drive/temp/file.txt")
    new_path = ObjectStoragePath("onedrive://conn/drive/archive/file.txt")
    old_path.move(new_path)

    # Delete file
    file_to_delete = ObjectStoragePath("msgraph://conn/drive/old_data.csv")
    file_to_delete.unlink()

Writing Files
^^^^^^^^^^^^^

.. code-block:: python

    # Write new file
    output_path = ObjectStoragePath("sharepoint://sp_conn/docs/reports/report.txt")

    with output_path.open("w") as f:
        f.write("Generated report data\n")
        f.write(f"Created at: {datetime.now()}\n")

Drive Discovery
^^^^^^^^^^^^^^^

When you need to find the correct drive ID for your URLs, you can use the Microsoft Graph API operators:

.. code-block:: python

    from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator

    # List all drives for a user
    list_drives = MSGraphAsyncOperator(
        task_id="list_drives",
        conn_id="msgraph_conn",
        url="me/drives",
        result_processor=lambda response: [
            {"id": drive["id"], "name": drive["name"]} for drive in response["value"]
        ],
    )

URL Scheme Mapping
------------------

The different URL schemes map to specific Microsoft Graph endpoints:

* ``msgraph://`` - General Microsoft Graph access
* ``onedrive://`` - OneDrive personal and business drives
* ``sharepoint://`` - SharePoint document libraries
* ``msgd://`` - Shortened form of msgraph://

All schemes use the same underlying Microsoft Graph API and authentication.

Requirements
------------

The Microsoft Graph filesystem requires:

* ``msgraphfs`` Python package
* Valid Azure AD application registration with appropriate permissions
* Microsoft Graph API access for your tenant

Required Microsoft Graph permissions:

* ``Files.Read`` - To read files
* ``Files.ReadWrite`` - To read and write files
* ``Sites.Read.All`` - To access SharePoint sites (if using ``sharepoint://`` URLs)

Cross-References
----------------

* :doc:`Microsoft Graph API Operators </operators/msgraph>` - For API operations and drive discovery

Reference
---------

For further information, look at:

* `Microsoft Graph Files API <https://learn.microsoft.com/en-us/graph/api/resources/onedrive>`__
* `msgraphfs Python package <https://pypi.org/project/msgraphfs/>`__
* `Use the Microsoft Graph API <https://learn.microsoft.com/en-us/graph/use-the-api/>`__
