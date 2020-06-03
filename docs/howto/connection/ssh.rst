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



SSH Connection
==============
The SSH connection type provides connection to use :class:`~airflow.providers.ssh.hooks.ssh.SSHHook` to run
commands on a remote server using :class:`~airflow.providers.ssh.operators.ssh.SSHOperator` or transfer
file from/to the remote server using :class:`~airflow.providers.sftp.operators.sftp.SFTPOperator`.

Configuring the Connection
--------------------------
Host (required)
    The Remote host to connect.

Username (optional)
    The Username to connect to the ``remote_host``.

Password (optional)
    Specify the password of the username to connect to the ``remote_host``.

Port (optional)
    Port of remote host to connect. Default is ``22``.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in ssh
    connection. The following parameters out of the standard python parameters
    are supported:

    * ``key_file`` - Full Path of the private SSH Key file that will be used to connect to the remote_host.
    * ``private_key`` - Content of the private key used to connect to the remote_host.
    * ``timeout`` - An optional timeout (in seconds) for the TCP connect. Default is ``10``.
    * ``compress`` - ``true`` to ask the remote client/server to compress traffic; ``false`` to refuse compression. Default is ``true``.
    * ``no_host_key_check`` - Set to ``false`` to restrict connecting to hosts with no entries in ``~/.ssh/known_hosts`` (Hosts file). This provides maximum protection against trojan horse attacks, but can be troublesome when the ``/etc/ssh/ssh_known_hosts`` file is poorly maintained or connections to new hosts are frequently made. This option forces the user to manually add all new hosts. Default is ``true``, ssh will automatically add new host keys to the user known hosts files.
    * ``allow_host_key_change`` - Set to ``true`` if you want to allow connecting to hosts that has host key changed or when you get 'REMOTE HOST IDENTIFICATION HAS CHANGED' error.  This wont protect against Man-In-The-Middle attacks. Other possible solution is to remove the host entry from ``~/.ssh/known_hosts`` file. Default is ``false``.

    Example "extras" field:

    .. code-block:: json

       {
          "key_file": "/home/airflow/.ssh/id_rsa",
          "timeout": "10",
          "compress": "false",
          "no_host_key_check": "false",
          "allow_host_key_change": "false"
       }

    When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
    following the standard syntax of connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    Example connection string with ``key_file`` (path to key file provided in connection):

    .. code-block:: bash

        export AIRFLOW_CONN_MAIN_SERVER='ssh://user:pass@localhost:22?timeout=10&compress=false&no_host_key_check=false&allow_host_key_change=true&key_file=%2Fhome%2Fairflow%2F.ssh%2Fid_rsa'

    Example connection string with ``private_key`` (actual private key provided in connection):

    .. code-block:: bash

        export AIRFLOW_CONN_SSH_SERVER='SSH://127.0.0.1?private_key=-----BEGIN+RSA+PRIVATE+KEY-----%0AMIIEpAIBAAKCAQEAvYUM9xouSUtCKMwm%2FkogT4r3Y%2Bh7H0IPnd7DF9sKCHt9FPJ%2B%0ALaQNX%2FRgnOoPf5ySN42A1nmqv4WX5AKdjEYMIJzN2g2whnol8RVjzP4s2Ao%2B%2BWJ9%0AKstey85CQUgjWFO57ye3TyhbfMZI3fBqDX5RjgkgAZmUpKmv6ttSiCfdgGxLweD7%0ADZexlAjuSfr7i0UZWBIbSKJdePMnWGvZZO%2BGerGlOIKs%2Bqx5agMbNJqDhWn0u8OV%0ACMANhc0yaUAbN08Pjac94%2FxmZPHASytrBmTGd6zYcuzOyxwK8KHMeLUagByT3u7l%0AvWcVyRx8FAXkl7nGF2SQZ0z3JLhmdWMSXuc1AQIDAQABAoIBAQC8%2Bp1REVQyVc8k%0A612%2Bl5%2FccU%2F62elb4%2F26iFS1xv8cMjcp2hwj2sBTfFWSYnsN3syWhI2CUFQJImex%0AP0Jmi7qwEmvaEWiCz%2B5hldisoo%2BI5b6h4qm5MI3YYFYEzrAf9W0kos%2FRKQcBRp%2BG%0AX6MAzYL5RPQbZE%2BqWmJGqGiFyGrBEISl%2FMdoaqSJewTRLHwDtbD9lt4WRPUO%2Font%0A%2FUKwOu3i9z5hMQm9HJJLuKr3hl5jmjJbJUg50a7fjVJzr52VfxH73Z%2Fst40fD3x4%0AH1DHGbX4ar9JOYvhzdXkuxyNXvoglJUIOiAk23Od8q9xOMQAITuwkc1QaVRXwiE7%0Aw41lMC8ZAoGBAOB9PEFyzGwYZgiReOQsAJrlwT7zsY053OGSXAeoLC2OzyLNb8v7%0AnKy2qoTMwxe9LHUDDAp6I8btprvLq35Y72iCbGg0ZK5fIYv%2Bt03NjvOOl1zEuUny%0A5xGe1IvP4YgMQuVMVw5dj11Jmna5eW3oFXlyOQrlth9hrexuI%2BG25qwvAoGBANgf%0AOhy%2FofyIgrIGwaRbgg55rlqViLNGFcJ6I3dVlsRqFxND4PvQZZWfCN3LhIGgI8cT%0AN6hFGPR9QrsmXe3eHM7%2FUpMk53oiPD9E0MemPtQh2AFPUb%2BznqxrXNGvtww6xYBM%0AKYLXcQVn%2FKELwwMYw3F0HGKgCFF0XthV34f%2Bt%2FXPAoGBALVLjqEQlBTsM2LSEP68%0AppRx3nn3lrmGNGMbryUj5OG6BoCFxrbG8gXt05JCR4Bhb4jkOBIyB7i87r2VQ19b%0AdaVCR0h0n6bO%2FymvQNwdmUgLLSRnX3hgKcpqKh7reKlFtbS2zUu1tXVSXuNo8K8Z%0AElatL3Ikh8uaODrLzECaVHpTAoGAXcReoC58h2Zq3faUeUzChqlAfki2gKF9u1zm%0AmlXmDd3BmTgwGtD14g6X%2BDLekKb8Htk1oqooA5t9IlmpExT1BtI7719pltHXtdOT%0AiauVQtBUOW1CmJvD0ibapJdKIeI14k4pDH2QqbnOH8lMmMFbupOX5SptsXl91Pqc%0A%2BxIGmn0CgYBOL2o0Sn%2F8d7uzAZKUBG1%2F0eFr4j6wYwWajVDFOfbJ7WdIf5j%2BL3nY%0A3440i%2Fb2NlEE8nLPDl6cwiOtwV0XFkoiF3ctHvutlhGBxAKHetIxIsnQk7vXqgfP%0AnhsgNypNAQXbxe3gjJEb4Fzw3Ufz3mq5PllYtXKhc%2Bmc4%2B3sN5uGow%3D%3D%0A-----END+RSA+PRIVATE+KEY-----%0A'
