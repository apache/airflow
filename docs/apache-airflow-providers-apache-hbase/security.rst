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

Security
--------

The Apache HBase provider uses the HappyBase library to connect to HBase via the Thrift protocol with comprehensive security features including SSL/TLS encryption and Kerberos authentication.

SSL/TLS Encryption
~~~~~~~~~~~~~~~~~~

The HBase provider supports SSL/TLS encryption for secure communication with HBase Thrift servers:

**SSL Connection Types:**

* **Direct SSL**: Connect directly to SSL-enabled Thrift servers
* **SSL Proxy**: Use stunnel or similar SSL proxy for legacy Thrift servers
* **Certificate Validation**: Full certificate chain validation with custom CA support

**Certificate Management:**

* Store SSL certificates in Airflow Variables or Secrets Backend
* Support for client certificates for mutual TLS authentication
* Automatic certificate validation and hostname verification
* Custom CA certificate support for private PKI

**Configuration Example:**

.. code-block:: python

    # SSL connection with certificates from Airflow Variables
    ssl_connection = Connection(
        conn_id="hbase_ssl",
        conn_type="hbase",
        host="hbase-ssl.example.com",
        port=9091,
        extra={
            "use_ssl": True,
            "ssl_cert_var": "hbase_client_cert",
            "ssl_key_var": "hbase_client_key",
            "ssl_ca_var": "hbase_ca_cert",
            "ssl_verify": True
        }
    )

Kerberos Authentication
~~~~~~~~~~~~~~~~~~~~~~~

The provider supports Kerberos authentication for secure access to HBase clusters:

**Kerberos Features:**

* SASL/GSSAPI authentication mechanism
* Keytab-based authentication
* Principal and realm configuration
* Integration with system Kerberos configuration

**Configuration Example:**

.. code-block:: python

    # Kerberos connection
    kerberos_connection = Connection(
        conn_id="hbase_kerberos",
        conn_type="hbase",
        host="hbase-kerb.example.com",
        port=9090,
        extra={
            "use_kerberos": True,
            "kerberos_principal": "airflow@EXAMPLE.COM",
            "kerberos_keytab": "/etc/security/keytabs/airflow.keytab"
        }
    )

Data Protection
~~~~~~~~~~~~~~~

**Sensitive Data Masking:**

* Automatic masking of sensitive data in logs and error messages
* Protection of authentication credentials and certificates
* Secure handling of connection parameters

**Secrets Management:**

* Integration with Airflow Secrets Backend
* Support for external secret management systems
* Secure storage of certificates and keys

Security Best Practices
~~~~~~~~~~~~~~~~~~~~~~~

**Connection Security:**

* Always use SSL/TLS encryption in production environments
* Implement proper certificate validation and hostname verification
* Use strong authentication mechanisms (Kerberos, client certificates)
* Regularly rotate certificates and keys

**Access Control:**

* Configure HBase ACLs to restrict table and column family access
* Use principle of least privilege for service accounts
* Implement proper network segmentation and firewall rules
* Monitor and audit HBase access logs

**Operational Security:**

* Store sensitive information in Airflow's connection management system
* Avoid hardcoding credentials in DAG files
* Use Airflow's secrets backend for enhanced security
* Regularly update HBase and Airflow to latest security patches

**Network Security:**

* Deploy HBase in a secure network environment
* Use VPNs or private networks for HBase communication
* Implement proper DNS security and hostname verification
* Monitor network traffic for anomalies

Compliance and Auditing
~~~~~~~~~~~~~~~~~~~~~~~

**Security Compliance:**

* The provider supports enterprise security requirements
* Compatible with SOC 2, HIPAA, and other compliance frameworks
* Comprehensive logging and audit trail capabilities
* Support for security scanning and vulnerability assessment

**Monitoring and Alerting:**

* Integration with Airflow's monitoring and alerting systems
* Security event logging and notification
* Connection health monitoring and failure detection
* Performance monitoring for security overhead assessment

For comprehensive security configuration, consult:

* `HBase Security Guide <https://hbase.apache.org/book.html#security>`_
* `Airflow Security Documentation <https://airflow.apache.org/docs/apache-airflow/stable/security/index.html>`_
* `Kerberos Authentication Guide <https://web.mit.edu/kerberos/krb5-latest/doc/>`_