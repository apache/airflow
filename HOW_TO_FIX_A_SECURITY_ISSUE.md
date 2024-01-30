<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Fixing Security Issues in Airflow

Generally speaking, Apache Airflow addresses security issues through the following steps:

## Process

1. **Vulnerability Identification**:
   The Apache Airflow community actively monitors security channels and mailing lists for reports of vulnerabilities.
   This includes monitoring security e-mailing lists, forums, and other channels for reports of vulnerabilities.

2. **Security Advisories**:
   When vulnerabilities are identified, the community may issue security advisories to inform users about the
   vulnerabilities and provide some guidance on mitigation.

3. **Patch Releases**:
   Fixes for identified vulnerabilities are incorporated into patch releases of Apache Airflow. These patches are made
   available to users, who are encouraged to upgrade to the latest version to address the vulnerabilities.

4. **Code Review**:
   Security-sensitive changes in Apache Airflow are subject to thorough code review to identify and address potential
   security issues before they are merged into the codebase.

5. **Security Features**:
   The Apache Airflow project continuously works on introducing new security features and enhancing the already existing
   ones to improve the overall security posture of the platform.

6. **Security Testing**:
   The Airflow community may conduct security testing, including vulnerability scanning and penetration testing,
   to proactively identify and address security vulnerabilities.

7. **Community Engagement**:
   The Apache Airflow project encourages responsible vulnerability disclosure from users and security researchers
   to ensure prompt and responsible handling of security issues. We already habe a security team which follows an on-call
   rotation policy to handle these issues better. For more, check [here](contributing-docs/01_roles_in_airflow_project.rst)


## Best Practices

1. When we implement low severity issues for security, sometimes the ones that are even not worthy of having a CVE,
   we avoid describing it as a security feature to avoid web scrappers / tools running against our repository commits
   to raise reports that were not subscribed to. These tools might as well violate our security practices.
