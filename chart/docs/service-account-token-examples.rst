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

Service Account Token Volume Examples
=====================================

This document provides comprehensive examples for configuring Service Account Token Volumes
in the Apache Airflow Helm Chart. These examples demonstrate various security scenarios and
use cases for pod-launching executors.

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
--------

Service Account Token Volume configuration allows you to manually control how Kubernetes
service account tokens are mounted into pods launched by Airflow. This feature implements the
**Principle of Least Privilege** by providing tokens only to containers that require Kubernetes API access.

**Container-Specific Security Model:**

- **Scheduler Container**: Receives Service Account Token (requires API access for pod management)
- **Init Container**: No Service Account Token (only performs database migrations)
- **Sidecar Container**: No Service Account Token (only performs log cleanup operations)

This feature is particularly useful for:

- Compliance with security policies that restrict automatic token mounting
- Implementation of defense-in-depth security strategies
- Custom token expiration and audience configuration
- Compatibility with security policy engines like Kyverno
- Meeting compliance frameworks that mandate container-specific privilege assignment

Basic Configuration Examples
----------------------------

Default Automatic Token Mounting (Current Behavior)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is the default behavior that continues to work without any changes:

.. code-block:: yaml

   # values.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: true  # Default value

Manual Token Volume Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Basic manual configuration that disables automatic mounting and enables manual token volume:

.. code-block:: yaml

   # values.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: false
       serviceAccountTokenVolume:
         enabled: true

This configuration:

- Disables automatic service account token mounting at the ServiceAccount level
- Enables manual token volume mounting at the Pod level
- Uses default values for all other token volume settings

Security-Focused Examples
-------------------------

High-Security Environment
^^^^^^^^^^^^^^^^^^^^^^^^^

For environments requiring enhanced security with shorter token lifetimes:

.. code-block:: yaml

   # values.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: false
       serviceAccountTokenVolume:
         enabled: true
         expirationSeconds: 1800  # 30 minutes instead of default 1 hour
         mountPath: /var/run/secrets/kubernetes.io/serviceaccount
         volumeName: secure-kube-access

**Security Benefits:**

- **Shorter token lifetime** reduces exposure window if compromised
- **Explicit control** over token mounting makes security intentional
- **Container isolation**: Only scheduler container receives API access
- **Reduced attack surface**: Init and sidecar containers cannot access Kubernetes API
- **Clear naming** for security auditing and compliance reporting

**Important Security Note:**

This configuration ensures that:

- **Database migration container** (``wait-for-airflow-migrations``) operates without API access
- **Log cleanup container** (``scheduler-log-groomer``) operates without API access
- **Only the scheduler container** receives the service account token for pod management

Kyverno Policy Compliance
^^^^^^^^^^^^^^^^^^^^^^^^^

Configuration that complies with Kyverno policies requiring ``automountServiceAccountToken: false``
(`Restrict Auto-Mount of Service Account Tokens in Service Account`_ and `Restrict Auto-Mount of Service Account Tokens`_):

.. _Restrict Auto-Mount of Service Account Tokens in Service Account: https://kyverno.io/policies/other/restrict-sa-automount-sa-token/restrict-sa-automount-sa-token/
.. _Restrict Auto-Mount of Service Account Tokens: https://kyverno.io/policies/other/restrict-sa-automount-sa-token/restrict-sa-automount-sa-token/

.. code-block:: yaml

   # values.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: false  # Required by Kyverno policy
       serviceAccountTokenVolume:
         enabled: true
         expirationSeconds: 3600
         audience: "https://kubernetes.default.svc.cluster.local"

Custom Mount Path Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For applications that expect service account tokens at custom locations:

.. code-block:: yaml

   # values.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: false
       serviceAccountTokenVolume:
         enabled: true
         mountPath: /custom/sa-token
         volumeName: custom-service-account-token
         expirationSeconds: 7200  # 2 hours

This configuration mounts the token at ``/custom/sa-token`` instead of the default location.

Executor-Specific Examples
--------------------------

KubernetesExecutor Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Optimal configuration for KubernetesExecutor with security focus:

.. code-block:: yaml

   # values.yaml
   executor: KubernetesExecutor

   scheduler:
     serviceAccount:
       automountServiceAccountToken: false
       serviceAccountTokenVolume:
         enabled: true
         expirationSeconds: 3600
         mountPath: /var/run/secrets/kubernetes.io/serviceaccount
         volumeName: k8s-executor-token

   # Ensure RBAC permissions for KubernetesExecutor
   rbac:
     create: true

CeleryKubernetesExecutor Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Configuration for hybrid executor that launches both Celery workers and Kubernetes task pods:

.. code-block:: yaml

   # values.yaml
   executor: CeleryKubernetesExecutor

   scheduler:
     serviceAccount:
       automountServiceAccountToken: false
       serviceAccountTokenVolume:
         enabled: true
         expirationSeconds: 5400  # 1.5 hours for longer-running tasks
         volumeName: hybrid-executor-token

   # Redis configuration for Celery
   redis:
     enabled: true

Multi-Environment Examples
--------------------------

Development Environment
^^^^^^^^^^^^^^^^^^^^^^^

Relaxed configuration for development with longer token lifetimes:

.. code-block:: yaml

   # values-dev.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: false
       serviceAccountTokenVolume:
         enabled: true
         expirationSeconds: 14400  # 4 hours for development convenience
         mountPath: /var/run/secrets/kubernetes.io/serviceaccount

Production Environment
^^^^^^^^^^^^^^^^^^^^^^

Strict production configuration with enhanced security:

.. code-block:: yaml

   # values-prod.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: false
       serviceAccountTokenVolume:
         enabled: true
         expirationSeconds: 1800   # 30 minutes for production security
         audience: "https://kubernetes.default.svc.cluster.local"
         volumeName: prod-airflow-token

   # Additional production security settings
   securityContexts:
     pod:
       runAsNonRoot: true
       runAsUser: 50000
       fsGroup: 0
     container:
       allowPrivilegeEscalation: false
       readOnlyRootFilesystem: true
       runAsNonRoot: true

Migration Examples
------------------

Gradual Migration from Automatic to Manual
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Step 1: Test manual configuration alongside automatic (for validation):

.. code-block:: yaml

   # values-test.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: true   # Keep automatic for now
       serviceAccountTokenVolume:
         enabled: false                     # Disable manual for testing

Step 2: Enable manual configuration while keeping automatic (transition phase):

.. code-block:: yaml

   # values-transition.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: true   # Still automatic
       serviceAccountTokenVolume:
         enabled: true                      # Test manual mounting
         expirationSeconds: 3600

Step 3: Complete migration to manual-only:

.. code-block:: yaml

   # values-final.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: false  # Disable automatic
       serviceAccountTokenVolume:
         enabled: true                      # Use manual only
         expirationSeconds: 3600

Troubleshooting Examples
------------------------

Debug Configuration
^^^^^^^^^^^^^^^^^^^

Configuration with extended token lifetime for troubleshooting:

.. code-block:: yaml

   # values-debug.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: false
       serviceAccountTokenVolume:
         enabled: true
         expirationSeconds: 86400  # 24 hours for debugging
         mountPath: /var/run/secrets/kubernetes.io/serviceaccount
         volumeName: debug-sa-token

   # Enable debug logging
   config:
     logging:
       logging_level: DEBUG

Validation Commands
^^^^^^^^^^^^^^^^^^^

Commands to validate the configuration is working correctly:

.. code-block:: bash

   # Check if the service account token is mounted correctly
   kubectl exec -it deployment/airflow-scheduler -- ls -la /var/run/secrets/kubernetes.io/serviceaccount/

   # Verify token content and expiration
   kubectl exec -it deployment/airflow-scheduler -- cat /var/run/secrets/kubernetes.io/serviceaccount/token | base64 -d

   # Check scheduler logs for authentication issues
   kubectl logs deployment/airflow-scheduler | grep -i "auth\|token\|permission"

   # Describe the scheduler pod to see volume mounts
   kubectl describe pod -l component=scheduler

Advanced Configuration Examples
-------------------------------

Custom Audience Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For environments requiring specific token audiences:

.. code-block:: yaml

   # values.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: false
       serviceAccountTokenVolume:
         enabled: true
         audience: "https://my-custom-api-server.example.com"
         expirationSeconds: 3600

Multi-Cluster Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Configuration for multi-cluster deployments:

.. code-block:: yaml

   # values-cluster-a.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: false
       serviceAccountTokenVolume:
         enabled: true
         audience: "https://cluster-a.k8s.example.com"
         volumeName: cluster-a-token
         expirationSeconds: 3600

Integration with External Security Tools
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Configuration compatible with external security scanning and policy tools:

.. code-block:: yaml

   # values-security-compliant.yaml
   scheduler:
     serviceAccount:
       automountServiceAccountToken: false
       serviceAccountTokenVolume:
         enabled: true
         expirationSeconds: 1800  # Short-lived tokens
         mountPath: /var/run/secrets/kubernetes.io/serviceaccount
         volumeName: security-compliant-token

   # Additional security annotations
   airflowPodAnnotations:
     security.policy/scanned: "true"
     security.policy/compliant: "service-account-token-manual"

Best Practices Summary
----------------------

**Container Security:**

1. **Understand container roles**: Only the scheduler container requires Kubernetes API access
2. **Verify token isolation**: Ensure init and sidecar containers operate without service account tokens
3. **Implement least privilege**: Each container should receive only the minimum required permissions

**Configuration Management:**

4. **Always test** manual token configuration in non-production environments first
5. **Use shorter token lifetimes** in production environments (1800-3600 seconds)
6. **Set explicit audiences** when integrating with external systems
7. **Use descriptive volume names** for easier troubleshooting and security auditing

**Security Monitoring:**

8. **Monitor logs** for authentication issues after migration
9. **Document your configuration** for security auditing and compliance purposes
10. **Audit container permissions** regularly to ensure compliance with security policies
11. **Combine with other security measures** like Pod Security Standards and network policies

**Migration Strategy:**

12. **Plan gradual migration** from automatic to manual token mounting
13. **Validate functionality** of each container type after configuration changes
14. **Maintain backward compatibility** during transition periods

**Why This Approach is More Secure:**

- **Reduced attack surface**: Containers without API access cannot be used to compromise the cluster
- **Clear security boundaries**: Explicit definition of which containers have API privileges
- **Compliance ready**: Meets requirements for container-specific privilege assignment
- **Audit friendly**: Clear documentation of security controls for compliance reporting

For more detailed information, see the :doc:`production-guide` section on Service Account Token Volume Configuration.
